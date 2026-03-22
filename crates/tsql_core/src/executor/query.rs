use crate::ast::{Expr, SelectItem, SelectStmt};
use crate::catalog::{Catalog, RoutineKind};
use crate::error::DbError;
use crate::parser::parse_expr_subquery_aware;
use crate::storage::{Storage, StoredRow};
use crate::types::Value;

use super::aggregates::is_aggregate_function;
use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::{eval_expr, eval_predicate};
use super::grouping::GroupExecutor;
use super::joins::apply_join;
use super::model::{BoundTable, JoinedRow};
use super::planner::PhysicalPlan;
use super::projection::{
    deduplicate_projected_rows, expand_projection_columns, expand_wildcard_values, eval_top_n,
};
use super::query_planner::{bind_table as planner_bind_table, build_logical_plan, build_physical_plan, execute_scan};

pub struct QueryExecutor<'a> {
    pub catalog: &'a dyn Catalog,
    pub storage: &'a dyn Storage,
    pub clock: &'a dyn Clock,
}

impl<'a> QueryExecutor<'a> {
    pub fn execute_select(
        &self,
        stmt: SelectStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        if stmt.from.is_none() {
            let source_rows = vec![vec![]];
            let has_aggregate = stmt
                .projection
                .iter()
                .any(|item| matches!(&item.expr, Expr::FunctionCall { name, .. } if is_aggregate_function(name)));
            let result = if !stmt.group_by.is_empty() || has_aggregate {
                let group_executor = GroupExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                };
                group_executor.execute_grouped_select(
                    stmt.projection,
                    source_rows,
                    stmt.group_by,
                    stmt.having,
                    ctx,
                )?
            } else {
                self.execute_flat_select(stmt.projection, source_rows, ctx)?
            };
            return Ok(result);
        }

        let logical = build_logical_plan(&stmt)?;
        let plan = build_physical_plan(
            &stmt,
            &logical,
            self.catalog,
            ctx,
            |tref, cat, c| self.bind_table(tref, cat, c),
        )?;
        self.execute_physical_plan(plan, ctx)
    }

    fn execute_physical_plan(
        &self,
        plan: PhysicalPlan,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let mut source_rows =
            execute_scan(&plan.base, ctx, self.catalog, self.storage, self.clock)?;

        for join_plan in &plan.joins {
            let right_rows =
                execute_scan(&join_plan.right, ctx, self.catalog, self.storage, self.clock)?;
            source_rows = apply_join(
                source_rows,
                right_rows,
                join_plan.right.bound.clone(),
                &join_plan.join,
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
        }

        if let Some(where_clause) = &plan.residual_filter {
            let mut filtered = Vec::new();
            for row in source_rows {
                if eval_predicate(
                    where_clause,
                    &row,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )? {
                    filtered.push(row);
                }
            }
            source_rows = filtered;
        }

        let has_aggregate = plan
            .projection
            .iter()
            .any(|item| matches!(&item.expr, Expr::FunctionCall { name, .. } if is_aggregate_function(name)));

        let result = if !plan.group_by.is_empty() || has_aggregate {
            let group_executor = GroupExecutor {
                catalog: self.catalog,
                storage: self.storage,
                clock: self.clock,
            };
            group_executor.execute_grouped_select(
                plan.projection,
                source_rows,
                plan.group_by,
                plan.having,
                ctx,
            )?
        } else {
            self.execute_flat_select(plan.projection, source_rows, ctx)?
        };

        let mut final_rows = result.rows;
        if plan.distinct {
            final_rows = deduplicate_projected_rows(final_rows);
        }

        if !plan.order_by.is_empty() && !plan.order_satisfied_by_scan {
            let columns = &result.columns;
            let order_by_refs = &plan.order_by;
            final_rows.sort_by(|a, b| {
                super::projection::compare_projected_rows(a, b, columns, order_by_refs)
            });
        }

        if let Some(top) = plan.top {
            let n = eval_top_n(&top, ctx, self.catalog, self.storage, self.clock)?;
            if final_rows.len() > n {
                final_rows.truncate(n);
            }
        }

        let _ = &plan.required_columns;
        Ok(super::result::QueryResult {
            columns: result.columns,
            rows: final_rows,
        })
    }

    fn bind_table(
        &self,
        tref: crate::ast::TableRef,
        catalog: &dyn Catalog,
        ctx: &mut ExecutionContext,
    ) -> Result<BoundTable, DbError> {
        if let Some(bound_tvf) = self.bind_inline_tvf(&tref, ctx)? {
            return Ok(bound_tvf);
        }
        planner_bind_table(tref, catalog, ctx)
    }

    fn bind_inline_tvf(
        &self,
        tref: &crate::ast::TableRef,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<BoundTable>, DbError> {
        let name = &tref.name.name;
        let Some(open) = name.find('(') else {
            return Ok(None);
        };
        if !name.ends_with(')') {
            return Ok(None);
        }
        let fname = name[..open].trim();
        let args_raw = &name[open + 1..name.len() - 1];
        let schema = tref.name.schema_or_dbo();
        let Some(routine) = self.catalog.find_routine(schema, fname).cloned() else {
            return Ok(None);
        };
        let RoutineKind::Function { body, .. } = routine.kind else {
            return Ok(None);
        };
        let crate::ast::FunctionBody::InlineTable(query) = body else {
            return Ok(None);
        };
        let arg_exprs = split_csv_top_level_local(args_raw);
        if arg_exprs.len() != routine.params.len() {
            return Err(DbError::Execution(format!(
                "TVF '{}.{}' expected {} args, got {}",
                schema,
                fname,
                routine.params.len(),
                arg_exprs.len()
            )));
        }

        ctx.enter_scope();
        for (param, arg_raw) in routine.params.iter().zip(arg_exprs.iter()) {
            let expr = parse_expr_subquery_aware(arg_raw)?;
            let val = eval_expr(
                &expr,
                &[],
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
            let ty = super::type_mapping::data_type_spec_to_runtime(&param.data_type);
            let coerced = super::value_ops::coerce_value_to_type(val, &ty)?;
            ctx.variables.insert(param.name.clone(), (ty, coerced));
            ctx.register_declared_var(&param.name);
        }

        let result = self.execute_select(query, ctx)?;
        ctx.leave_scope();

        let table_def = crate::catalog::TableDef {
            id: 0,
            schema_id: 1,
            name: fname.to_string(),
            columns: result
                .columns
                .iter()
                .enumerate()
                .map(|(i, cname)| crate::catalog::ColumnDef {
                    id: (i + 1) as u32,
                    name: cname.clone(),
                    data_type: crate::types::DataType::VarChar { max_len: 4000 },
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    identity: None,
                    default: None,
                    default_constraint_name: None,
                    check: None,
                    check_constraint_name: None,
                    computed_expr: None,
                })
                .collect(),
            check_constraints: vec![],
        };
        let rows = result
            .rows
            .into_iter()
            .map(|values| StoredRow {
                values,
                deleted: false,
            })
            .collect::<Vec<_>>();
        Ok(Some(BoundTable {
            alias: tref.alias.clone().unwrap_or_else(|| fname.to_string()),
            table: table_def,
            virtual_rows: Some(rows),
        }))
    }

    fn execute_flat_select(
        &self,
        projection: Vec<SelectItem>,
        rows: Vec<JoinedRow>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let columns = expand_projection_columns(&projection, rows.first());
        let projected_rows = self.project_flat_rows(&projection, &rows, ctx);
        Ok(super::result::QueryResult {
            columns,
            rows: projected_rows,
        })
    }

    fn project_flat_rows(
        &self,
        projection: &[SelectItem],
        rows: &[JoinedRow],
        ctx: &mut ExecutionContext,
    ) -> Vec<Vec<Value>> {
        rows.iter()
            .map(|row| {
                let mut out = Vec::new();
                for item in projection {
                    match &item.expr {
                        Expr::Wildcard => out.extend(expand_wildcard_values(row)),
                        expr => out.push(
                            eval_expr(expr, row, ctx, self.catalog, self.storage, self.clock)
                                .unwrap_or(Value::Null),
                        ),
                    }
                }
                out
            })
            .collect()
    }
}

fn split_csv_top_level_local(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut depth = 0usize;
    let mut in_string = false;
    for ch in input.chars() {
        match ch {
            '\'' => {
                in_string = !in_string;
                buf.push(ch);
            }
            '(' if !in_string => {
                depth += 1;
                buf.push(ch);
            }
            ')' if !in_string => {
                depth = depth.saturating_sub(1);
                buf.push(ch);
            }
            ',' if !in_string && depth == 0 => {
                if !buf.trim().is_empty() {
                    out.push(buf.trim().to_string());
                }
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() {
        out.push(buf.trim().to_string());
    }
    out
}
