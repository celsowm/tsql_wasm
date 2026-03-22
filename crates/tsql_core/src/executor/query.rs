use std::cmp::Ordering;

use crate::ast::{BinaryOp, Expr, JoinType, SelectItem, SelectStmt};
use crate::catalog::{Catalog, RoutineKind};
use crate::error::DbError;
use crate::parser::parse_expr_subquery_aware;
use crate::storage::{Storage, StoredRow};
use crate::types::Value;

use super::aggregates::{
    eval_aggregate_avg, eval_aggregate_count, eval_aggregate_max, eval_aggregate_min,
    eval_aggregate_sum, is_aggregate_function, Group,
};
use super::clock::Clock;
use super::context::ExecutionContext;
use super::cte::{cte_to_context_rows, resolve_cte_table};
use super::evaluator::{eval_expr, eval_predicate};
use super::joins::apply_join;
use super::metadata::resolve_virtual_table;
use super::model::{BoundTable, ContextTable, JoinedRow};
use super::operators::eval_binary;
use super::planner::{LogicalPlan, PhysicalJoin, PhysicalPlan, PhysicalScan, ScanStrategy};
use super::projection::{
    compare_projected_rows, deduplicate_projected_rows, eval_top_n, expand_projection_columns,
    expand_wildcard_values,
};
use super::value_ops::{compare_values, truthy};

fn eval_having_expr(
    expr: &Expr,
    row: &[ContextTable],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match expr {
        Expr::FunctionCall { name, args } if is_aggregate_function(name) => {
            match name.to_uppercase().as_str() {
                "COUNT" => Ok(eval_aggregate_count(
                    args, group, ctx, catalog, storage, clock,
                )),
                "SUM" => eval_aggregate_sum(args, group, ctx, catalog, storage, clock),
                "AVG" => eval_aggregate_avg(args, group, ctx, catalog, storage, clock),
                "MIN" => eval_aggregate_min(args, group, ctx, catalog, storage, clock),
                "MAX" => eval_aggregate_max(args, group, ctx, catalog, storage, clock),
                _ => eval_expr(expr, row, ctx, catalog, storage, clock),
            }
        }
        Expr::Binary { left, op, right } => {
            let lv = eval_having_expr(left, row, group, ctx, catalog, storage, clock)?;
            let rv = eval_having_expr(right, row, group, ctx, catalog, storage, clock)?;
            eval_binary(op, lv, rv)
        }
        Expr::Unary { op, expr: inner } => {
            let val = eval_having_expr(inner, row, group, ctx, catalog, storage, clock)?;
            super::operators::eval_unary(op, val)
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let operand_val = match operand {
                Some(e) => Some(eval_having_expr(
                    e, row, group, ctx, catalog, storage, clock,
                )?),
                None => None,
            };
            for clause in when_clauses {
                let match_found = if let Some(ref op_val) = operand_val {
                    let when_val = eval_having_expr(
                        &clause.condition,
                        row,
                        group,
                        ctx,
                        catalog,
                        storage,
                        clock,
                    )?;
                    matches!(compare_values(&op_val, &when_val), Ordering::Equal)
                } else {
                    let cond = eval_having_expr(
                        &clause.condition,
                        row,
                        group,
                        ctx,
                        catalog,
                        storage,
                        clock,
                    )?;
                    truthy(&cond)
                };
                if match_found {
                    return eval_having_expr(
                        &clause.result,
                        row,
                        group,
                        ctx,
                        catalog,
                        storage,
                        clock,
                    );
                }
            }
            match else_result {
                Some(expr) => eval_having_expr(expr, row, group, ctx, catalog, storage, clock),
                None => Ok(Value::Null),
            }
        }
        Expr::Between {
            expr: between_expr,
            low,
            high,
            negated,
        } => {
            let val = eval_having_expr(between_expr, row, group, ctx, catalog, storage, clock)?;
            let low_val = eval_having_expr(low, row, group, ctx, catalog, storage, clock)?;
            let high_val = eval_having_expr(high, row, group, ctx, catalog, storage, clock)?;
            if val.is_null() || low_val.is_null() || high_val.is_null() {
                return Ok(Value::Null);
            }
            let ge_low = matches!(
                compare_values(&val, &low_val),
                Ordering::Greater | Ordering::Equal
            );
            let le_high = matches!(
                compare_values(&val, &high_val),
                Ordering::Less | Ordering::Equal
            );
            let result = ge_low && le_high;
            Ok(Value::Bit(if *negated { !result } else { result }))
        }
        _ => eval_expr(expr, row, ctx, catalog, storage, clock),
    }
}

fn eval_having_predicate(
    expr: &Expr,
    row: &[ContextTable],
    group: &Group,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<bool, DbError> {
    let value = eval_having_expr(expr, row, group, ctx, catalog, storage, clock)?;
    let result = match &value {
        Value::Bit(v) => *v,
        Value::Null => false,
        other => truthy(other),
    };
    Ok(result)
}

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
                self.execute_grouped_select(
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

        let logical = self.build_logical_plan(&stmt)?;
        let plan = self.build_physical_plan(stmt, logical, ctx)?;
        self.execute_physical_plan(plan, ctx)
    }

    fn build_logical_plan(&self, stmt: &SelectStmt) -> Result<LogicalPlan, DbError> {
        let Some(from) = &stmt.from else {
            return Err(DbError::Execution("planner requires FROM source".into()));
        };
        let mut plan = LogicalPlan::Scan {
            table: from.clone(),
        };
        for join in &stmt.joins {
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                join: join.clone(),
            };
        }
        if let Some(selection) = &stmt.selection {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: selection.clone(),
            };
        }
        if !stmt.group_by.is_empty() || stmt.having.is_some() {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: stmt.group_by.clone(),
                having: stmt.having.clone(),
            };
        }
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            projection: stmt.projection.clone(),
        };
        if stmt.distinct {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }
        if !stmt.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: stmt.order_by.clone(),
            };
        }
        if let Some(top) = &stmt.top {
            plan = LogicalPlan::Top {
                input: Box::new(plan),
                top: top.clone(),
            };
        }
        Ok(plan)
    }

    fn build_physical_plan(
        &self,
        mut stmt: SelectStmt,
        logical: LogicalPlan,
        ctx: &mut ExecutionContext,
    ) -> Result<PhysicalPlan, DbError> {
        let Some(from) = stmt.from.clone() else {
            return Err(DbError::Execution("planner requires FROM source".into()));
        };

        // Safe v1 pushdown: only single-source scans.
        let all_inner = stmt.joins.iter().all(|j| j.join_type == JoinType::Inner);
        let mut alias_predicates: std::collections::HashMap<String, Vec<Expr>> =
            std::collections::HashMap::new();
        let mut residual = stmt.selection.clone();
        if all_inner && stmt.joins.is_empty() {
            alias_predicates.insert("".to_string(), split_conjuncts(stmt.selection.clone()));
            residual = None;
        }

        if all_inner && !stmt.joins.is_empty() {
            stmt.joins = self.reorder_inner_joins_heuristic(&from, stmt.joins, ctx)?;
        }

        let base_bound = self.bind_table(from, ctx)?;
        let base_predicate = if stmt.joins.is_empty() {
            alias_predicates.remove("").and_then(and_terms)
        } else {
            None
        };
        let base_scan = self.plan_scan(base_bound, base_predicate, &stmt.order_by);

        let mut joins = Vec::new();
        for join in stmt.joins {
            let right_bound = self.bind_table(join.table.clone(), ctx)?;
            let right_pred = alias_predicates
                .remove(&right_bound.alias.to_uppercase())
                .and_then(and_terms);
            let right_scan = self.plan_scan(right_bound, right_pred, &[]);
            joins.push(PhysicalJoin {
                right: right_scan,
                join,
            });
        }

        let required_columns = required_columns_from_logical(&logical);
        let order_satisfied_by_scan = joins.is_empty()
            && base_scan.pushed_predicate.is_none()
            && scan_satisfies_order(&base_scan, &stmt.order_by, self.catalog);

        Ok(PhysicalPlan {
            base: base_scan,
            joins,
            residual_filter: residual,
            projection: stmt.projection,
            group_by: stmt.group_by,
            having: stmt.having,
            distinct: stmt.distinct,
            order_by: stmt.order_by,
            top: stmt.top,
            required_columns,
            order_satisfied_by_scan,
        })
    }

    fn plan_scan(
        &self,
        bound: BoundTable,
        pushed_predicate: Option<Expr>,
        order_by: &[crate::ast::OrderByExpr],
    ) -> PhysicalScan {
        let strategy =
            choose_scan_strategy(&bound, pushed_predicate.as_ref(), order_by, self.catalog);
        PhysicalScan {
            bound,
            strategy,
            pushed_predicate,
        }
    }

    fn reorder_inner_joins_heuristic(
        &self,
        from: &crate::ast::TableRef,
        joins: Vec<crate::ast::JoinClause>,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<crate::ast::JoinClause>, DbError> {
        // Keep lexical join order to preserve ON-clause binding semantics.
        // The method still exists as the join-planning hook for future cost/rule work.
        let _ = from;
        let _ = ctx;
        Ok(joins)
    }

    fn execute_physical_plan(
        &self,
        plan: PhysicalPlan,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let mut source_rows = self.execute_scan(&plan.base, ctx)?;

        for join_plan in &plan.joins {
            let right_rows = self.execute_scan(&join_plan.right, ctx)?;
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
            self.execute_grouped_select(
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
            final_rows.sort_by(|a, b| compare_projected_rows(a, b, columns, order_by_refs));
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

    fn execute_scan(
        &self,
        scan: &PhysicalScan,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<JoinedRow>, DbError> {
        let rows = self.bind_table_rows(&scan.bound, ctx)?;
        let mut scanned = match scan.strategy {
            ScanStrategy::TableScan => rows,
            ScanStrategy::IndexSeek { .. } | ScanStrategy::IndexScan { .. } => {
                self.apply_index_strategy(rows, scan, ctx)?
            }
        };
        if let Some(predicate) = &scan.pushed_predicate {
            scanned.retain(|row| {
                eval_predicate(predicate, row, ctx, self.catalog, self.storage, self.clock)
                    .unwrap_or(false)
            });
        }
        Ok(scanned)
    }

    fn apply_index_strategy(
        &self,
        rows: Vec<JoinedRow>,
        scan: &PhysicalScan,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<JoinedRow>, DbError> {
        let index_id = match scan.strategy {
            ScanStrategy::IndexSeek { index_id } | ScanStrategy::IndexScan { index_id } => index_id,
            ScanStrategy::TableScan => return Ok(rows),
        };
        let Some(index) = self
            .catalog
            .get_indexes()
            .iter()
            .find(|idx| idx.id == index_id && idx.table_id == scan.bound.table.id)
        else {
            return Ok(rows);
        };
        let Some(first_col_id) = index.column_ids.first().copied() else {
            return Ok(rows);
        };
        let Some(col_idx) = scan
            .bound
            .table
            .columns
            .iter()
            .position(|c| c.id == first_col_id)
        else {
            return Ok(rows);
        };

        let mut keyed: Vec<(Value, JoinedRow)> = rows
            .into_iter()
            .filter_map(|row| {
                let v = row
                    .first()
                    .and_then(|ct| ct.row.as_ref())
                    .and_then(|r| r.values.get(col_idx))
                    .cloned()?;
                Some((v, row))
            })
            .collect();
        keyed.sort_by(|a, b| compare_values(&a.0, &b.0));

        let out = if matches!(scan.strategy, ScanStrategy::IndexSeek { .. }) {
            if let Some((op, rhs)) = extract_index_predicate_rhs(
                scan.pushed_predicate.as_ref(),
                &scan.bound.alias,
                &scan.bound.table.columns[col_idx].name,
            ) {
                let rhs_val = eval_expr(&rhs, &[], ctx, self.catalog, self.storage, self.clock)?;
                keyed
                    .into_iter()
                    .filter(|(lhs, _)| {
                        matches!(op, BinaryOp::Eq)
                            && compare_values(lhs, &rhs_val) == Ordering::Equal
                    })
                    .map(|(_, row)| row)
                    .collect()
            } else {
                keyed.into_iter().map(|(_, row)| row).collect()
            }
        } else if let Some((op, rhs)) = extract_index_predicate_rhs(
            scan.pushed_predicate.as_ref(),
            &scan.bound.alias,
            &scan.bound.table.columns[col_idx].name,
        ) {
            let rhs_val = eval_expr(&rhs, &[], ctx, self.catalog, self.storage, self.clock)?;
            keyed
                .into_iter()
                .filter(|(lhs, _)| compare_with_op(compare_values(lhs, &rhs_val), op))
                .map(|(_, row)| row)
                .collect()
        } else {
            keyed.into_iter().map(|(_, row)| row).collect()
        };
        Ok(out)
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

    fn execute_grouped_select(
        &self,
        projection: Vec<SelectItem>,
        rows: Vec<JoinedRow>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let groups = self.build_groups(rows, &group_by, ctx)?;
        let mut projected_rows = Vec::new();

        for group in groups {
            if let Some(having_expr) = &having {
                let sample_row = if group.rows.is_empty() {
                    vec![]
                } else {
                    group.rows[0].clone()
                };
                if !eval_having_predicate(
                    having_expr,
                    &sample_row,
                    &group,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )? {
                    continue;
                }
            }
            projected_rows.push(self.project_group_row(&projection, &group, ctx)?);
        }

        let columns = expand_projection_columns(&projection, None);
        Ok(super::result::QueryResult {
            columns,
            rows: projected_rows,
        })
    }

    fn build_groups(
        &self,
        rows: Vec<JoinedRow>,
        group_by: &[Expr],
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<Group>, DbError> {
        if group_by.is_empty() {
            return Ok(vec![Group { key: vec![], rows }]);
        }
        let mut groups: Vec<Group> = Vec::new();
        for row in rows {
            let mut key = Vec::new();
            for expr in group_by {
                key.push(eval_expr(
                    expr,
                    &row,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?);
            }

            if let Some(group) = groups.iter_mut().find(|g| g.key == key) {
                group.rows.push(row);
            } else {
                groups.push(Group {
                    key,
                    rows: vec![row],
                });
            }
        }
        Ok(groups)
    }

    fn bind_table(
        &self,
        mut tref: crate::ast::TableRef,
        ctx: &mut ExecutionContext,
    ) -> Result<BoundTable, DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&tref.name.name) {
            tref.name.name = mapped;
            if tref.name.schema.is_none() {
                tref.name.schema = Some("dbo".to_string());
            }
        }
        let schema = tref.name.schema_or_dbo();
        let name = &tref.name.name;

        if let Some(bound_tvf) = self.bind_inline_tvf(schema, name, &tref, ctx)? {
            return Ok(bound_tvf);
        }

        if let Some(cte) = resolve_cte_table(&ctx.ctes, schema, name) {
            return Ok(BoundTable {
                alias: tref.alias.clone().unwrap_or_else(|| name.clone()),
                table: cte.table_def.clone(),
                virtual_rows: None,
            });
        }

        if let Some((table, rows)) = resolve_virtual_table(schema, name, self.catalog) {
            return Ok(BoundTable {
                alias: tref.alias.clone().unwrap_or_else(|| name.clone()),
                table,
                virtual_rows: Some(rows),
            });
        }

        let table = self
            .catalog
            .find_table(schema, name)
            .ok_or_else(|| DbError::Semantic(format!("table '{}.{}' not found", schema, name)))?
            .clone();

        Ok(BoundTable {
            alias: tref.alias.clone().unwrap_or_else(|| table.name.clone()),
            table,
            virtual_rows: None,
        })
    }

    fn bind_table_rows(
        &self,
        bound: &BoundTable,
        ctx: &ExecutionContext,
    ) -> Result<Vec<JoinedRow>, DbError> {
        if let Some(cte) = ctx.ctes.get(&bound.table.name.to_uppercase()) {
            return Ok(cte_to_context_rows(cte, &bound.alias));
        }

        if let Some(rows) = &bound.virtual_rows {
            return Ok(rows
                .iter()
                .map(|row| {
                    vec![ContextTable {
                        table: bound.table.clone(),
                        alias: bound.alias.clone(),
                        row: Some(row.clone()),
                    }]
                })
                .collect());
        }

        let stored_rows = self.storage.get_rows(bound.table.id)?;

        Ok(stored_rows
            .iter()
            .filter(|r| !r.deleted)
            .map(|row| {
                vec![ContextTable {
                    table: bound.table.clone(),
                    alias: bound.alias.clone(),
                    row: Some(row.clone()),
                }]
            })
            .collect())
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

    fn project_group_row(
        &self,
        projection: &[SelectItem],
        group: &Group,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<Value>, DbError> {
        let mut out = Vec::new();
        let sample_row = if group.rows.is_empty() {
            None
        } else {
            Some(&group.rows[0])
        };

        for item in projection {
            match &item.expr {
                Expr::FunctionCall { name, args } if is_aggregate_function(name) => {
                    out.push(self.eval_aggregate(name, args, group, ctx));
                }
                Expr::Wildcard => {
                    if let Some(row) = sample_row {
                        out.extend(super::projection::expand_wildcard_values(row));
                    }
                }
                expr => {
                    if let Some(row) = sample_row {
                        out.push(eval_expr(
                            expr,
                            row,
                            ctx,
                            self.catalog,
                            self.storage,
                            self.clock,
                        )?);
                    } else {
                        out.push(Value::Null);
                    }
                }
            }
        }
        Ok(out)
    }

    fn eval_aggregate(
        &self,
        name: &str,
        args: &[Expr],
        group: &Group,
        ctx: &mut ExecutionContext,
    ) -> Value {
        match name.to_uppercase().as_str() {
            "COUNT" => {
                eval_aggregate_count(args, group, ctx, self.catalog, self.storage, self.clock)
            }
            "SUM" => eval_aggregate_sum(args, group, ctx, self.catalog, self.storage, self.clock)
                .unwrap_or(Value::Null),
            "AVG" => eval_aggregate_avg(args, group, ctx, self.catalog, self.storage, self.clock)
                .unwrap_or(Value::Null),
            "MIN" => eval_aggregate_min(args, group, ctx, self.catalog, self.storage, self.clock)
                .unwrap_or(Value::Null),
            "MAX" => eval_aggregate_max(args, group, ctx, self.catalog, self.storage, self.clock)
                .unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }

    fn bind_inline_tvf(
        &self,
        schema: &str,
        name: &str,
        tref: &crate::ast::TableRef,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<BoundTable>, DbError> {
        let Some(open) = name.find('(') else {
            return Ok(None);
        };
        if !name.ends_with(')') {
            return Ok(None);
        }
        let fname = name[..open].trim();
        let args_raw = &name[open + 1..name.len() - 1];
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
            let val = super::evaluator::eval_expr(
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

fn split_conjuncts(expr: Option<Expr>) -> Vec<Expr> {
    fn walk(expr: Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::Binary {
                left,
                op: BinaryOp::And,
                right,
            } => {
                walk(*left, out);
                walk(*right, out);
            }
            other => out.push(other),
        }
    }
    let mut out = Vec::new();
    if let Some(e) = expr {
        walk(e, &mut out);
    }
    out
}

fn and_terms(mut terms: Vec<Expr>) -> Option<Expr> {
    if terms.is_empty() {
        return None;
    }
    let mut acc = terms.remove(0);
    for term in terms {
        acc = Expr::Binary {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(term),
        };
    }
    Some(acc)
}

fn expr_aliases(expr: &Expr) -> std::collections::HashSet<String> {
    fn walk(expr: &Expr, out: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::QualifiedIdentifier(parts) => {
                if let Some(alias) = parts.first() {
                    out.insert(alias.to_uppercase());
                }
            }
            Expr::Binary { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            Expr::Unary { expr, .. } => walk(expr, out),
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => walk(inner, out),
            Expr::Cast { expr, .. } => walk(expr, out),
            Expr::Convert { expr, .. } => walk(expr, out),
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                if let Some(op) = operand {
                    walk(op, out);
                }
                for wc in when_clauses {
                    walk(&wc.condition, out);
                    walk(&wc.result, out);
                }
                if let Some(er) = else_result {
                    walk(er, out);
                }
            }
            Expr::InList { expr, list, .. } => {
                walk(expr, out);
                for item in list {
                    walk(item, out);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                walk(expr, out);
                walk(low, out);
                walk(high, out);
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, out);
                walk(pattern, out);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    walk(arg, out);
                }
            }
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {}
            _ => {}
        }
    }
    let mut out = std::collections::HashSet::new();
    walk(expr, &mut out);
    out
}

fn required_columns_from_logical(plan: &LogicalPlan) -> Vec<String> {
    fn collect(plan: &LogicalPlan, out: &mut std::collections::HashSet<String>) {
        match plan {
            LogicalPlan::Scan { table } => {
                out.insert(format!(
                    "{}.{}",
                    table.name.schema_or_dbo().to_uppercase(),
                    table.name.name.to_uppercase()
                ));
            }
            LogicalPlan::Join { left, join } => {
                collect(left, out);
                out.insert(join.table.name.name.to_uppercase());
            }
            LogicalPlan::Filter { input, predicate } => {
                collect(input, out);
                for alias in expr_aliases(predicate) {
                    out.insert(alias);
                }
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                having,
            } => {
                collect(input, out);
                for expr in group_by {
                    for alias in expr_aliases(expr) {
                        out.insert(alias);
                    }
                }
                if let Some(h) = having {
                    for alias in expr_aliases(h) {
                        out.insert(alias);
                    }
                }
            }
            LogicalPlan::Project { input, projection } => {
                collect(input, out);
                for item in projection {
                    for alias in expr_aliases(&item.expr) {
                        out.insert(alias);
                    }
                }
            }
            LogicalPlan::Distinct { input } => collect(input, out),
            LogicalPlan::Sort { input, order_by } => {
                collect(input, out);
                for item in order_by {
                    for alias in expr_aliases(&item.expr) {
                        out.insert(alias);
                    }
                }
            }
            LogicalPlan::Top { input, top } => {
                collect(input, out);
                for alias in expr_aliases(&top.value) {
                    out.insert(alias);
                }
            }
        }
    }
    let mut out = std::collections::HashSet::new();
    collect(plan, &mut out);
    out.into_iter().collect()
}

fn choose_scan_strategy(
    bound: &BoundTable,
    predicate: Option<&Expr>,
    order_by: &[crate::ast::OrderByExpr],
    catalog: &dyn Catalog,
) -> ScanStrategy {
    let indexes: Vec<&crate::catalog::IndexDef> = catalog
        .get_indexes()
        .iter()
        .filter(|idx| idx.table_id == bound.table.id)
        .collect();
    if indexes.is_empty() {
        return ScanStrategy::TableScan;
    }
    let Some(idx) = indexes.first() else {
        return ScanStrategy::TableScan;
    };
    let Some(first_col_id) = idx.column_ids.first() else {
        return ScanStrategy::TableScan;
    };
    let Some(first_col) = bound.table.columns.iter().find(|c| c.id == *first_col_id) else {
        return ScanStrategy::TableScan;
    };

    if let Some(pred) = predicate {
        if let Some((op, _)) =
            extract_index_predicate_rhs(Some(pred), &bound.alias, &first_col.name)
        {
            if matches!(op, BinaryOp::Eq) {
                return ScanStrategy::IndexSeek { index_id: idx.id };
            }
            return ScanStrategy::IndexScan { index_id: idx.id };
        }
    }
    if order_by.len() == 1 {
        if let Expr::QualifiedIdentifier(parts) = &order_by[0].expr {
            if parts.len() >= 2
                && parts[0].eq_ignore_ascii_case(&bound.alias)
                && parts[1].eq_ignore_ascii_case(&first_col.name)
                && !order_by[0].desc
            {
                return ScanStrategy::IndexScan { index_id: idx.id };
            }
        }
    }
    ScanStrategy::TableScan
}

fn scan_satisfies_order(
    scan: &PhysicalScan,
    order_by: &[crate::ast::OrderByExpr],
    catalog: &dyn Catalog,
) -> bool {
    if order_by.is_empty() {
        return true;
    }
    let index_id = match scan.strategy {
        ScanStrategy::IndexSeek { index_id } | ScanStrategy::IndexScan { index_id } => index_id,
        ScanStrategy::TableScan => return false,
    };
    let Some(index) = catalog.get_indexes().iter().find(|idx| idx.id == index_id) else {
        return false;
    };
    let Some(col_id) = index.column_ids.first() else {
        return false;
    };
    let Some(col) = scan.bound.table.columns.iter().find(|c| c.id == *col_id) else {
        return false;
    };
    if order_by.len() != 1 || order_by[0].desc {
        return false;
    }
    match &order_by[0].expr {
        Expr::QualifiedIdentifier(parts) if parts.len() >= 2 => {
            parts[0].eq_ignore_ascii_case(&scan.bound.alias)
                && parts[1].eq_ignore_ascii_case(&col.name)
        }
        Expr::Identifier(name) => name.eq_ignore_ascii_case(&col.name),
        _ => false,
    }
}

fn extract_index_predicate_rhs(
    predicate: Option<&Expr>,
    alias: &str,
    column: &str,
) -> Option<(BinaryOp, Expr)> {
    let pred = predicate?;
    match pred {
        Expr::Binary { left, op, right } => {
            if let Expr::QualifiedIdentifier(parts) = left.as_ref() {
                if parts.len() >= 2
                    && parts[0].eq_ignore_ascii_case(alias)
                    && parts[1].eq_ignore_ascii_case(column)
                    && is_supported_index_op(*op)
                {
                    return Some((*op, (*right.clone())));
                }
            }
            if let Expr::QualifiedIdentifier(parts) = right.as_ref() {
                if parts.len() >= 2
                    && parts[0].eq_ignore_ascii_case(alias)
                    && parts[1].eq_ignore_ascii_case(column)
                    && is_supported_index_op(*op)
                {
                    return Some((*op, (*left.clone())));
                }
            }
            if *op == BinaryOp::And {
                extract_index_predicate_rhs(Some(left), alias, column)
                    .or_else(|| extract_index_predicate_rhs(Some(right), alias, column))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn is_supported_index_op(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Gt | BinaryOp::Gte | BinaryOp::Lt | BinaryOp::Lte
    )
}

fn compare_with_op(ord: Ordering, op: BinaryOp) -> bool {
    match op {
        BinaryOp::Eq => ord == Ordering::Equal,
        BinaryOp::Gt => ord == Ordering::Greater,
        BinaryOp::Gte => ord == Ordering::Greater || ord == Ordering::Equal,
        BinaryOp::Lt => ord == Ordering::Less,
        BinaryOp::Lte => ord == Ordering::Less || ord == Ordering::Equal,
        _ => false,
    }
}
