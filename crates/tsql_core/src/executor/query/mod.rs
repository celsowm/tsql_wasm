pub(crate) mod binder;
pub(crate) mod transformer;
pub(crate) mod projection;

use crate::ast::{Expr, SelectStmt};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;

use super::aggregates::is_aggregate_function;
use super::clock::Clock;
use super::context::ExecutionContext;
use super::grouping::GroupExecutor;
use super::joins::apply_join;
use super::model::{BoundTable, JoinedRow};
use super::planner::PhysicalPlan;
use super::query_planner::{build_logical_plan, build_physical_plan, execute_scan};
use super::window::{has_window_function, WindowExecutor};

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
        let into_table = stmt.into_table.clone();
        let result = self.execute_select_internal(stmt, ctx)?;
        let mut result = result;

        if ctx.options.rowcount > 0 && result.rows.len() > ctx.options.rowcount as usize {
            result.rows.truncate(ctx.options.rowcount as usize);
        }

        if into_table.is_some() {
            return Err(DbError::Execution("SELECT INTO is handled by ScriptExecutor".into()));
        }

        Ok(result)
    }

    fn execute_select_internal(
        &self,
        stmt: SelectStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        self.enforce_query_governor_cost_limit(&stmt, ctx)?;

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
                projection::execute_flat_select(
                    self.catalog,
                    self.storage,
                    self.clock,
                    stmt.projection,
                    source_rows,
                    ctx,
                )?
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

        for apply_clause in &plan.applies {
            source_rows = transformer::execute_apply(source_rows, apply_clause, ctx, |s, c| self.execute_select(s, c))?;
        }

        for pivot in &plan.pivots {
            source_rows = transformer::execute_pivot(self.catalog, self.storage, self.clock, source_rows, pivot, ctx)?;
        }

        for unpivot in &plan.unpivots {
            source_rows = transformer::execute_unpivot(source_rows, unpivot, ctx)?;
        }

        if let Some(where_clause) = &plan.residual_filter {
            let sample_row = build_sample_row(&plan);
            let bound_where = super::binder::bind_expr(where_clause, &sample_row, ctx)
                .unwrap_or_else(|_| super::binder::BoundExpr::Dynamic(where_clause.clone()));

            let mut filtered = Vec::new();
            for row in source_rows {
                if super::value_ops::truthy(&super::binder::eval_bound_expr(
                    &bound_where,
                    &row,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?) {
                    filtered.push(row);
                }
            }
            source_rows = filtered;
        }

        self.execute_physical_plan_to_result(plan, source_rows, ctx)
    }

    pub fn execute_to_joined_rows(
        &self,
        stmt: SelectStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<JoinedRow>, DbError> {
        let logical = build_logical_plan(&stmt)?;
        let plan = build_physical_plan(
            &stmt,
            &logical,
            self.catalog,
            ctx,
            |tref, cat, c| self.bind_table(tref, cat, c),
        )?;

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

        for apply_clause in &plan.applies {
            source_rows = transformer::execute_apply(source_rows, apply_clause, ctx, |s, c| self.execute_select(s, c))?;
        }

        for pivot in &plan.pivots {
            source_rows = transformer::execute_pivot(self.catalog, self.storage, self.clock, source_rows, pivot, ctx)?;
        }

        for unpivot in &plan.unpivots {
            source_rows = transformer::execute_unpivot(source_rows, unpivot, ctx)?;
        }

        if let Some(where_clause) = &plan.residual_filter {
            let sample_row = build_sample_row(&plan);
            let bound_where = super::binder::bind_expr(where_clause, &sample_row, ctx)
                .unwrap_or_else(|_| super::binder::BoundExpr::Dynamic(where_clause.clone()));

            let mut filtered = Vec::new();
            for row in source_rows {
                if super::value_ops::truthy(&super::binder::eval_bound_expr(
                    &bound_where,
                    &row,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?) {
                    filtered.push(row);
                }
            }
            source_rows = filtered;
        }

        Ok(source_rows)
    }

    fn execute_physical_plan_to_result(
        &self,
        plan: PhysicalPlan,
        mut source_rows: Vec<JoinedRow>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let has_aggregate = plan
            .projection
            .iter()
            .any(|item| matches!(&item.expr, Expr::FunctionCall { name, .. } if is_aggregate_function(name)));

        let has_window = plan
            .projection
            .iter()
            .any(|item| has_window_function(&item.expr));

        let projection_columns: Vec<String> = plan
            .projection
            .iter()
            .map(|item| super::projection::expand_projection_columns(&[item.clone()], None))
            .flatten()
            .collect();

        let needs_pre_sort = !plan.order_by.is_empty()
            && !plan.order_satisfied_by_scan
            && !has_aggregate
            && plan.order_by.iter().any(|ob| {
                let idx = super::projection::resolve_projected_order_index(&projection_columns, ob);
                idx.is_none()
            });

        if needs_pre_sort {
            let order_by = &plan.order_by;
            source_rows.sort_by(|a, b| {
                for item in order_by {
                    let va = super::evaluator::eval_expr(&item.expr, a, ctx, self.catalog, self.storage, self.clock)
                        .unwrap_or(crate::types::Value::Null);
                    let vb = super::evaluator::eval_expr(&item.expr, b, ctx, self.catalog, self.storage, self.clock)
                        .unwrap_or(crate::types::Value::Null);
                    let ord = super::value_ops::compare_values(&va, &vb);
                    if ord != std::cmp::Ordering::Equal {
                        return if item.asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

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
        } else if has_window {
            let window_executor = WindowExecutor::new(self.catalog, self.storage, self.clock);
            window_executor.execute(&plan.projection, source_rows, ctx)?
        } else {
            projection::execute_flat_select(
                self.catalog,
                self.storage,
                self.clock,
                plan.projection,
                source_rows,
                ctx,
            )?
        };

        let mut final_rows = result.rows;
        
        if plan.distinct {
            final_rows = super::projection::deduplicate_projected_rows(final_rows);
        }

        if !plan.order_by.is_empty() && !plan.order_satisfied_by_scan && !needs_pre_sort {
            let columns = &result.columns;
            let order_by_refs = &plan.order_by;

            // Pre-validate order by columns to ensure they can be resolved
            for item in order_by_refs {
                if super::projection::resolve_projected_order_index(columns, item).is_none() {
                    return Err(DbError::invalid_identifier(&format!(
                        "invalid column in ORDER BY: {}",
                        super::projection::expr_label(&item.expr)
                    )));
                }
            }

            final_rows.sort_by(|a, b| {
                super::projection::compare_projected_rows(a, b, columns, order_by_refs)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        if let Some(top) = plan.top {
            let n = super::projection::eval_top_n(&top, ctx, self.catalog, self.storage, self.clock)?;
            if final_rows.len() > n {
                final_rows.truncate(n);
            }
        }

        if let Some(ref offset_expr) = plan.offset {
            let offset_val =
                super::evaluator::eval_expr(offset_expr, &[], ctx, self.catalog, self.storage, self.clock)?;
            let offset_n = match offset_val {
                crate::types::Value::Int(n) => n.max(0) as usize,
                crate::types::Value::BigInt(n) => n.max(0) as usize,
                crate::types::Value::SmallInt(n) => n.max(0) as usize,
                crate::types::Value::TinyInt(n) => n as usize,
                _ => 0,
            };
            if offset_n < final_rows.len() {
                final_rows = final_rows[offset_n..].to_vec();
            } else {
                final_rows = vec![];
            }

            if let Some(ref fetch_expr) = plan.fetch {
                let fetch_val =
                    super::evaluator::eval_expr(fetch_expr, &[], ctx, self.catalog, self.storage, self.clock)?;
                let fetch_n = match fetch_val {
                    crate::types::Value::Int(n) => n.max(0) as usize,
                    crate::types::Value::BigInt(n) => n.max(0) as usize,
                    crate::types::Value::SmallInt(n) => n.max(0) as usize,
                    crate::types::Value::TinyInt(n) => n as usize,
                    _ => 0,
                };
                if final_rows.len() > fetch_n {
                    final_rows.truncate(fetch_n);
                }
            }
        }

        Ok(super::result::QueryResult {
            columns: result.columns,
            column_types: result.column_types,
            rows: final_rows,
            ..Default::default()
        })
    }

    fn bind_table(
        &self,
        tref: crate::ast::TableRef,
        catalog: &dyn Catalog,
        ctx: &mut ExecutionContext,
    ) -> Result<BoundTable, DbError> {
        binder::bind_table(catalog, self.storage, self.clock, tref, ctx, |s, c| self.execute_select(s, c))
    }

    fn enforce_query_governor_cost_limit(
        &self,
        stmt: &SelectStmt,
        ctx: &ExecutionContext,
    ) -> Result<(), DbError> {
        let limit = ctx.options.query_governor_cost_limit;
        if limit <= 0 {
            return Ok(());
        }

        let mut cost = 1i64;
        cost += stmt.joins.len() as i64;
        cost += stmt.applies.len() as i64;
        cost += stmt.group_by.len() as i64;
        if stmt.selection.is_some() {
            cost += 1;
        }
        if stmt.having.is_some() {
            cost += 1;
        }
        if stmt.distinct {
            cost += 1;
        }
        if !stmt.order_by.is_empty() {
            cost += 1;
        }
        if stmt.top.is_some() {
            cost += 1;
        }
        if stmt.offset.is_some() {
            cost += 1;
        }
        if stmt.fetch.is_some() {
            cost += 1;
        }

        if cost > limit {
            return Err(DbError::Execution(format!(
                "Query governor cost limit {} exceeded by estimated cost {}",
                limit, cost
            )));
        }

        Ok(())
    }
}

/// Builds a sample row from the physical plan's bound tables for pre-binding expressions.
/// The sample row has the correct schema (table aliases, column names, types) but no data.
fn build_sample_row(plan: &PhysicalPlan) -> JoinedRow {
    let mut row = Vec::new();
    // Add base table
    row.push(crate::executor::model::ContextTable {
        table: plan.base.bound.table.clone(),
        alias: plan.base.bound.alias.clone(),
        row: None,
        storage_index: None,
    });
    // Add join tables
    for join_plan in &plan.joins {
        row.push(crate::executor::model::ContextTable {
            table: join_plan.right.bound.table.clone(),
            alias: join_plan.right.bound.alias.clone(),
            row: None,
            storage_index: None,
        });
    }
    row
}
