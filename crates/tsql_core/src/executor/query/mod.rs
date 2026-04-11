pub(crate) mod binder;
pub(crate) mod projection;
pub(crate) mod transformer;

use crate::ast::{Expr, FromNode, SelectStmt, TableRef};
use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};

use super::aggregates::is_aggregate_function;
use super::clock::Clock;
use super::context::ExecutionContext;
use super::grouping::GroupExecutor;
use super::joins::apply_join;
use super::model::{BoundTable, ContextTable, JoinedRow};
use super::planner::{PhysicalPivot, PhysicalScan, PhysicalUnpivot, ScanStrategy};
use super::query_planner::execute_scan;
use super::window::{has_window_function, WindowExecutor};

struct FromEval {
    rows: Vec<JoinedRow>,
    shape: Vec<ContextTable>,
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

        let mut source_eval = if let Some(from_clause) = stmt.from_clause.clone() {
            self.execute_from_node(from_clause, ctx)?
        } else {
            FromEval {
                rows: vec![vec![]],
                shape: vec![],
            }
        };

        for apply_clause in &stmt.applies {
            source_eval.rows =
                transformer::execute_apply(source_eval.rows, apply_clause, ctx, |s, c| {
                    self.execute_select(s, c)
                })?;
            source_eval.shape = source_eval.rows.first().cloned().unwrap_or_default();
        }

        if let Some(where_clause) = &stmt.selection {
            let bound_where = super::binder::bind_expr(where_clause, &source_eval.shape, ctx)
                .unwrap_or_else(|_| super::binder::BoundExpr::Dynamic(where_clause.clone()));

            let mut filtered = Vec::new();
            for row in source_eval.rows {
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
            source_eval.rows = filtered;
        }

        self.execute_rows_to_result(stmt, source_eval.rows, ctx)
    }

    pub fn execute_to_joined_rows(
        &self,
        stmt: SelectStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Vec<JoinedRow>, DbError> {
        let mut source_eval = if let Some(from_clause) = stmt.from_clause {
            self.execute_from_node(from_clause, ctx)?
        } else {
            FromEval {
                rows: vec![vec![]],
                shape: vec![],
            }
        };

        for apply_clause in &stmt.applies {
            source_eval.rows =
                transformer::execute_apply(source_eval.rows, apply_clause, ctx, |s, c| {
                    self.execute_select(s, c)
                })?;
            source_eval.shape = source_eval.rows.first().cloned().unwrap_or_default();
        }

        if let Some(where_clause) = &stmt.selection {
            let bound_where = super::binder::bind_expr(where_clause, &source_eval.shape, ctx)
                .unwrap_or_else(|_| super::binder::BoundExpr::Dynamic(where_clause.clone()));
            let mut filtered = Vec::new();
            for row in source_eval.rows {
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
            source_eval.rows = filtered;
        }

        Ok(source_eval.rows)
    }

    fn execute_rows_to_result(
        &self,
        stmt: SelectStmt,
        mut source_rows: Vec<JoinedRow>,
        ctx: &mut ExecutionContext,
    ) -> Result<super::result::QueryResult, DbError> {
        let has_aggregate = stmt
            .projection
            .iter()
            .any(|item| matches!(&item.expr, Expr::FunctionCall { name, .. } if is_aggregate_function(name)));

        let has_window = stmt
            .projection
            .iter()
            .any(|item| has_window_function(&item.expr));

        let projection_columns: Vec<String> = stmt
            .projection
            .iter()
            .flat_map(|item| super::projection::expand_projection_columns(&[item.clone()], None))
            .collect();

        let needs_pre_sort = !stmt.order_by.is_empty()
            && !has_aggregate
            && stmt
                .order_by
                .iter()
                .any(|ob| super::projection::resolve_projected_order_index(&projection_columns, ob).is_none());

        if needs_pre_sort {
            let order_by = &stmt.order_by;
            source_rows.sort_by(|a, b| {
                for item in order_by {
                    let va = super::evaluator::eval_expr(
                        &item.expr,
                        a,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )
                    .unwrap_or(crate::types::Value::Null);
                    let vb = super::evaluator::eval_expr(
                        &item.expr,
                        b,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )
                    .unwrap_or(crate::types::Value::Null);
                    let ord = super::value_ops::compare_values(&va, &vb);
                    if ord != std::cmp::Ordering::Equal {
                        return if item.asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let result = if !stmt.group_by.is_empty() || has_aggregate {
            let group_executor = GroupExecutor {
                catalog: self.catalog,
                storage: self.storage,
                clock: self.clock,
            };
            group_executor.execute_grouped_select(
                stmt.projection.clone(),
                source_rows,
                stmt.group_by.clone(),
                stmt.having.clone(),
                ctx,
            )?
        } else if has_window {
            let window_executor = WindowExecutor::new(self.catalog, self.storage, self.clock);
            window_executor.execute(&stmt.projection, source_rows, ctx)?
        } else {
            projection::execute_flat_select(
                self.catalog,
                self.storage,
                self.clock,
                stmt.projection.clone(),
                source_rows,
                ctx,
            )?
        };

        let mut final_rows = result.rows;

        if stmt.distinct {
            final_rows = super::projection::deduplicate_projected_rows(final_rows);
        }

        if !stmt.order_by.is_empty() && !needs_pre_sort {
            let columns = &result.columns;
            let order_by_refs = &stmt.order_by;
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

        if let Some(top) = stmt.top {
            let n = super::projection::eval_top_n(
                &top,
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
            if final_rows.len() > n {
                final_rows.truncate(n);
            }
        }

        if let Some(offset_expr) = stmt.offset {
            let offset_val = super::evaluator::eval_expr(
                &offset_expr,
                &[],
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
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
            if let Some(fetch_expr) = stmt.fetch {
                let fetch_val = super::evaluator::eval_expr(
                    &fetch_expr,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
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

    fn execute_from_node(
        &self,
        node: FromNode,
        ctx: &mut ExecutionContext,
    ) -> Result<FromEval, DbError> {
        match node {
            FromNode::Table(table_ref) => self.execute_table_ref(table_ref, ctx),
            FromNode::Aliased { source, alias } => {
                let source_eval = self.execute_from_node(*source, ctx)?;
                self.apply_from_alias(source_eval, &alias)
            }
            FromNode::Join {
                left,
                join_type,
                right,
                on,
            } => {
                let left_eval = self.execute_from_node(*left, ctx)?;
                let right_eval = self.execute_from_node(*right, ctx)?;
                let rows = apply_join(
                    left_eval.rows,
                    &left_eval.shape,
                    right_eval.rows,
                    &right_eval.shape,
                    join_type,
                    on.as_ref(),
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                let mut shape = left_eval.shape;
                shape.extend(right_eval.shape);
                Ok(FromEval { rows, shape })
            }
        }
    }

    fn execute_table_ref(
        &self,
        table_ref: TableRef,
        ctx: &mut ExecutionContext,
    ) -> Result<FromEval, DbError> {
        let bound = self.bind_table(table_ref.clone(), self.catalog, ctx)?;
        let base_shape = vec![ContextTable {
            table: bound.table.clone(),
            alias: bound.alias.clone(),
            row: None,
            storage_index: None,
        }];
        let scan = PhysicalScan {
            bound,
            strategy: ScanStrategy::TableScan,
            pushed_predicate: None,
        };
        let mut rows = execute_scan(&scan, ctx, self.catalog, self.storage, self.clock)?;

        if let Some(pivot) = &table_ref.pivot {
            rows = transformer::execute_pivot(
                self.catalog,
                self.storage,
                self.clock,
                rows,
                &PhysicalPivot {
                    spec: (**pivot).clone(),
                    alias: table_ref
                        .alias
                        .clone()
                        .unwrap_or_else(|| "pivoted".to_string()),
                },
                ctx,
            )?;
        }

        if let Some(unpivot) = &table_ref.unpivot {
            rows = transformer::execute_unpivot(
                rows,
                &PhysicalUnpivot {
                    spec: (**unpivot).clone(),
                    alias: table_ref
                        .alias
                        .clone()
                        .unwrap_or_else(|| "unpivoted".to_string()),
                },
                ctx,
            )?;
        }

        let shape = rows.first().cloned().unwrap_or(base_shape);
        Ok(FromEval { rows, shape })
    }

    fn apply_from_alias(&self, source: FromEval, alias: &str) -> Result<FromEval, DbError> {
        let mut columns = Vec::new();
        for ctx_table in &source.shape {
            for col in &ctx_table.table.columns {
                columns.push(ColumnDef {
                    id: (columns.len() + 1) as u32,
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    identity: None,
                    default: None,
                    default_constraint_name: None,
                    check: None,
                    check_constraint_name: None,
                    computed_expr: None,
                    ansi_padding_on: true,
                });
            }
        }
        let alias_table = TableDef {
            id: 0,
            schema_id: 1,
            schema_name: "dbo".to_string(),
            name: alias.to_string(),
            columns,
            check_constraints: vec![],
            foreign_keys: vec![],
        };

        let mut aliased_rows = Vec::with_capacity(source.rows.len());
        for row in source.rows {
            let mut values = Vec::new();
            for ctx_table in &row {
                if let Some(stored) = &ctx_table.row {
                    values.extend(stored.values.clone());
                } else {
                    values.extend((0..ctx_table.table.columns.len()).map(|_| crate::types::Value::Null));
                }
            }
            aliased_rows.push(vec![ContextTable {
                table: alias_table.clone(),
                alias: alias.to_string(),
                row: Some(StoredRow {
                    values,
                    deleted: false,
                }),
                storage_index: None,
            }]);
        }

        let shape = vec![ContextTable {
            table: alias_table,
            alias: alias.to_string(),
            row: None,
            storage_index: None,
        }];

        Ok(FromEval {
            rows: aliased_rows,
            shape,
        })
    }

    fn bind_table(
        &self,
        tref: crate::ast::TableRef,
        catalog: &dyn Catalog,
        ctx: &mut ExecutionContext,
    ) -> Result<BoundTable, DbError> {
        binder::bind_table(catalog, self.storage, self.clock, tref, ctx, |s, c| {
            self.execute_select(s, c)
        })
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
        cost += count_joins(&stmt.from_clause) as i64;
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

fn count_joins(from_clause: &Option<FromNode>) -> usize {
    match from_clause {
        None => 0,
        Some(FromNode::Table(_)) => 0,
        Some(FromNode::Aliased { source, .. }) => count_joins(&Some((**source).clone())),
        Some(FromNode::Join { left, right, .. }) => {
            1 + count_joins(&Some((**left).clone())) + count_joins(&Some((**right).clone()))
        }
    }
}
