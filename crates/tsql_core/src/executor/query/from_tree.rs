use crate::ast::FromNode;
use crate::catalog::{ColumnDef, TableDef};
use crate::error::DbError;
use crate::storage::StoredRow;

use super::super::context::ExecutionContext;
use super::super::joins::apply_join;
use super::super::model::{ContextTable, JoinedRow};
use super::super::physical::{PhysicalPivot, PhysicalScan, PhysicalUnpivot};
use super::plan::RelationalQuery;
use super::scan::{choose_scan_strategy, execute_scan};
use super::transformer;
use super::QueryExecutor;

#[derive(Debug, Clone)]
pub(crate) struct FromEval {
    pub(crate) rows: Vec<JoinedRow>,
    pub(crate) shape: Vec<ContextTable>,
}

pub(crate) fn execute_from_clause(
    executor: &QueryExecutor<'_>,
    node: FromNode,
    ctx: &mut ExecutionContext,
) -> Result<FromEval, DbError> {
    match node {
        FromNode::Table(table_ref) => execute_table_ref(executor, table_ref, ctx),
        FromNode::Aliased { source, alias } => {
            let source_eval = execute_from_clause(executor, *source, ctx)?;
            apply_from_alias(source_eval, &alias)
        }
        FromNode::Join {
            left,
            join_type,
            right,
            on,
        } => {
            let left_eval = execute_from_clause(executor, *left, ctx)?;
            let right_eval = execute_from_clause(executor, *right, ctx)?;
            let rows = apply_join(
                left_eval.rows,
                &left_eval.shape,
                right_eval.rows,
                &right_eval.shape,
                join_type,
                on.as_ref(),
                ctx,
                executor.catalog,
                executor.storage,
                executor.clock,
            )?;
            let mut shape = left_eval.shape;
            shape.extend(right_eval.shape);
            Ok(FromEval { rows, shape })
        }
    }
}

pub(crate) fn enforce_query_governor_cost_limit(
    query: &RelationalQuery,
    ctx: &ExecutionContext,
) -> Result<(), DbError> {
    let limit = ctx.options.query_governor_cost_limit;
    if limit <= 0 {
        return Ok(());
    }

    let mut cost = 1i64;
    cost += count_joins(&query.from_clause) as i64;
    cost += query.applies.len() as i64;
    cost += query.filter.group_by.len() as i64;
    if query.filter.selection.is_some() {
        cost += 1;
    }
    if query.filter.having.is_some() {
        cost += 1;
    }
    if query.projection.distinct {
        cost += 1;
    }
    if !query.sort.order_by.is_empty() {
        cost += 1;
    }
    if query.pagination.top.is_some() {
        cost += 1;
    }
    if query.pagination.offset.is_some() {
        cost += 1;
    }
    if query.pagination.fetch.is_some() {
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

fn execute_table_ref(
    executor: &QueryExecutor<'_>,
    table_ref: crate::ast::TableRef,
    ctx: &mut ExecutionContext,
) -> Result<FromEval, DbError> {
    let bound = super::binding::bind_table(executor, executor.catalog, table_ref.clone(), ctx)?;
    let base_shape = vec![ContextTable {
        table: bound.table.clone(),
        alias: bound.alias.clone(),
        row: None,
        storage_index: None,
    }
    .null_row()];
    let strategy = choose_scan_strategy(&bound, None, &[], executor.catalog);
    let scan = PhysicalScan {
        bound,
        strategy,
        pushed_predicate: None,
    };
    let mut rows = execute_scan(
        &scan,
        ctx,
        executor.catalog,
        executor.storage,
        executor.clock,
    )?;

    if let Some(pivot) = &table_ref.pivot {
        rows = transformer::execute_pivot(
            executor.catalog,
            executor.storage,
            executor.clock,
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

fn apply_from_alias(source: FromEval, alias: &str) -> Result<FromEval, DbError> {
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
                values
                    .extend((0..ctx_table.table.columns.len()).map(|_| crate::types::Value::Null));
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
    }
    .null_row()];

    Ok(FromEval {
        rows: aliased_rows,
        shape,
    })
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
