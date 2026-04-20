use crate::ast::FromNode;
use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::error::DbError;
use crate::executor::clock::Clock;
use crate::storage::{Storage, StoredRow};
use std::collections::HashMap;

use super::super::context::ExecutionContext;
use super::super::model::{ContextTable, JoinedRow};
use super::super::physical::{PhysicalPivot, PhysicalScan, PhysicalUnpivot};
pub(crate) use super::pipeline::iterator::{
    FilterIterator, HashJoinIterator, NestedLoopJoinIterator, RowIterator, ScanIterator,
    TableScanIterator,
};
use super::plan::RelationalQuery;
use super::scan::{choose_scan_strategy, execute_scan};
use super::transformer;
use super::QueryExecutor;

pub(crate) struct FromEval {
    pub(crate) iter: Box<dyn RowIterator>,
    pub(crate) shape: Vec<ContextTable>,
}

impl FromEval {
    pub fn materialize(
        &mut self,
        ctx: &mut ExecutionContext,
        catalog: &dyn Catalog,
        storage: &dyn Storage,
        clock: &dyn Clock,
    ) -> Result<Vec<JoinedRow>, DbError> {
        let mut rows = Vec::new();
        while let Some(row) = self.iter.next_row(ctx, catalog, storage, clock)? {
            rows.push(row);
        }
        Ok(rows)
    }
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
            apply_from_alias(executor, source_eval, &alias, ctx)
        }
        FromNode::Join {
            left,
            join_type,
            right,
            on,
        } => {
            let left_eval = execute_from_clause(executor, *left, ctx)?;
            let right_eval = execute_from_clause(executor, *right, ctx)?;

            let mut shape = left_eval.shape.clone();
            shape.extend(right_eval.shape.clone());

            // Try Hash Join for equi-joins
            if let Some(on_expr) = &on {
                if let Some((left_keys, right_keys)) =
                    crate::executor::joins::find_equi_join_conditions(
                        on_expr,
                        &left_eval.shape,
                        &right_eval.shape,
                    )
                {
                    return Ok(FromEval {
                        iter: Box::new(HashJoinIterator {
                            left: left_eval.iter,
                            right: right_eval.iter,
                            left_keys,
                            right_keys,
                            join_type,
                            left_shape: left_eval.shape,
                            right_shape: right_eval.shape,
                            build_done: false,
                            right_materialized: Vec::new(),
                            hash_map: HashMap::new(),
                            right_matched: Vec::new(),
                            current_left: None,
                            current_matches: Vec::new(),
                            current_match_idx: 0,
                            finishing_right: false,
                            finishing_idx: 0,
                        }),
                        shape,
                    });
                }
            }

            // Fallback to Nested Loop Join
            Ok(FromEval {
                iter: Box::new(NestedLoopJoinIterator {
                    left: left_eval.iter,
                    right: right_eval.iter,
                    current_left: None,
                    join_type,
                    on,
                    right_shape: right_eval.shape,
                    matched_current_left: false,
                }),
                shape,
            })
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

    // Check if it's a CTE
    if let Some(cte) = ctx
        .row
        .ctes
        .get(&crate::executor::string_norm::normalize_identifier(
            &bound.table.name,
        ))
    {
        let rows = crate::executor::cte::cte_to_context_rows(cte, &bound.alias);
        let shape = rows.first().cloned().unwrap_or_else(|| {
            vec![ContextTable {
                table: bound.table.clone(),
                alias: bound.alias.clone(),
                row: None,
                storage_index: None,
                source_aliases: Vec::new(),
            }
            .null_row()]
        });
        return Ok(FromEval {
            iter: Box::new(ScanIterator::new(rows)),
            shape,
        });
    }

    // Check if it's virtual rows (VALUES or metadata virtual tables)
    if let Some(rows) = &bound.virtual_rows {
        let ctx_rows: Vec<JoinedRow> = rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                vec![ContextTable {
                    table: bound.table.clone(),
                    alias: bound.alias.clone(),
                    row: Some(row.clone()),
                    storage_index: Some(i),
                    source_aliases: Vec::new(),
                }]
            })
            .collect();
        let mut rows = ctx_rows;

        if let Some(pivot) = &table_ref.pivot {
            rows = transformer::execute_pivot(
                executor.catalog,
                executor.storage,
                executor.clock,
                rows,
                &PhysicalPivot {
                    spec: (**pivot).clone(),
                    alias: pivot.alias.clone().unwrap_or_else(|| "pivoted".to_string()),
                },
                ctx,
            )?;
        }

        if let Some(unpivot) = &table_ref.unpivot {
            let unpivot_alias = unpivot
                .alias
                .clone()
                .unwrap_or_else(|| "unpivoted".to_string());
            let mut source_aliases: Vec<String> = rows
                .first()
                .map(|r| {
                    r.iter()
                        .flat_map(|ct| {
                            let mut aliases = vec![ct.alias.clone()];
                            if !ct.alias.eq_ignore_ascii_case(&ct.table.name) {
                                aliases.push(ct.table.name.clone());
                            }
                            aliases
                        })
                        .collect()
                })
                .unwrap_or_default();
            if let Some(sa) = &unpivot.source_alias {
                if !source_aliases.iter().any(|a| a.eq_ignore_ascii_case(sa)) {
                    source_aliases.push(sa.clone());
                }
            }
            rows = transformer::execute_unpivot(
                rows,
                &PhysicalUnpivot {
                    spec: (**unpivot).clone(),
                    alias: unpivot_alias,
                    source_aliases,
                },
                ctx,
            )?;
        }

        let shape = rows.first().cloned().unwrap_or_else(|| {
            vec![ContextTable {
                table: bound.table.clone(),
                alias: bound.alias.clone(),
                row: None,
                storage_index: None,
                source_aliases: Vec::new(),
            }
            .null_row()]
        });
        return Ok(FromEval {
            iter: Box::new(ScanIterator::new(rows)),
            shape,
        });
    }

    let strategy = choose_scan_strategy(&bound, None, &[], executor.catalog);

    // Only use truly lazy TableScanIterator if it's a simple TableScan strategy
    // and we don't have complex PIVOT/UNPIVOT (which still materialize for now)
    if matches!(strategy, crate::executor::physical::ScanStrategy::TableScan)
        && table_ref.pivot.is_none()
        && table_ref.unpivot.is_none()
    {
        let table_iter = TableScanIterator {
            bound: bound.clone(),
            next_index: 0,
        };

        let shape = vec![ContextTable {
            table: bound.table.clone(),
            alias: bound.alias.clone(),
            row: None,
            storage_index: None,
            source_aliases: Vec::new(),
        }
        .null_row()];

        return Ok(FromEval {
            iter: Box::new(table_iter),
            shape,
        });
    }

    // Fallback to materialized execute_scan for complex cases (IndexSeek, Pivot, etc.)
    let base_shape = vec![ContextTable {
        table: bound.table.clone(),
        alias: bound.alias.clone(),
        row: None,
        storage_index: None,
        source_aliases: Vec::new(),
    }
    .null_row()];
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
                alias: pivot.alias.clone().unwrap_or_else(|| "pivoted".to_string()),
            },
            ctx,
        )?;
    }

    if let Some(unpivot) = &table_ref.unpivot {
        let unpivot_alias = unpivot
            .alias
            .clone()
            .unwrap_or_else(|| "unpivoted".to_string());
        let mut source_aliases: Vec<String> = rows
            .first()
            .map(|r| {
                r.iter()
                    .flat_map(|ct| {
                        let mut aliases = vec![ct.alias.clone()];
                        if !ct.alias.eq_ignore_ascii_case(&ct.table.name) {
                            aliases.push(ct.table.name.clone());
                        }
                        aliases
                    })
                    .collect()
            })
            .unwrap_or_default();
        if let Some(sa) = &unpivot.source_alias {
            if !source_aliases.iter().any(|a| a.eq_ignore_ascii_case(sa)) {
                source_aliases.push(sa.clone());
            }
        }
        rows = transformer::execute_unpivot(
            rows,
            &PhysicalUnpivot {
                spec: (**unpivot).clone(),
                alias: unpivot_alias,
                source_aliases,
            },
            ctx,
        )?;
    }

    let shape = rows.first().cloned().unwrap_or(base_shape);
    Ok(FromEval {
        iter: Box::new(ScanIterator::new(rows)),
        shape,
    })
}

fn apply_from_alias(
    executor: &QueryExecutor<'_>,
    mut source: FromEval,
    alias: &str,
    ctx: &mut ExecutionContext,
) -> Result<FromEval, DbError> {
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
                collation: None,
                is_clustered: false,
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

    let source_rows =
        source.materialize(ctx, executor.catalog, executor.storage, executor.clock)?;
    let mut aliased_rows = Vec::with_capacity(source_rows.len());
    for row in source_rows {
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
            source_aliases: Vec::new(),
        }]);
    }

    let shape = vec![ContextTable {
        table: alias_table,
        alias: alias.to_string(),
        row: None,
        storage_index: None,
        source_aliases: Vec::new(),
    }
    .null_row()];

    Ok(FromEval {
        iter: Box::new(ScanIterator::new(aliased_rows)),
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
