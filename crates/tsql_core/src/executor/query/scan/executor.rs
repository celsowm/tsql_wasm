use std::cmp::Ordering;

use crate::ast::BinaryOp;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::types::Value;

use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::model::{BoundTable, JoinedRow};
use crate::executor::physical::{PhysicalScan, ScanStrategy};
use crate::executor::value_ops::compare_values;
use crate::storage::IndexStorage;

pub(crate) fn execute_scan(
    scan: &PhysicalScan,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let rows = bind_table_rows(&scan.bound, ctx, storage)?;
    let mut scanned = match scan.strategy {
        ScanStrategy::TableScan => rows,
        ScanStrategy::IndexSeek { .. } | ScanStrategy::IndexScan { .. } => {
            apply_index_strategy(rows, scan, ctx, catalog, storage, clock)?
        }
    };
    if let Some(predicate) = &scan.pushed_predicate {
        scanned.retain(|row| {
            crate::executor::evaluator::eval_predicate(predicate, row, ctx, catalog, storage, clock)
                .unwrap_or(false)
        });
    }
    Ok(scanned)
}

fn bind_table_rows(
    bound: &BoundTable,
    ctx: &ExecutionContext,
    storage: &dyn crate::storage::Storage,
) -> Result<Vec<JoinedRow>, DbError> {
    if let Some(cte) = ctx
        .row
        .ctes
        .get(&crate::executor::string_norm::normalize_identifier(
            &bound.table.name,
        ))
    {
        return Ok(crate::executor::cte::cte_to_context_rows(cte, &bound.alias));
    }

    if let Some(rows) = &bound.virtual_rows {
        return Ok(rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                vec![crate::executor::model::ContextTable {
                    table: bound.table.clone(),
                    alias: bound.alias.clone(),
                    row: Some(row.clone()),
                    storage_index: Some(i),
                    source_aliases: Vec::new(),
                }]
            })
            .collect());
    }

    let stored_rows = storage.scan_rows(bound.table.id)?;
    let mut rows = Vec::new();

    for (i, row) in stored_rows.enumerate() {
        let row = row?;
        if row.deleted {
            continue;
        }
        rows.push(vec![crate::executor::model::ContextTable {
            table: bound.table.clone(),
            alias: bound.alias.clone(),
            row: Some(row),
            storage_index: Some(i),
            source_aliases: Vec::new(),
        }]);
    }

    Ok(rows)
}

fn apply_index_strategy(
    rows: Vec<JoinedRow>,
    scan: &PhysicalScan,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let index_id = match scan.strategy {
        ScanStrategy::IndexSeek { index_id } | ScanStrategy::IndexScan { index_id } => index_id,
        ScanStrategy::TableScan => return Ok(rows),
    };

    if let Some(index_storage) = storage.as_index_storage() {
        return apply_index_strategy_indexed(
            rows,
            scan,
            ctx,
            catalog,
            storage,
            index_storage,
            clock,
        );
    }

    let Some(index) = catalog
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
        if let Some((op, rhs)) = super::strategy::extract_index_predicate_rhs(
            scan.pushed_predicate.as_ref(),
            &scan.bound.alias,
            &scan.bound.table.columns[col_idx].name,
        ) {
            let rhs_val = eval_expr(&rhs, &[], ctx, catalog, storage, clock)?;
            keyed
                .into_iter()
                .filter(|(lhs, _)| {
                    matches!(op, BinaryOp::Eq) && compare_values(lhs, &rhs_val) == Ordering::Equal
                })
                .map(|(_, row)| row)
                .collect()
        } else {
            keyed.into_iter().map(|(_, row)| row).collect()
        }
    } else if let Some((op, rhs)) = super::strategy::extract_index_predicate_rhs(
        scan.pushed_predicate.as_ref(),
        &scan.bound.alias,
        &scan.bound.table.columns[col_idx].name,
    ) {
        let rhs_val = eval_expr(&rhs, &[], ctx, catalog, storage, clock)?;
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

fn apply_index_strategy_indexed(
    _rows: Vec<JoinedRow>,
    scan: &PhysicalScan,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn crate::storage::Storage,
    index_storage: &dyn IndexStorage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let index_id = match scan.strategy {
        ScanStrategy::IndexSeek { index_id } | ScanStrategy::IndexScan { index_id } => index_id,
        ScanStrategy::TableScan => return Ok(Vec::new()),
    };

    let Some(first_col_id) = catalog
        .get_indexes()
        .iter()
        .find(|idx| idx.id == index_id && idx.table_id == scan.bound.table.id)
        .and_then(|idx| idx.column_ids.first().copied())
    else {
        return Ok(Vec::new());
    };

    let Some(col_idx) = scan
        .bound
        .table
        .columns
        .iter()
        .position(|c| c.id == first_col_id)
    else {
        return Ok(Vec::new());
    };

    let row_indices = if matches!(scan.strategy, ScanStrategy::IndexSeek { .. }) {
        if let Some((op, rhs)) = super::strategy::extract_index_predicate_rhs(
            scan.pushed_predicate.as_ref(),
            &scan.bound.alias,
            &scan.bound.table.columns[col_idx].name,
        ) {
            let rhs_val = eval_expr(&rhs, &[], ctx, catalog, storage, clock)?;
            match op {
                BinaryOp::Eq => index_storage.seek_index(index_id, &rhs_val)?,
                BinaryOp::Gt => {
                    let results = index_storage.seek_index_range(index_id, Some(&rhs_val), None)?;
                    results
                        .into_iter()
                        .flat_map(|(_, indices)| indices)
                        .collect()
                }
                BinaryOp::Gte => {
                    let results = index_storage.seek_index_range(index_id, Some(&rhs_val), None)?;
                    results
                        .into_iter()
                        .flat_map(|(_, indices)| indices)
                        .collect()
                }
                BinaryOp::Lt => {
                    let results = index_storage.seek_index_range(index_id, None, Some(&rhs_val))?;
                    results
                        .into_iter()
                        .flat_map(|(_, indices)| indices)
                        .collect()
                }
                BinaryOp::Lte => {
                    let results = index_storage.seek_index_range(index_id, None, Some(&rhs_val))?;
                    results
                        .into_iter()
                        .flat_map(|(_, indices)| indices)
                        .collect()
                }
                _ => Vec::new(),
            }
        } else {
            let all = index_storage.seek_index_range(index_id, None, None)?;
            all.into_iter().flat_map(|(_, indices)| indices).collect()
        }
    } else {
        let all = index_storage.seek_index_range(index_id, None, None)?;
        all.into_iter().flat_map(|(_, indices)| indices).collect()
    };

    Ok(row_indices
        .into_iter()
        .map(|idx| {
            vec![crate::executor::model::ContextTable {
                table: scan.bound.table.clone(),
                alias: scan.bound.alias.clone(),
                row: None,
                storage_index: Some(idx),
                source_aliases: Vec::new(),
            }]
        })
        .collect())
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
