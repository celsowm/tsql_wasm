use crate::ast::{JoinClause, JoinType};
use crate::error::DbError;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_predicate;
use super::model::{BoundTable, ContextTable, JoinedRow};
use crate::catalog::Catalog;
use crate::storage::Storage;

pub fn apply_join(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    right: BoundTable,
    join: &JoinClause,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    match join.join_type {
        JoinType::Inner | JoinType::Left => {
            apply_join_left(rows, right_rows, right, join, ctx, catalog, storage, clock)
        }
        JoinType::Right => {
            apply_join_right(rows, right_rows, right, join, ctx, catalog, storage, clock)
        }
        JoinType::Full => {
            apply_join_full(rows, right_rows, right, join, ctx, catalog, storage, clock)
        }
    }
}

fn apply_join_left(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    right: BoundTable,
    join: &JoinClause,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut next_rows = Vec::new();

    for left_row in rows {
        let mut matched = false;
        for right_row in &right_rows {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(&join.on, &candidate, ctx, catalog, storage, clock)? {
                matched = true;
                next_rows.push(candidate);
            }
        }

        if !matched && join.join_type == JoinType::Left {
            let mut candidate = left_row.clone();
            candidate.push(ContextTable {
                table: right.table.clone(),
                alias: right.alias.clone(),
                row: None,
            });
            next_rows.push(candidate);
        }
    }

    Ok(next_rows)
}

fn apply_join_right(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    _right: BoundTable,
    join: &JoinClause,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut next_rows = Vec::new();

    for right_row in &right_rows {
        let mut matched = false;
        for left_row in &rows {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(&join.on, &candidate, ctx, catalog, storage, clock)? {
                matched = true;
                next_rows.push(candidate);
            }
        }

        if !matched {
            let left_table = rows
                .first()
                .and_then(|r| r.first())
                .map(|ctx| (ctx.table.clone(), ctx.alias.clone()));
            if let Some((table, alias)) = left_table {
                let mut candidate = vec![ContextTable {
                    table,
                    alias,
                    row: None,
                }];
                candidate.extend(right_row.clone());
                next_rows.push(candidate);
            }
        }
    }

    Ok(next_rows)
}

fn apply_join_full(
    rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    right: BoundTable,
    join: &JoinClause,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let mut next_rows = Vec::new();
    let mut matched_right: Vec<bool> = vec![false; right_rows.len()];

    for left_row in &rows {
        let mut matched = false;
        for (ri, right_row) in right_rows.iter().enumerate() {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(&join.on, &candidate, ctx, catalog, storage, clock)? {
                matched = true;
                matched_right[ri] = true;
                next_rows.push(candidate);
            }
        }

        if !matched {
            let mut candidate = left_row.clone();
            candidate.push(ContextTable {
                table: right.table.clone(),
                alias: right.alias.clone(),
                row: None,
            });
            next_rows.push(candidate);
        }
    }

    let left_table = rows
        .first()
        .and_then(|r| r.first())
        .map(|ctx| (ctx.table.clone(), ctx.alias.clone()));
    for (ri, matched) in matched_right.iter().enumerate() {
        if !matched {
            if let Some((table, alias)) = &left_table {
                let mut candidate = vec![ContextTable {
                    table: table.clone(),
                    alias: alias.clone(),
                    row: None,
                }];
                candidate.extend(right_rows[ri].clone());
                next_rows.push(candidate);
            }
        }
    }

    Ok(next_rows)
}
