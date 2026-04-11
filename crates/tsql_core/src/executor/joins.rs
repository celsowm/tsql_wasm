use crate::ast::{Expr, JoinType};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_predicate;
use super::model::{ContextTable, JoinedRow};

#[allow(clippy::too_many_arguments)]
pub fn apply_join(
    left_rows: Vec<JoinedRow>,
    left_shape: &[ContextTable],
    right_rows: Vec<JoinedRow>,
    right_shape: &[ContextTable],
    join_type: JoinType,
    on: Option<&Expr>,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    match join_type {
        JoinType::Cross => apply_cross_join(left_rows, right_rows),
        JoinType::Inner | JoinType::Left => apply_join_left(
            left_rows,
            right_rows,
            right_shape,
            join_type,
            on,
            ctx,
            catalog,
            storage,
            clock,
        ),
        JoinType::Right => apply_join_right(
            left_rows, left_shape, right_rows, on, ctx, catalog, storage, clock,
        ),
        JoinType::Full => apply_join_full(
            left_rows,
            left_shape,
            right_rows,
            right_shape,
            on,
            ctx,
            catalog,
            storage,
            clock,
        ),
    }
}

fn apply_cross_join(left_rows: Vec<JoinedRow>, right_rows: Vec<JoinedRow>) -> Result<Vec<JoinedRow>, DbError> {
    let mut next_rows = Vec::new();
    for left_row in &left_rows {
        for right_row in &right_rows {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            next_rows.push(candidate);
        }
    }
    Ok(next_rows)
}

#[allow(clippy::too_many_arguments)]
fn apply_join_left(
    left_rows: Vec<JoinedRow>,
    right_rows: Vec<JoinedRow>,
    right_shape: &[ContextTable],
    join_type: JoinType,
    on: Option<&Expr>,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let on_expr = on.ok_or_else(|| DbError::Parse("JOIN requires ON clause".into()))?;
    let mut next_rows = Vec::new();

    for left_row in left_rows {
        let mut matched = false;
        for right_row in &right_rows {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(on_expr, &candidate, ctx, catalog, storage, clock)? {
                matched = true;
                next_rows.push(candidate);
            }
        }

        if !matched && join_type == JoinType::Left {
            let mut candidate = left_row.clone();
            candidate.extend(right_shape.iter().map(ContextTable::null_row));
            next_rows.push(candidate);
        }
    }

    Ok(next_rows)
}

#[allow(clippy::too_many_arguments)]
fn apply_join_right(
    left_rows: Vec<JoinedRow>,
    left_shape: &[ContextTable],
    right_rows: Vec<JoinedRow>,
    on: Option<&Expr>,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let on_expr = on.ok_or_else(|| DbError::Parse("JOIN requires ON clause".into()))?;
    let mut next_rows = Vec::new();

    for right_row in &right_rows {
        let mut matched = false;
        for left_row in &left_rows {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(on_expr, &candidate, ctx, catalog, storage, clock)? {
                matched = true;
                next_rows.push(candidate);
            }
        }

        if !matched {
            let mut candidate: JoinedRow = left_shape.iter().map(ContextTable::null_row).collect();
            candidate.extend(right_row.clone());
            next_rows.push(candidate);
        }
    }

    Ok(next_rows)
}

#[allow(clippy::too_many_arguments)]
fn apply_join_full(
    left_rows: Vec<JoinedRow>,
    left_shape: &[ContextTable],
    right_rows: Vec<JoinedRow>,
    right_shape: &[ContextTable],
    on: Option<&Expr>,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Vec<JoinedRow>, DbError> {
    let on_expr = on.ok_or_else(|| DbError::Parse("JOIN requires ON clause".into()))?;
    let mut next_rows = Vec::new();
    let mut matched_right: Vec<bool> = vec![false; right_rows.len()];

    for left_row in &left_rows {
        let mut matched = false;
        for (ri, right_row) in right_rows.iter().enumerate() {
            let mut candidate = left_row.clone();
            candidate.extend(right_row.clone());
            if eval_predicate(on_expr, &candidate, ctx, catalog, storage, clock)? {
                matched = true;
                matched_right[ri] = true;
                next_rows.push(candidate);
            }
        }

        if !matched {
            let mut candidate = left_row.clone();
            candidate.extend(right_shape.iter().map(ContextTable::null_row));
            next_rows.push(candidate);
        }
    }

    for (ri, matched) in matched_right.iter().enumerate() {
        if !matched {
            let mut candidate: JoinedRow = left_shape.iter().map(ContextTable::null_row).collect();
            candidate.extend(right_rows[ri].clone());
            next_rows.push(candidate);
        }
    }

    Ok(next_rows)
}
