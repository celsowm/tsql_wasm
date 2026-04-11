use std::cmp::Ordering;

use crate::ast::{Expr, WhenClause};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
use super::model::ContextTable;
use super::operators::compare_bool;
use super::query::plan::RelationalQuery;
use super::query::QueryExecutor;
use super::result::QueryResult;
use super::value_ops::truthy;
use crate::storage::Storage;

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_case(
    operand: Option<&Expr>,
    when_clauses: &[WhenClause],
    else_result: Option<&Expr>,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let operand_val = match operand {
        Some(e) => Some(eval_expr(e, row, ctx, catalog, storage, clock)?),
        None => None,
    };

    for clause in when_clauses {
        let match_found = if let Some(ref op_val) = operand_val {
            let when_val = eval_expr(&clause.condition, row, ctx, catalog, storage, clock)?;
            compare_bool(
                op_val.clone(),
                when_val,
                |o| o == Ordering::Equal,
                ctx.metadata.ansi_nulls,
            )
        } else {
            let cond = eval_expr(&clause.condition, row, ctx, catalog, storage, clock)?;
            Value::Bit(truthy(&cond))
        };

        if let Value::Bit(true) = match_found {
            return eval_expr(&clause.result, row, ctx, catalog, storage, clock);
        }
    }

    match else_result {
        Some(expr) => eval_expr(expr, row, ctx, catalog, storage, clock),
        None => Ok(Value::Null),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_in_list(
    in_expr: &Expr,
    list: &[Expr],
    negated: bool,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let val = eval_expr(in_expr, row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let mut found = false;
    for item in list {
        let item_val = eval_expr(item, row, ctx, catalog, storage, clock)?;
        if item_val.is_null() {
            return Ok(Value::Null);
        }
        if compare_bool(
            val.clone(),
            item_val,
            |o| o == Ordering::Equal,
            ctx.metadata.ansi_nulls,
        ) == Value::Bit(true)
        {
            found = true;
            break;
        }
    }
    Ok(Value::Bit(if negated { !found } else { found }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_between(
    between_expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let val = eval_expr(between_expr, row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let low_val = eval_expr(low, row, ctx, catalog, storage, clock)?;
    let high_val = eval_expr(high, row, ctx, catalog, storage, clock)?;

    let ge_low = compare_bool(
        val.clone(),
        low_val,
        |o| matches!(o, Ordering::Greater | Ordering::Equal),
        ctx.metadata.ansi_nulls,
    ) == Value::Bit(true);
    let le_high = compare_bool(
        val,
        high_val,
        |o| matches!(o, Ordering::Less | Ordering::Equal),
        ctx.metadata.ansi_nulls,
    ) == Value::Bit(true);

    let result = ge_low && le_high;
    Ok(Value::Bit(if negated { !result } else { result }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_like(
    like_expr: &Expr,
    pattern: &Expr,
    negated: bool,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let val = eval_expr(like_expr, row, ctx, catalog, storage, clock)?;
    let pat = eval_expr(pattern, row, ctx, catalog, storage, clock)?;

    if val.is_null() || pat.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let p = pat.to_string_value();
    let matched = like_match(&s, &p);
    Ok(Value::Bit(if negated { !matched } else { matched }))
}

fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.to_ascii_uppercase().chars().collect();
    let p_chars: Vec<char> = pattern.to_ascii_uppercase().chars().collect();
    like_match_impl(&s_chars, 0, &p_chars, 0)
}

fn like_match_impl(s: &[char], si: usize, p: &[char], pi: usize) -> bool {
    if pi >= p.len() {
        return si >= s.len();
    }
    match p[pi] {
        '%' => {
            if pi + 1 >= p.len() {
                return true;
            }
            for skip in 0..=(s.len() - si) {
                if like_match_impl(s, si + skip, p, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            if si >= s.len() {
                return false;
            }
            like_match_impl(s, si + 1, p, pi + 1)
        }
        _ => {
            if si >= s.len() || s[si] != p[pi] {
                return false;
            }
            like_match_impl(s, si + 1, p, pi + 1)
        }
    }
}

pub(crate) fn eval_scalar_subquery(
    stmt: &crate::ast::SelectStmt,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let mut sub_ctx = ctx.with_outer_row(row.to_vec());
    let query_result = execute_subquery_select(stmt, &mut sub_ctx, catalog, storage, clock)?;

    if query_result.rows.is_empty() {
        return Ok(Value::Null);
    }

    let first_row = &query_result.rows[0];
    if first_row.is_empty() {
        return Ok(Value::Null);
    }

    let val = first_row[0].clone();
    Ok(val)
}

pub(crate) fn eval_exists(
    stmt: &crate::ast::SelectStmt,
    negated: bool,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let mut sub_ctx = ctx.with_outer_row(row.to_vec());
    let query_result = execute_subquery_select(stmt, &mut sub_ctx, catalog, storage, clock)?;
    let exists = !query_result.rows.is_empty();
    Ok(Value::Bit(if negated { !exists } else { exists }))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_in_subquery(
    in_expr: &Expr,
    stmt: &crate::ast::SelectStmt,
    negated: bool,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let val = eval_expr(in_expr, row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }

    let mut sub_ctx = ctx.with_outer_row(row.to_vec());
    let query_result = execute_subquery_select(stmt, &mut sub_ctx, catalog, storage, clock)?;

    if query_result.rows.is_empty() {
        return Ok(Value::Bit(negated));
    }

    let mut found = false;
    let mut has_null = false;

    for row_data in &query_result.rows {
        if row_data.is_empty() {
            continue;
        }
        let subq_val = &row_data[0];

        if subq_val.is_null() {
            has_null = true;
            continue;
        }

        if compare_bool(
            val.clone(),
            subq_val.clone(),
            |o| o == Ordering::Equal,
            ctx.metadata.ansi_nulls,
        ) == Value::Bit(true)
        {
            found = true;
            break;
        }
    }

    if !found && has_null && !negated {
        return Ok(Value::Null);
    }

    Ok(Value::Bit(if negated { !found } else { found }))
}

fn execute_subquery_select(
    stmt: &crate::ast::SelectStmt,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<QueryResult, DbError> {
    let mut sub_ctx = ctx.subquery();
    let qe = QueryExecutor {
        catalog,
        storage,
        clock,
    };

    qe.execute_select(RelationalQuery::from(stmt.clone()), &mut sub_ctx)
}
