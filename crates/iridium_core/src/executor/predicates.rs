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
    escape: Option<&Expr>,
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
    let esc = match escape {
        Some(e) => {
            let ev = eval_expr(e, row, ctx, catalog, storage, clock)?;
            if ev.is_null() {
                return Ok(Value::Null);
            }
            Some(ev.to_string_value())
        }
        None => None,
    };
    let matched = like_match(&s, &p, esc.as_deref());
    Ok(Value::Bit(if negated { !matched } else { matched }))
}

fn like_match(s: &str, pattern: &str, escape: Option<&str>) -> bool {
    let s: Vec<char> = s.to_ascii_uppercase().chars().collect();
    let p_raw: Vec<char> = pattern.to_ascii_uppercase().chars().collect();
    let esc_char = escape.and_then(|e| e.chars().next().map(|c| c.to_ascii_uppercase()));

    let mut p = Vec::new();
    let mut escaped = Vec::new();
    let mut i = 0;
    while i < p_raw.len() {
        if let Some(ec) = esc_char {
            if p_raw[i] == ec && i + 1 < p_raw.len() {
                p.push(p_raw[i + 1]);
                escaped.push(true);
                i += 2;
                continue;
            }
        }
        p.push(p_raw[i]);
        escaped.push(false);
        i += 1;
    }

    let sn = s.len();
    let pn = p.len();

    let mut dp = vec![false; pn + 1];
    dp[0] = true;
    for j in 0..pn {
        if p[j] == '%' && !escaped[j] {
            dp[j + 1] = dp[j];
        } else {
            break;
        }
    }

    for i_s in 0..sn {
        let mut prev = dp[0];
        dp[0] = false;
        for j in 0..pn {
            let tmp = dp[j + 1];
            dp[j + 1] = if p[j] == '%' && !escaped[j] {
                tmp || dp[j]
            } else if (p[j] == '_' && !escaped[j]) || (p[j] == s[i_s]) {
                prev
            } else {
                false
            };
            prev = tmp;
        }
    }

    dp[pn]
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
    if is_uncorrelated(stmt) {
        let key = format!("{:?}", stmt);
        {
            let cache = ctx.subquery_cache.lock();
            if let Some(cached) = cache.get(&key) {
                return Ok(cached.clone());
            }
        }

        let mut sub_ctx = ctx.subquery();
        let qe = QueryExecutor {
            catalog,
            storage,
            clock,
        };
        let result = qe.execute_select(RelationalQuery::from(stmt.clone()), &mut sub_ctx)?;

        {
            let mut cache = ctx.subquery_cache.lock();
            cache.insert(key, result.clone());
        }
        return Ok(result);
    }

    let mut sub_ctx = ctx.subquery();
    let qe = QueryExecutor {
        catalog,
        storage,
        clock,
    };

    qe.execute_select(RelationalQuery::from(stmt.clone()), &mut sub_ctx)
}

fn is_uncorrelated(stmt: &crate::ast::SelectStmt) -> bool {
    // If it has NO FromNode, it's uncorrelated if selection/projection are uncorrelated.
    if stmt.from_clause.is_none() {
        return stmt
            .projection
            .iter()
            .all(|i| is_expr_uncorrelated(&i.expr))
            && stmt.selection.as_ref().map_or(true, is_expr_uncorrelated);
    }
    // For now, only literals are considered uncorrelated.
    false
}

fn is_expr_uncorrelated(expr: &Expr) -> bool {
    match expr {
        Expr::Integer(_)
        | Expr::FloatLiteral(_)
        | Expr::BinaryLiteral(_)
        | Expr::String(_)
        | Expr::UnicodeString(_)
        | Expr::Null => true,
        Expr::Binary { left, right, .. } => {
            is_expr_uncorrelated(left) && is_expr_uncorrelated(right)
        }
        Expr::Unary { expr, .. } => is_expr_uncorrelated(expr),
        Expr::Cast { expr, .. }
        | Expr::TryCast { expr, .. }
        | Expr::Convert { expr, .. }
        | Expr::TryConvert { expr, .. } => is_expr_uncorrelated(expr),
        Expr::FunctionCall { args, .. } => args.iter().all(is_expr_uncorrelated),
        _ => false,
    }
}
