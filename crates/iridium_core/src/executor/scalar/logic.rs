use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr;
use super::super::model::ContextTable;

pub(crate) fn eval_coalesce(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution(
            "COALESCE requires at least one argument".into(),
        ));
    }
    for arg in args {
        let val = eval_expr(arg, row, ctx, catalog, storage, clock)?;
        if !val.is_null() {
            return Ok(val);
        }
    }
    Ok(Value::Null)
}

pub(crate) fn eval_isnull(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("ISNULL expects 2 arguments".into()));
    }
    let left = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if !left.is_null() {
        Ok(left)
    } else {
        eval_expr(&args[1], row, ctx, catalog, storage, clock)
    }
}

pub(crate) fn eval_iif(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("IIF expects 3 arguments".into()));
    }
    let condition = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let is_true = match condition {
        Value::Bit(b) => b,
        Value::Null => false,
        Value::Int(v) => v != 0,
        Value::BigInt(v) => v != 0,
        Value::TinyInt(v) => v != 0,
        Value::SmallInt(v) => v != 0,
        _ => !condition.to_string_value().is_empty(),
    };
    if is_true {
        eval_expr(&args[1], row, ctx, catalog, storage, clock)
    } else {
        eval_expr(&args[2], row, ctx, catalog, storage, clock)
    }
}

pub(crate) fn eval_nullif(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("NULLIF expects 2 arguments".into()));
    }
    let left = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let right = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if left == right {
        Ok(Value::Null)
    } else {
        Ok(left)
    }
}

pub(crate) fn eval_choose(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 {
        return Err(DbError::Execution(
            "CHOOSE expects at least 2 arguments".into(),
        ));
    }
    let index_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let idx = index_val.to_integer_i64().unwrap_or(0);
    if idx < 1 || idx as usize > args.len() - 1 {
        return Ok(Value::Null);
    }
    eval_expr(&args[idx as usize], row, ctx, catalog, storage, clock)
}

pub(crate) fn eval_greatest(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution("GREATEST requires at least 1 argument".into()));
    }
    let mut result = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    for arg in &args[1..] {
        let val = eval_expr(arg, row, ctx, catalog, storage, clock)?;
        if val.is_null() || result.is_null() {
            return Ok(Value::Null);
        }
        if crate::executor::value_ops::compare_values(&val, &result) == std::cmp::Ordering::Greater {
            result = val;
        }
    }
    Ok(result)
}

pub(crate) fn eval_least(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution("LEAST requires at least 1 argument".into()));
    }
    let mut result = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    for arg in &args[1..] {
        let val = eval_expr(arg, row, ctx, catalog, storage, clock)?;
        if val.is_null() || result.is_null() {
            return Ok(Value::Null);
        }
        if crate::executor::value_ops::compare_values(&val, &result) == std::cmp::Ordering::Less {
            result = val;
        }
    }
    Ok(result)
}
