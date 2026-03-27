use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::types::Value;
use crate::storage::Storage;

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
