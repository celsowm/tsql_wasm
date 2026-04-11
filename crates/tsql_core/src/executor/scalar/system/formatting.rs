use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::super::clock::Clock;
use super::super::super::context::ExecutionContext;
use super::super::super::evaluator::eval_expr;
use super::super::super::model::ContextTable;

pub(crate) fn eval_parsename(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("PARSENAME expects 2 arguments".into()));
    }
    let obj_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let piece_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if obj_val.is_null() || piece_val.is_null() {
        return Ok(Value::Null);
    }

    let obj = obj_val.to_string_value();
    let piece = piece_val.to_integer_i64().unwrap_or(0);

    let parts: Vec<&str> = obj.split('.').rev().collect();
    let result = match piece {
        1 => parts.first().copied(), // Object name
        2 => {
            if parts.len() >= 2 {
                Some(parts[1])
            } else {
                None
            }
        } // Schema name
        3 => {
            if parts.len() >= 3 {
                Some(parts[2])
            } else {
                None
            }
        } // Database name
        4 => {
            if parts.len() >= 4 {
                Some(parts[3])
            } else {
                None
            }
        } // Server name
        _ => None,
    };

    match result {
        Some(s) => Ok(Value::NVarChar(s.to_string())),
        None => Ok(Value::Null),
    }
}

pub(crate) fn eval_quotename(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution(
            "QUOTENAME expects 1 or 2 arguments".into(),
        ));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let s = val.to_string_value();
    let quote_char = if args.len() == 2 {
        let v = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
        let qs = v.to_string_value();
        qs.chars().next().unwrap_or('[')
    } else {
        '['
    };

    let result = match quote_char {
        '\'' => format!("'{}'", s.replace('\'', "''")),
        '[' | ']' => format!("[{}]", s.replace(']', "]]")),
        '"' => format!("\"{}\"", s.replace('"', "\"\"")),
        _ => {
            return Err(DbError::Execution(format!(
                "Unsupported quote character '{}'",
                quote_char
            )))
        }
    };
    Ok(Value::NVarChar(result))
}
