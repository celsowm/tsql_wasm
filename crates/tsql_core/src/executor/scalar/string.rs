use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::types::Value;
use crate::storage::Storage;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr;
use super::super::model::ContextTable;

pub(crate) fn eval_len(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("LEN expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    match val {
        Value::Null => Ok(Value::Null),
        Value::Char(s) | Value::VarChar(s) | Value::NChar(s) | Value::NVarChar(s) => {
            Ok(Value::Int(s.trim_end().len() as i32))
        }
        _ => {
            let s = val.to_string_value();
            Ok(Value::Int(s.trim_end().len() as i32))
        }
    }
}

pub(crate) fn eval_substring(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("SUBSTRING expects 3 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let start = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let length = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;

    let s = val.to_string_value();
    let start_i = match start {
        Value::Int(v) => v,
        Value::BigInt(v) => v as i32,
        Value::TinyInt(v) => v as i32,
        Value::SmallInt(v) => v as i32,
        _ => {
            return Err(DbError::Execution(
                "SUBSTRING start must be an integer".into(),
            ))
        }
    };
    let len_i = match length {
        Value::Int(v) => v,
        Value::BigInt(v) => v as i32,
        Value::TinyInt(v) => v as i32,
        Value::SmallInt(v) => v as i32,
        _ => {
            return Err(DbError::Execution(
                "SUBSTRING length must be an integer".into(),
            ))
        }
    };

    let chars: Vec<char> = s.chars().collect();
    let start_idx = if start_i <= 0 {
        0
    } else {
        (start_i as usize - 1).min(chars.len())
    };
    let end_idx = (start_idx + len_i.max(0) as usize).min(chars.len());
    let result: String = chars[start_idx..end_idx].iter().collect();

    match val {
        Value::NVarChar(_) | Value::NChar(_) => Ok(Value::NVarChar(result)),
        _ => Ok(Value::VarChar(result)),
    }
}

pub(crate) fn eval_upper(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("UPPER expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    match val {
        Value::Null => Ok(Value::Null),
        Value::VarChar(s) => Ok(Value::VarChar(s.to_uppercase())),
        Value::NVarChar(s) => Ok(Value::NVarChar(s.to_uppercase())),
        Value::Char(s) => Ok(Value::Char(s.to_uppercase())),
        Value::NChar(s) => Ok(Value::NChar(s.to_uppercase())),
        _ => Ok(Value::VarChar(val.to_string_value().to_uppercase())),
    }
}

pub(crate) fn eval_lower(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("LOWER expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    match val {
        Value::Null => Ok(Value::Null),
        Value::VarChar(s) => Ok(Value::VarChar(s.to_lowercase())),
        Value::NVarChar(s) => Ok(Value::NVarChar(s.to_lowercase())),
        Value::Char(s) => Ok(Value::Char(s.to_lowercase())),
        Value::NChar(s) => Ok(Value::NChar(s.to_lowercase())),
        _ => Ok(Value::VarChar(val.to_string_value().to_lowercase())),
    }
}

pub(crate) fn eval_trim(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    left: bool,
    right: bool,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "TRIM/LTRIM/RTRIM expects 1 argument".into(),
        ));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    match val {
        Value::Null => Ok(Value::Null),
        Value::VarChar(s) => Ok(Value::VarChar(trim_str(&s, left, right))),
        Value::NVarChar(s) => Ok(Value::NVarChar(trim_str(&s, left, right))),
        Value::Char(s) => Ok(Value::VarChar(trim_str(&s, left, right))),
        Value::NChar(s) => Ok(Value::NVarChar(trim_str(&s, left, right))),
        _ => Ok(Value::VarChar(trim_str(
            &val.to_string_value(),
            left,
            right,
        ))),
    }
}

fn trim_str(s: &str, trim_left: bool, trim_right: bool) -> String {
    let mut result = s;
    if trim_left {
        result = result.trim_start();
    }
    if trim_right {
        result = result.trim_end();
    }
    result.to_string()
}

pub(crate) fn eval_replace(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("REPLACE expects 3 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let from = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let to = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;

    if val.is_null() || from.is_null() || to.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let f = from.to_string_value();
    let t = to.to_string_value();
    Ok(Value::VarChar(s.replace(&f, &t)))
}

pub(crate) fn eval_left(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("LEFT expects 2 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let count = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if val.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let n = count.to_integer_i64().unwrap_or(0) as usize;
    let result: String = s.chars().take(n).collect();
    Ok(Value::VarChar(result))
}

pub(crate) fn eval_right(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("RIGHT expects 2 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let count = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if val.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let n = count.to_integer_i64().unwrap_or(0) as usize;
    let chars: Vec<char> = s.chars().collect();
    let start = chars.len().saturating_sub(n);
    let result: String = chars[start..].iter().collect();
    Ok(Value::VarChar(result))
}

pub(crate) fn eval_charindex(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DbError::Execution(
            "CHARINDEX expects 2 or 3 arguments".into(),
        ));
    }
    let search = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let target = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if search.is_null() || target.is_null() {
        return Ok(Value::Null);
    }

    let search_str = search.to_string_value();
    let target_str = target.to_string_value();

    let start_pos = if args.len() == 3 {
        let sp = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
        sp.to_integer_i64().unwrap_or(1) as usize
    } else {
        1
    };

    let start_idx = if start_pos > 0 { start_pos - 1 } else { 0 };
    let result = if start_idx < target_str.len() {
        target_str[start_idx..]
            .find(&search_str)
            .map(|pos| (start_idx + pos + 1) as i64)
            .unwrap_or(0)
    } else {
        0
    };

    Ok(Value::Int(result as i32))
}

pub(crate) fn eval_unistr(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("UNISTR expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let s = val.to_string_value();
    
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(&next) = chars.peek() {
                if next == 'u' || next == 'U' {
                    chars.next();
                    let hex_len = if next == 'u' { 4 } else { 8 };
                    let mut hex = String::with_capacity(hex_len);
                    for _ in 0..hex_len {
                        if let Some(h) = chars.next() {
                            hex.push(h);
                        }
                    }
                    if let Ok(code) = u32::from_str_radix(&hex, 16) {
                        if let Some(unicode_char) = std::char::from_u32(code) {
                            result.push(unicode_char);
                            continue;
                        }
                    }
                    result.push('\\');
                    result.push(next);
                    result.push_str(&hex);
                    continue;
                }
            }
        }
        result.push(ch);
    }
    
    Ok(Value::NVarChar(result))
}
