use crate::ast::Expr;
use crate::catalog::{Catalog, RoutineKind};
use crate::error::DbError;
use crate::types::Value;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::date_time::{apply_dateadd, parse_datetime_parts};
use super::evaluator::eval_expr;
use super::model::ContextTable;
use super::value_helpers::value_to_f64;
use crate::storage::Storage;

pub(crate) fn eval_function(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if name.eq_ignore_ascii_case("GETDATE") {
        if !args.is_empty() {
            return Err(DbError::Execution("GETDATE expects no arguments".into()));
        }
        Ok(Value::DateTime(clock.now_datetime_literal()))
    } else if name.eq_ignore_ascii_case("ISNULL") {
        if args.len() != 2 {
            return Err(DbError::Execution("ISNULL expects 2 arguments".into()));
        }
        let left = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        if !left.is_null() {
            Ok(left)
        } else {
            eval_expr(&args[1], row, ctx, catalog, storage, clock)
        }
    } else if name.eq_ignore_ascii_case("COALESCE") {
        eval_coalesce(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("LEN") {
        eval_len(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("SUBSTRING") {
        eval_substring(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("DATEADD") {
        eval_dateadd(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("DATEDIFF") {
        eval_datediff(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("COUNT")
        || name.eq_ignore_ascii_case("SUM")
        || name.eq_ignore_ascii_case("AVG")
    {
        Err(DbError::Execution(format!(
            "{} is only supported in grouped projection",
            name
        )))
    } else if name.eq_ignore_ascii_case("MIN") || name.eq_ignore_ascii_case("MAX") {
        Err(DbError::Execution(
            "MIN/MAX require a FROM clause when used in scalar context (use in GROUP BY)".into(),
        ))
    } else if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
        if !args.is_empty() {
            return Err(DbError::Execution(
                "CURRENT_TIMESTAMP expects no arguments".into(),
            ));
        }
        Ok(Value::DateTime(clock.now_datetime_literal()))
    } else if name.eq_ignore_ascii_case("UPPER") {
        eval_upper(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("LOWER") {
        eval_lower(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("LTRIM") {
        eval_trim(args, row, ctx, catalog, storage, clock, true, false)
    } else if name.eq_ignore_ascii_case("RTRIM") {
        eval_trim(args, row, ctx, catalog, storage, clock, false, true)
    } else if name.eq_ignore_ascii_case("TRIM") {
        eval_trim(args, row, ctx, catalog, storage, clock, true, true)
    } else if name.eq_ignore_ascii_case("REPLACE") {
        eval_replace(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("ROUND") {
        eval_round(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("CEILING") {
        eval_math_unary(
            args,
            row,
            ctx,
            catalog,
            storage,
            clock,
            "CEILING",
            |f: f64| f.ceil(),
        )
    } else if name.eq_ignore_ascii_case("FLOOR") {
        eval_math_unary(
            args,
            row,
            ctx,
            catalog,
            storage,
            clock,
            "FLOOR",
            |f: f64| f.floor(),
        )
    } else if name.eq_ignore_ascii_case("ABS") {
        eval_abs(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("CHARINDEX") {
        eval_charindex(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("NEWID") {
        if !args.is_empty() {
            return Err(DbError::Execution("NEWID expects no arguments".into()));
        }
        Ok(Value::UniqueIdentifier(clock.now_datetime_literal()))
    } else if name.eq_ignore_ascii_case("COUNT_BIG") {
        Err(DbError::Execution(
            "COUNT_BIG is only supported in grouped projection".into(),
        ))
    } else if name.eq_ignore_ascii_case("OBJECT_ID") {
        eval_object_id(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("COLUMNPROPERTY") {
        eval_columnproperty(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("SCOPE_IDENTITY") {
        if !args.is_empty() {
            return Err(DbError::Execution(
                "SCOPE_IDENTITY expects no arguments".into(),
            ));
        }
        Ok(match ctx.current_scope_identity() {
            Some(v) => Value::BigInt(v),
            None => Value::Null,
        })
    } else if name.eq_ignore_ascii_case("@@IDENTITY") {
        if !args.is_empty() {
            return Err(DbError::Execution("@@IDENTITY expects no arguments".into()));
        }
        Ok(match *ctx.session_last_identity {
            Some(v) => Value::BigInt(v),
            None => Value::Null,
        })
    } else if name.eq_ignore_ascii_case("IDENT_CURRENT") {
        eval_ident_current(args, row, ctx, catalog, storage, clock)
    } else {
        eval_user_scalar_function(name, args, row, ctx, catalog, storage, clock)
    }
}

fn eval_user_scalar_function(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let (schema, fname) = if let Some(dot) = name.find('.') {
        (&name[..dot], &name[dot + 1..])
    } else {
        ("dbo", name)
    };
    let Some(routine) = catalog.find_routine(schema, fname) else {
        return Err(DbError::Execution(format!(
            "function '{}' not supported",
            name
        )));
    };
    let RoutineKind::Function { body, .. } = &routine.kind else {
        return Err(DbError::Execution(format!("'{}' is not a function", name)));
    };
    let crate::ast::FunctionBody::ScalarReturn(expr) = body else {
        return Err(DbError::Execution(format!(
            "inline TVF '{}' cannot be used in scalar context",
            name
        )));
    };
    if args.len() != routine.params.len() {
        return Err(DbError::Execution(format!(
            "function '{}' expected {} args, got {}",
            name,
            routine.params.len(),
            args.len()
        )));
    }

    ctx.enter_scope();
    for (param, arg_expr) in routine.params.iter().zip(args.iter()) {
        let val = eval_expr(arg_expr, row, ctx, catalog, storage, clock)?;
        let ty = super::type_mapping::data_type_spec_to_runtime(&param.data_type);
        let coerced = super::value_ops::coerce_value_to_type(val, &ty)?;
        ctx.variables.insert(param.name.clone(), (ty, coerced));
        ctx.register_declared_var(&param.name);
    }
    let out = eval_expr(expr, row, ctx, catalog, storage, clock);
    ctx.leave_scope();
    out
}

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

pub(crate) fn extract_datepart(expr: &Expr) -> Result<String, DbError> {
    match expr {
        Expr::Identifier(name) => Ok(name.to_lowercase()),
        Expr::String(s) | Expr::UnicodeString(s) => Ok(s.to_lowercase()),
        _ => Err(DbError::Execution(
            "datepart must be an identifier or string".into(),
        )),
    }
}

pub(crate) fn eval_dateadd(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("DATEADD expects 3 arguments".into()));
    }

    let part = extract_datepart(&args[0])?;
    let number = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let date_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
    let num = match number {
        Value::Int(v) => v as i64,
        Value::BigInt(v) => v,
        Value::SmallInt(v) => v as i64,
        Value::TinyInt(v) => v as i64,
        _ => {
            return Err(DbError::Execution(
                "DATEADD number must be an integer".into(),
            ))
        }
    };
    let date_str = date_val.to_string_value();

    let result = apply_dateadd(&part, num, &date_str)?;

    match date_val {
        Value::Date(_) => Ok(Value::Date(result)),
        Value::DateTime(_) => Ok(Value::DateTime(result)),
        Value::DateTime2(_) => Ok(Value::DateTime2(result)),
        _ => Ok(Value::DateTime(result)),
    }
}

pub(crate) fn eval_datediff(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("DATEDIFF expects 3 arguments".into()));
    }

    let part = extract_datepart(&args[0])?;
    let start_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let end_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;

    let start_str = start_val.to_string_value();
    let end_str = end_val.to_string_value();

    let (sy, sm, sd, sh, smi, ss) = parse_datetime_parts(&start_str)?;
    let (ey, em, ed, ehi, emi, es) = parse_datetime_parts(&end_str)?;

    use crate::executor::date_time::date_to_days;

    let result = match part.as_str() {
        "year" | "yy" | "yyyy" => ey - sy,
        "month" | "mm" | "m" => (ey - sy) * 12 + (em - sm),
        "day" | "dd" | "d" => (date_to_days(ey, em, ed) - date_to_days(sy, sm, sd)) as i32,
        "hour" | "hh" => {
            let day_diff = date_to_days(ey, em, ed) - date_to_days(sy, sm, sd);
            (day_diff * 24 + (ehi - sh) as i64) as i32
        }
        "minute" | "mi" | "n" => {
            let day_diff = date_to_days(ey, em, ed) - date_to_days(sy, sm, sd);
            (day_diff * 1440 + (ehi - sh) as i64 * 60 + (emi - smi) as i64) as i32
        }
        "second" | "ss" | "s" => {
            let day_diff = date_to_days(ey, em, ed) - date_to_days(sy, sm, sd);
            (day_diff * 86400
                + (ehi - sh) as i64 * 3600
                + (emi - smi) as i64 * 60
                + (es - ss) as i64) as i32
        }
        _ => return Err(DbError::Execution(format!("unknown datepart '{}'", part))),
    };

    Ok(Value::Int(result))
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

pub(crate) fn eval_round(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution("ROUND expects 1 or 2 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let precision = if args.len() == 2 {
        eval_expr(&args[1], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(0) as i32
    } else {
        0
    };

    if val.is_null() {
        return Ok(Value::Null);
    }

    let f = match &val {
        Value::Decimal(raw, scale) => {
            let divisor = 10f64.powi(*scale as i32);
            *raw as f64 / divisor
        }
        Value::TinyInt(v) => *v as f64,
        Value::SmallInt(v) => *v as f64,
        Value::Int(v) => *v as f64,
        Value::BigInt(v) => *v as f64,
        _ => {
            return Err(DbError::Execution(
                "ROUND requires a numeric argument".into(),
            ))
        }
    };

    let multiplier = 10f64.powi(precision);
    let rounded = (f * multiplier).round() / multiplier;
    Ok(Value::VarChar(rounded.to_string()))
}

pub(crate) fn eval_math_unary<F>(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    name: &str,
    func: F,
) -> Result<Value, DbError>
where
    F: Fn(f64) -> f64,
{
    if args.len() != 1 {
        return Err(DbError::Execution(format!("{} expects 1 argument", name)));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let f = value_to_f64(&val)?;
    let result = func(f);
    Ok(Value::VarChar(result.to_string()))
}

pub(crate) fn eval_abs(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("ABS expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    match &val {
        Value::TinyInt(v) => Ok(Value::TinyInt(*v)),
        Value::SmallInt(v) => Ok(Value::SmallInt(v.abs())),
        Value::Int(v) => Ok(Value::Int(v.abs())),
        Value::BigInt(v) => Ok(Value::BigInt(v.abs())),
        Value::Decimal(raw, scale) => Ok(Value::Decimal(raw.abs(), *scale)),
        _ => {
            let f = value_to_f64(&val)?;
            Ok(Value::VarChar(f.abs().to_string()))
        }
    }
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

pub(crate) fn eval_object_id(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("OBJECT_ID expects 1 argument".into()));
    }

    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let raw = val.to_string_value();
    let cleaned = raw.trim().trim_matches('[').trim_matches(']');
    let parts: Vec<&str> = cleaned.split('.').collect();
    let (schema, name) = if parts.len() == 2 {
        (parts[0].trim(), parts[1].trim())
    } else {
        ("dbo", cleaned.trim())
    };

    Ok(match catalog.object_id(schema, name) {
        Some(id) => Value::Int(id),
        None => Value::Null,
    })
}

pub(crate) fn eval_columnproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution(
            "COLUMNPROPERTY expects 3 arguments".into(),
        ));
    }

    let object_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let column_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let property_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;

    if object_val.is_null() || column_val.is_null() || property_val.is_null() {
        return Ok(Value::Null);
    }

    let object_id = match object_val {
        Value::Int(v) => Some(v),
        Value::BigInt(v) => Some(v as i32),
        Value::SmallInt(v) => Some(v as i32),
        Value::TinyInt(v) => Some(v as i32),
        Value::VarChar(_) | Value::NVarChar(_) | Value::Char(_) | Value::NChar(_) => {
            let raw = object_val.to_string_value();
            let cleaned = raw.trim().trim_matches('[').trim_matches(']');
            let parts: Vec<&str> = cleaned.split('.').collect();
            let (schema, name) = if parts.len() == 2 {
                (parts[0].trim(), parts[1].trim())
            } else {
                ("dbo", cleaned.trim())
            };
            catalog.object_id(schema, name)
        }
        _ => None,
    };

    let Some(object_id) = object_id else {
        return Ok(Value::Null);
    };
    let column_name = column_val.to_string_value();
    let property_name = property_val.to_string_value().to_uppercase();

    let Some(table) = catalog
        .get_tables()
        .iter()
        .find(|t| t.id as i32 == object_id)
    else {
        return Ok(Value::Null);
    };

    let Some((ordinal, col)) = table
        .columns
        .iter()
        .enumerate()
        .find(|(_, c)| c.name.eq_ignore_ascii_case(&column_name))
    else {
        return Ok(Value::Null);
    };

    match property_name.as_str() {
        "ALLOWSNULL" => Ok(Value::Int(if col.nullable { 1 } else { 0 })),
        "ISCOMPUTED" => Ok(Value::Int(if col.computed_expr.is_some() { 1 } else { 0 })),
        "COLUMNID" => Ok(Value::Int((ordinal + 1) as i32)),
        _ => Ok(Value::Null),
    }
}

pub(crate) fn eval_ident_current(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "IDENT_CURRENT expects 1 argument".into(),
        ));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let raw = val.to_string_value();
    let parts: Vec<&str> = raw.split('.').collect();
    let (schema, name) = if parts.len() == 2 {
        (parts[0].trim(), parts[1].trim())
    } else {
        ("dbo", raw.trim())
    };
    let Some(table) = catalog.find_table(schema, name) else {
        return Ok(Value::Null);
    };
    for col in &table.columns {
        if let Some(identity) = &col.identity {
            return Ok(Value::BigInt(identity.current - identity.increment));
        }
    }
    Ok(Value::Null)
}
