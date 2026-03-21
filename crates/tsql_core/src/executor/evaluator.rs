use std::cmp::Ordering;

use crate::ast::{BinaryOp, Expr, UnaryOp};
use crate::error::DbError;
use crate::types::{DataType, Value};

use super::clock::Clock;
use super::model::JoinedRow;
use super::type_mapping::data_type_spec_to_runtime;
use super::value_ops::{coerce_value_to_type, compare_values, truthy};

pub(crate) fn eval_expr_to_type_constant(
    expr: &Expr,
    ty: &DataType,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let value = eval_constant_expr(expr, clock)?;
    coerce_value_to_type(value, ty)
}

pub(crate) fn eval_expr_to_type_in_context(
    expr: &Expr,
    ty: &DataType,
    row: &JoinedRow,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let value = eval_expr(expr, row, clock)?;
    coerce_value_to_type(value, ty)
}

pub(crate) fn eval_constant_expr(expr: &Expr, clock: &dyn Clock) -> Result<Value, DbError> {
    let ctx: JoinedRow = vec![];
    eval_expr(expr, &ctx, clock)
}

pub(crate) fn eval_expr(expr: &Expr, row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    match expr {
        Expr::Identifier(name) => resolve_identifier(row, name),
        Expr::QualifiedIdentifier(parts) => resolve_qualified_identifier(row, parts),
        Expr::Wildcard => Err(DbError::Execution(
            "wildcard is not a scalar expression".into(),
        )),
        Expr::Integer(v) => Ok(if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
            Value::Int(*v as i32)
        } else {
            Value::BigInt(*v)
        }),
        Expr::FloatLiteral(s) => {
            let f: f64 = s
                .parse()
                .map_err(|_| DbError::Execution(format!("invalid float literal '{}'", s)))?;
            let raw = (f * 1e6_f64) as i128;
            Ok(Value::Decimal(raw, 6))
        }
        Expr::String(v) => Ok(Value::VarChar(v.clone())),
        Expr::UnicodeString(v) => Ok(Value::NVarChar(v.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::FunctionCall { name, args } => eval_function(name, args, row, clock),
        Expr::Binary { left, op, right } => {
            let lv = eval_expr(left, row, clock)?;
            let rv = eval_expr(right, row, clock)?;
            eval_binary(op, lv, rv)
        }
        Expr::Unary { op, expr: inner } => {
            let val = eval_expr(inner, row, clock)?;
            eval_unary(op, val)
        }
        Expr::IsNull(inner) => Ok(Value::Bit(eval_expr(inner, row, clock)?.is_null())),
        Expr::IsNotNull(inner) => Ok(Value::Bit(!eval_expr(inner, row, clock)?.is_null())),
        Expr::Cast { expr, target } => {
            let value = eval_expr(expr, row, clock)?;
            coerce_value_to_type(value, &data_type_spec_to_runtime(target))
        }
        Expr::Convert { target, expr } => {
            let value = eval_expr(expr, row, clock)?;
            coerce_value_to_type(value, &data_type_spec_to_runtime(target))
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => eval_case(operand, when_clauses, else_result, row, clock),
        Expr::InList {
            expr: in_expr,
            list,
            negated,
        } => eval_in_list(in_expr, list, *negated, row, clock),
        Expr::Between {
            expr: between_expr,
            low,
            high,
            negated,
        } => eval_between(between_expr, low, high, *negated, row, clock),
        Expr::Like {
            expr: like_expr,
            pattern,
            negated,
        } => eval_like(like_expr, pattern, *negated, row, clock),
    }
}

pub(crate) fn eval_predicate(
    expr: &Expr,
    row: &JoinedRow,
    clock: &dyn Clock,
) -> Result<bool, DbError> {
    let value = eval_expr(expr, row, clock)?;
    Ok(match value {
        Value::Bit(v) => v,
        Value::Null => false,
        other => truthy(&other),
    })
}

pub(crate) fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. }
            if name.eq_ignore_ascii_case("COUNT")
                || name.eq_ignore_ascii_case("SUM")
                || name.eq_ignore_ascii_case("AVG")
                || name.eq_ignore_ascii_case("MIN")
                || name.eq_ignore_ascii_case("MAX") =>
        {
            true
        }
        Expr::Binary { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::Unary { expr: inner, .. } => contains_aggregate(inner),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => contains_aggregate(inner),
        Expr::Cast { expr, .. } | Expr::Convert { expr, .. } => contains_aggregate(expr),
        Expr::Case {
            when_clauses,
            else_result,
            ..
        } => {
            when_clauses
                .iter()
                .any(|w| contains_aggregate(&w.condition) || contains_aggregate(&w.result))
                || else_result.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        Expr::InList { expr: e, list, .. } => {
            contains_aggregate(e) || list.iter().any(contains_aggregate)
        }
        Expr::Between {
            expr: e, low, high, ..
        } => contains_aggregate(e) || contains_aggregate(low) || contains_aggregate(high),
        Expr::Like {
            expr: e, pattern, ..
        } => contains_aggregate(e) || contains_aggregate(pattern),
        _ => false,
    }
}

fn resolve_identifier(row: &JoinedRow, name: &str) -> Result<Value, DbError> {
    let mut found: Option<Value> = None;
    for binding in row {
        if let Some(idx) = binding
            .table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
        {
            let value = binding
                .row
                .as_ref()
                .map(|r| r.values[idx].clone())
                .unwrap_or(Value::Null);
            if found.is_some() {
                return Err(DbError::Semantic(format!("ambiguous column '{}'", name)));
            }
            found = Some(value);
        }
    }
    found.ok_or_else(|| DbError::Semantic(format!("column '{}' not found", name)))
}

fn resolve_qualified_identifier(row: &JoinedRow, parts: &[String]) -> Result<Value, DbError> {
    if parts.len() != 2 {
        return Err(DbError::Semantic(
            "only two-part identifiers are supported in this build".into(),
        ));
    }

    let table_name = &parts[0];
    let column_name = &parts[1];
    for binding in row {
        if binding.alias.eq_ignore_ascii_case(table_name)
            || binding.table.name.eq_ignore_ascii_case(table_name)
        {
            let idx = binding
                .table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| {
                    DbError::Semantic(format!("column '{}.{}' not found", table_name, column_name))
                })?;
            return Ok(binding
                .row
                .as_ref()
                .map(|r| r.values[idx].clone())
                .unwrap_or(Value::Null));
        }
    }

    Err(DbError::Semantic(format!(
        "table or alias '{}' not found",
        table_name
    )))
}

fn eval_function(
    name: &str,
    args: &[Expr],
    row: &JoinedRow,
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
        let left = eval_expr(&args[0], row, clock)?;
        if !left.is_null() {
            Ok(left)
        } else {
            eval_expr(&args[1], row, clock)
        }
    } else if name.eq_ignore_ascii_case("COALESCE") {
        eval_coalesce(args, row, clock)
    } else if name.eq_ignore_ascii_case("LEN") {
        eval_len(args, row, clock)
    } else if name.eq_ignore_ascii_case("SUBSTRING") {
        eval_substring(args, row, clock)
    } else if name.eq_ignore_ascii_case("DATEADD") {
        eval_dateadd(args, row, clock)
    } else if name.eq_ignore_ascii_case("DATEDIFF") {
        eval_datediff(args, row, clock)
    } else if name.eq_ignore_ascii_case("COUNT") {
        Err(DbError::Execution(
            "COUNT is only supported in grouped projection".into(),
        ))
    } else if name.eq_ignore_ascii_case("SUM")
        || name.eq_ignore_ascii_case("AVG")
        || name.eq_ignore_ascii_case("MIN")
        || name.eq_ignore_ascii_case("MAX")
    {
        Err(DbError::Execution(format!(
            "{} is only supported in grouped projection",
            name
        )))
    } else if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
        if !args.is_empty() {
            return Err(DbError::Execution(
                "CURRENT_TIMESTAMP expects no arguments".into(),
            ));
        }
        Ok(Value::DateTime(clock.now_datetime_literal()))
    } else if name.eq_ignore_ascii_case("UPPER") {
        eval_upper(args, row, clock)
    } else if name.eq_ignore_ascii_case("LOWER") {
        eval_lower(args, row, clock)
    } else if name.eq_ignore_ascii_case("LTRIM") {
        eval_trim(args, row, clock, true, false)
    } else if name.eq_ignore_ascii_case("RTRIM") {
        eval_trim(args, row, clock, false, true)
    } else if name.eq_ignore_ascii_case("TRIM") {
        eval_trim(args, row, clock, true, true)
    } else if name.eq_ignore_ascii_case("REPLACE") {
        eval_replace(args, row, clock)
    } else if name.eq_ignore_ascii_case("ROUND") {
        eval_round(args, row, clock)
    } else if name.eq_ignore_ascii_case("CEILING") {
        eval_math_unary(args, row, clock, "CEILING", |f: f64| f.ceil())
    } else if name.eq_ignore_ascii_case("FLOOR") {
        eval_math_unary(args, row, clock, "FLOOR", |f: f64| f.floor())
    } else if name.eq_ignore_ascii_case("ABS") {
        eval_abs(args, row, clock)
    } else if name.eq_ignore_ascii_case("CHARINDEX") {
        eval_charindex(args, row, clock)
    } else if name.eq_ignore_ascii_case("NEWID") {
        if !args.is_empty() {
            return Err(DbError::Execution("NEWID expects no arguments".into()));
        }
        Ok(Value::UniqueIdentifier(clock.now_datetime_literal()))
    } else if name.eq_ignore_ascii_case("COUNT_BIG") {
        Err(DbError::Execution(
            "COUNT_BIG is only supported in grouped projection".into(),
        ))
    } else {
        Err(DbError::Execution(format!(
            "function '{}' not supported",
            name
        )))
    }
}

fn eval_coalesce(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution(
            "COALESCE requires at least one argument".into(),
        ));
    }
    for arg in args {
        let val = eval_expr(arg, row, clock)?;
        if !val.is_null() {
            return Ok(val);
        }
    }
    Ok(Value::Null)
}

fn eval_len(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("LEN expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, clock)?;
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

fn eval_substring(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("SUBSTRING expects 3 arguments".into()));
    }
    let val = eval_expr(&args[0], row, clock)?;
    let start = eval_expr(&args[1], row, clock)?;
    let length = eval_expr(&args[2], row, clock)?;

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

fn extract_datepart(expr: &Expr) -> Result<String, DbError> {
    match expr {
        Expr::Identifier(name) => Ok(name.to_lowercase()),
        Expr::String(s) | Expr::UnicodeString(s) => Ok(s.to_lowercase()),
        _ => Err(DbError::Execution(
            "datepart must be an identifier or string".into(),
        )),
    }
}

fn eval_dateadd(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("DATEADD expects 3 arguments".into()));
    }

    let part = extract_datepart(&args[0])?;
    let number = eval_expr(&args[1], row, clock)?;
    let date_val = eval_expr(&args[2], row, clock)?;
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

fn apply_dateadd(part: &str, num: i64, date_str: &str) -> Result<String, DbError> {
    let (y, m, d, h, mi, s) = parse_datetime_parts(date_str)?;

    let (ny, nm, nd, nh, nmi, ns) = match part {
        "year" | "yy" | "yyyy" => (y + num as i32, m, d, h, mi, s),
        "month" | "mm" | "m" => {
            let total = (y as i64) * 12 + (m as i64 - 1) + num;
            let ny = (total / 12) as i32;
            let nm = (total % 12 + 1) as i32;
            (ny, nm, d, h, mi, s)
        }
        "day" | "dd" | "d" => {
            let total_days = date_to_days(y, m, d) + num;
            let (ny, nm, nd) = days_to_date(total_days);
            (ny, nm, nd, h, mi, s)
        }
        "hour" | "hh" => {
            let total_hours = (date_to_days(y, m, d) * 24) + h as i64 + num;
            let total_days = total_hours.div_euclid(24);
            let nh = total_hours.rem_euclid(24) as i32;
            let (ny, nm, nd) = days_to_date(total_days);
            (ny, nm, nd, nh, mi, s)
        }
        "minute" | "mi" | "n" => {
            let total_minutes = (date_to_days(y, m, d) * 24 * 60) + h as i64 * 60 + mi as i64 + num;
            let total_days = total_minutes.div_euclid(24 * 60);
            let remainder = total_minutes.rem_euclid(24 * 60);
            let nh = (remainder / 60) as i32;
            let nmi = (remainder % 60) as i32;
            let (ny, nm, nd) = days_to_date(total_days);
            (ny, nm, nd, nh, nmi, s)
        }
        "second" | "ss" | "s" => {
            let total_secs =
                (date_to_days(y, m, d) * 86400) + h as i64 * 3600 + mi as i64 * 60 + s as i64 + num;
            let total_days = total_secs.div_euclid(86400);
            let remainder = total_secs.rem_euclid(86400);
            let nh = (remainder / 3600) as i32;
            let nmi = ((remainder % 3600) / 60) as i32;
            let ns = (remainder % 60) as i32;
            let (ny, nm, nd) = days_to_date(total_days);
            (ny, nm, nd, nh, nmi, ns)
        }
        _ => return Err(DbError::Execution(format!("unknown datepart '{}'", part))),
    };

    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        ny, nm, nd, nh, nmi, ns
    ))
}

fn eval_datediff(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("DATEDIFF expects 3 arguments".into()));
    }

    let part = extract_datepart(&args[0])?;
    let start_val = eval_expr(&args[1], row, clock)?;
    let end_val = eval_expr(&args[2], row, clock)?;

    let start_str = start_val.to_string_value();
    let end_str = end_val.to_string_value();

    let (sy, sm, sd, sh, smi, ss) = parse_datetime_parts(&start_str)?;
    let (ey, em, ed, ehi, emi, es) = parse_datetime_parts(&end_str)?;

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

// ─── New built-in functions ────────────────────────────────────────────

fn eval_upper(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("UPPER expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, clock)?;
    match val {
        Value::Null => Ok(Value::Null),
        Value::VarChar(s) => Ok(Value::VarChar(s.to_uppercase())),
        Value::NVarChar(s) => Ok(Value::NVarChar(s.to_uppercase())),
        Value::Char(s) => Ok(Value::Char(s.to_uppercase())),
        Value::NChar(s) => Ok(Value::NChar(s.to_uppercase())),
        _ => Ok(Value::VarChar(val.to_string_value().to_uppercase())),
    }
}

fn eval_lower(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("LOWER expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, clock)?;
    match val {
        Value::Null => Ok(Value::Null),
        Value::VarChar(s) => Ok(Value::VarChar(s.to_lowercase())),
        Value::NVarChar(s) => Ok(Value::NVarChar(s.to_lowercase())),
        Value::Char(s) => Ok(Value::Char(s.to_lowercase())),
        Value::NChar(s) => Ok(Value::NChar(s.to_lowercase())),
        _ => Ok(Value::VarChar(val.to_string_value().to_lowercase())),
    }
}

fn eval_trim(
    args: &[Expr],
    row: &JoinedRow,
    clock: &dyn Clock,
    left: bool,
    right: bool,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "TRIM/LTRIM/RTRIM expects 1 argument".into(),
        ));
    }
    let val = eval_expr(&args[0], row, clock)?;
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

fn eval_replace(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("REPLACE expects 3 arguments".into()));
    }
    let val = eval_expr(&args[0], row, clock)?;
    let from = eval_expr(&args[1], row, clock)?;
    let to = eval_expr(&args[2], row, clock)?;

    if val.is_null() || from.is_null() || to.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let f = from.to_string_value();
    let t = to.to_string_value();
    Ok(Value::VarChar(s.replace(&f, &t)))
}

fn eval_round(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() < 1 || args.len() > 2 {
        return Err(DbError::Execution("ROUND expects 1 or 2 arguments".into()));
    }
    let val = eval_expr(&args[0], row, clock)?;
    let precision = if args.len() == 2 {
        eval_expr(&args[1], row, clock)?
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

fn eval_math_unary<F>(
    args: &[Expr],
    row: &JoinedRow,
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
    let val = eval_expr(&args[0], row, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let f = value_to_f64(&val)?;
    let result = func(f);
    Ok(Value::VarChar(result.to_string()))
}

fn eval_abs(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("ABS expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, clock)?;
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

fn eval_charindex(args: &[Expr], row: &JoinedRow, clock: &dyn Clock) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DbError::Execution(
            "CHARINDEX expects 2 or 3 arguments".into(),
        ));
    }
    let search = eval_expr(&args[0], row, clock)?;
    let target = eval_expr(&args[1], row, clock)?;

    if search.is_null() || target.is_null() {
        return Ok(Value::Null);
    }

    let search_str = search.to_string_value();
    let target_str = target.to_string_value();

    let start_pos = if args.len() == 3 {
        let sp = eval_expr(&args[2], row, clock)?;
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

fn value_to_f64(v: &Value) -> Result<f64, DbError> {
    match v {
        Value::TinyInt(n) => Ok(*n as f64),
        Value::SmallInt(n) => Ok(*n as f64),
        Value::Int(n) => Ok(*n as f64),
        Value::BigInt(n) => Ok(*n as f64),
        Value::Decimal(raw, scale) => {
            let divisor = 10f64.powi(*scale as i32);
            Ok(*raw as f64 / divisor)
        }
        Value::VarChar(s) | Value::NVarChar(s) => s
            .parse::<f64>()
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to float", s))),
        _ => Err(DbError::Execution(format!(
            "cannot convert {:?} to float",
            v.data_type()
        ))),
    }
}

fn parse_datetime_parts(s: &str) -> Result<(i32, i32, i32, i32, i32, i32), DbError> {
    let s = s.trim();
    let t_parts: Vec<&str> = s.splitn(2, 'T').collect();
    let date_part = t_parts[0];
    let time_part = t_parts.get(1).copied().unwrap_or("00:00:00");

    let date_segments: Vec<&str> = date_part.split('-').collect();
    if date_segments.len() < 3 {
        return Err(DbError::Execution(format!(
            "invalid datetime format: '{}'",
            s
        )));
    }
    let y: i32 = date_segments[0]
        .parse()
        .map_err(|_| DbError::Execution(format!("invalid year in '{}'", s)))?;
    let m: i32 = date_segments[1]
        .parse()
        .map_err(|_| DbError::Execution(format!("invalid month in '{}'", s)))?;
    let d: i32 = date_segments[2]
        .parse()
        .map_err(|_| DbError::Execution(format!("invalid day in '{}'", s)))?;

    let time_segments: Vec<&str> = time_part.split(':').collect();
    let h: i32 = time_segments
        .first()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mi: i32 = time_segments
        .get(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let s_secs: f64 = time_segments
        .get(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    let s: i32 = s_secs as i32;

    Ok((y, m, d, h, mi, s))
}

fn date_to_days(y: i32, m: i32, d: i32) -> i64 {
    let (y_adj, m_adj) = if m <= 2 { (y - 1, m + 12) } else { (y, m) };
    let era = y_adj as i64 / 400;
    let yoe = y_adj as i64 - era * 400;
    let doy = (153 * (m_adj as i64 - 3) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn days_to_date(days: i64) -> (i32, i32, i32) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as i32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as i32;
    let y = if m <= 2 { y + 1 } else { y } as i32;
    (y, m, d)
}

pub(crate) fn eval_binary(op: &BinaryOp, lv: Value, rv: Value) -> Result<Value, DbError> {
    match op {
        BinaryOp::Eq => Ok(compare_bool(lv, rv, |o| o == Ordering::Equal)),
        BinaryOp::NotEq => Ok(compare_bool(lv, rv, |o| o != Ordering::Equal)),
        BinaryOp::Gt => Ok(compare_bool(lv, rv, |o| o == Ordering::Greater)),
        BinaryOp::Lt => Ok(compare_bool(lv, rv, |o| o == Ordering::Less)),
        BinaryOp::Gte => Ok(compare_bool(lv, rv, |o| {
            matches!(o, Ordering::Greater | Ordering::Equal)
        })),
        BinaryOp::Lte => Ok(compare_bool(lv, rv, |o| {
            matches!(o, Ordering::Less | Ordering::Equal)
        })),
        BinaryOp::And => eval_and(lv, rv),
        BinaryOp::Or => eval_or(lv, rv),
        BinaryOp::Add => eval_add(lv, rv),
        BinaryOp::Subtract => eval_subtract(lv, rv),
        BinaryOp::Multiply => eval_multiply(lv, rv),
        BinaryOp::Divide => eval_divide(lv, rv),
        BinaryOp::Modulo => eval_modulo(lv, rv),
    }
}

fn eval_add(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    // String concatenation
    if is_string_type(&lv) || is_string_type(&rv) {
        let ls = lv.to_string_value();
        let rs = rv.to_string_value();
        return Ok(Value::VarChar(format!("{}{}", ls, rs)));
    }
    // Numeric addition
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            let max_scale = as_.max(bs);
            let a = rescale_raw(ar, as_, max_scale);
            let b = rescale_raw(br, bs, max_scale);
            Ok(Value::Decimal(a + b, max_scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            Ok(Value::BigInt(a + b))
        }
    }
}

fn eval_subtract(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            let max_scale = as_.max(bs);
            let a = rescale_raw(ar, as_, max_scale);
            let b = rescale_raw(br, bs, max_scale);
            Ok(Value::Decimal(a - b, max_scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            Ok(Value::BigInt(a - b))
        }
    }
}

fn eval_multiply(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            let result_scale = as_ + bs;
            Ok(Value::Decimal(ar * br, result_scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            Ok(Value::BigInt(a * b))
        }
    }
}

fn eval_divide(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    match (&lv, &rv) {
        (Value::Decimal(_, _), _) | (_, Value::Decimal(_, _)) => {
            let (ar, as_) = to_decimal_parts(&lv);
            let (br, bs) = to_decimal_parts(&rv);
            if br == 0 {
                return Ok(Value::Null);
            }
            let scale = 6u8.max(as_);
            let numerator = rescale_raw(ar, as_, scale + bs);
            Ok(Value::Decimal(numerator / br, scale))
        }
        _ => {
            let a = to_i64(&lv)?;
            let b = to_i64(&rv)?;
            if b == 0 {
                return Ok(Value::Null);
            }
            Ok(Value::BigInt(a / b))
        }
    }
}

fn eval_modulo(lv: Value, rv: Value) -> Result<Value, DbError> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    let a = to_i64(&lv)?;
    let b = to_i64(&rv)?;
    if b == 0 {
        return Ok(Value::Null);
    }
    Ok(Value::BigInt(a % b))
}

fn eval_unary(op: &UnaryOp, val: Value) -> Result<Value, DbError> {
    if val.is_null() {
        return Ok(Value::Null);
    }
    match op {
        UnaryOp::Negate => match val {
            Value::TinyInt(v) => Ok(Value::SmallInt(-(v as i16))),
            Value::SmallInt(v) => Ok(Value::SmallInt(-v)),
            Value::Int(v) => Ok(Value::Int(-v)),
            Value::BigInt(v) => Ok(Value::BigInt(-v)),
            Value::Decimal(raw, scale) => Ok(Value::Decimal(-raw, scale)),
            _ => Err(DbError::Execution(format!(
                "cannot negate value of type {:?}",
                val.data_type()
            ))),
        },
        UnaryOp::Not => Ok(Value::Bit(!truthy(&val))),
    }
}

fn eval_case(
    operand: &Option<Box<Expr>>,
    when_clauses: &[crate::ast::WhenClause],
    else_result: &Option<Box<Expr>>,
    row: &JoinedRow,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let operand_val = operand
        .as_ref()
        .map(|e| eval_expr(e, row, clock))
        .transpose()?;

    for clause in when_clauses {
        let match_found = if let Some(ref op_val) = operand_val {
            let when_val = eval_expr(&clause.condition, row, clock)?;
            compare_bool(op_val.clone(), when_val, |o| o == Ordering::Equal)
        } else {
            let cond = eval_expr(&clause.condition, row, clock)?;
            Value::Bit(truthy(&cond))
        };

        if let Value::Bit(true) = match_found {
            return eval_expr(&clause.result, row, clock);
        }
    }

    match else_result {
        Some(expr) => eval_expr(expr, row, clock),
        None => Ok(Value::Null),
    }
}

fn eval_in_list(
    in_expr: &Expr,
    list: &[Expr],
    negated: bool,
    row: &JoinedRow,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let val = eval_expr(in_expr, row, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let mut found = false;
    for item in list {
        let item_val = eval_expr(item, row, clock)?;
        if item_val.is_null() {
            return Ok(Value::Null);
        }
        if compare_bool(val.clone(), item_val, |o| o == Ordering::Equal) == Value::Bit(true) {
            found = true;
            break;
        }
    }
    Ok(Value::Bit(if negated { !found } else { found }))
}

fn eval_between(
    between_expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    row: &JoinedRow,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let val = eval_expr(between_expr, row, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let low_val = eval_expr(low, row, clock)?;
    let high_val = eval_expr(high, row, clock)?;

    let ge_low = compare_bool(val.clone(), low_val, |o| {
        matches!(o, Ordering::Greater | Ordering::Equal)
    }) == Value::Bit(true);
    let le_high = compare_bool(val, high_val, |o| {
        matches!(o, Ordering::Less | Ordering::Equal)
    }) == Value::Bit(true);

    let result = ge_low && le_high;
    Ok(Value::Bit(if negated { !result } else { result }))
}

fn eval_like(
    like_expr: &Expr,
    pattern: &Expr,
    negated: bool,
    row: &JoinedRow,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let val = eval_expr(like_expr, row, clock)?;
    let pat = eval_expr(pattern, row, clock)?;

    if val.is_null() || pat.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let p = pat.to_string_value();
    let matched = like_match(&s, &p);
    Ok(Value::Bit(if negated { !matched } else { matched }))
}

fn like_match(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_match_impl(&s_chars, 0, &p_chars, 0)
}

fn like_match_impl(s: &[char], si: usize, p: &[char], pi: usize) -> bool {
    if pi >= p.len() {
        return si >= s.len();
    }
    match p[pi] {
        '%' => {
            // % matches zero or more characters
            if pi + 1 >= p.len() {
                return true; // trailing % matches everything
            }
            for skip in 0..=(s.len() - si) {
                if like_match_impl(s, si + skip, p, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // _ matches exactly one character
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

fn is_string_type(v: &Value) -> bool {
    matches!(
        v,
        Value::Char(_) | Value::VarChar(_) | Value::NChar(_) | Value::NVarChar(_)
    )
}

fn to_i64(v: &Value) -> Result<i64, DbError> {
    match v {
        Value::Bit(b) => Ok(if *b { 1 } else { 0 }),
        Value::TinyInt(v) => Ok(*v as i64),
        Value::SmallInt(v) => Ok(*v as i64),
        Value::Int(v) => Ok(*v as i64),
        Value::BigInt(v) => Ok(*v),
        Value::Decimal(raw, scale) => {
            let divisor = 10i128.pow(*scale as u32);
            Ok((*raw / divisor) as i64)
        }
        _ => Err(DbError::Execution(format!(
            "cannot convert {:?} to integer",
            v.data_type()
        ))),
    }
}

fn to_decimal_parts(v: &Value) -> (i128, u8) {
    match v {
        Value::Decimal(raw, scale) => (*raw, *scale),
        Value::Bit(b) => (if *b { 1 } else { 0 }, 0),
        Value::TinyInt(v) => (*v as i128, 0),
        Value::SmallInt(v) => (*v as i128, 0),
        Value::Int(v) => (*v as i128, 0),
        Value::BigInt(v) => (*v as i128, 0),
        _ => (0, 0),
    }
}

fn rescale_raw(raw: i128, from_scale: u8, to_scale: u8) -> i128 {
    if from_scale == to_scale {
        return raw;
    }
    if to_scale > from_scale {
        raw * 10i128.pow((to_scale - from_scale) as u32)
    } else {
        raw / 10i128.pow((from_scale - to_scale) as u32)
    }
}

fn eval_and(lv: Value, rv: Value) -> Result<Value, DbError> {
    match (&lv, &rv) {
        (Value::Null, Value::Null) => Ok(Value::Null),
        (Value::Null, Value::Bit(false)) => Ok(Value::Bit(false)),
        (Value::Bit(false), Value::Null) => Ok(Value::Bit(false)),
        (Value::Null, _) => Ok(Value::Null),
        (_, Value::Null) => Ok(Value::Null),
        _ => Ok(Value::Bit(truthy(&lv) && truthy(&rv))),
    }
}

fn eval_or(lv: Value, rv: Value) -> Result<Value, DbError> {
    match (&lv, &rv) {
        (Value::Null, Value::Null) => Ok(Value::Null),
        (Value::Null, Value::Bit(true)) => Ok(Value::Bit(true)),
        (Value::Bit(true), Value::Null) => Ok(Value::Bit(true)),
        (Value::Null, _) => Ok(Value::Null),
        (_, Value::Null) => Ok(Value::Null),
        _ => Ok(Value::Bit(truthy(&lv) || truthy(&rv))),
    }
}

fn compare_bool<F>(lv: Value, rv: Value, pred: F) -> Value
where
    F: FnOnce(Ordering) -> bool,
{
    if lv.is_null() || rv.is_null() {
        return Value::Null;
    }
    Value::Bit(pred(compare_values(&lv, &rv)))
}
