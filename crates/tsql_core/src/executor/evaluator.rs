use std::cmp::Ordering;

use crate::ast::{BinaryOp, Expr};
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
        Expr::String(v) => Ok(Value::VarChar(v.clone())),
        Expr::UnicodeString(v) => Ok(Value::NVarChar(v.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::FunctionCall { name, args } => eval_function(name, args, row, clock),
        Expr::Binary { left, op, right } => {
            let lv = eval_expr(left, row, clock)?;
            let rv = eval_expr(right, row, clock)?;
            eval_binary(op, lv, rv)
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
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => contains_aggregate(inner),
        Expr::Cast { expr, .. } | Expr::Convert { expr, .. } => contains_aggregate(expr),
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
