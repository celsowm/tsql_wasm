use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::types::Value;
use crate::storage::Storage;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr;
use super::super::model::ContextTable;
use super::super::date_time::{apply_dateadd, day_of_week_from_date, date_to_days, parse_datetime_parts};

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

pub(crate) fn eval_datepart(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("DATEPART expects 2 arguments".into()));
    }

    let part = extract_datepart(&args[0])?;
    let date_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let date_str = date_val.to_string_value();
    let (y, m, d, h, mi, s) = parse_datetime_parts(&date_str)?;

    let result = match part.as_str() {
        "year" | "yy" | "yyyy" => y as i32,
        "month" | "mm" | "m" => m,
        "day" | "dd" | "d" => d,
        "hour" | "hh" => h,
        "minute" | "mi" | "n" => mi,
        "second" | "ss" | "s" => s,
        "weekday" | "dw" | "w" => {
            let dow = day_of_week_from_date(y, m, d);
            let datefirst = ctx.datefirst;
            ((dow - datefirst + 7) % 7 + 1) as i32
        }
        "dayofweek" => {
            let dow = day_of_week_from_date(y, m, d);
            (dow + 1) as i32
        }
        "dayofyear" | "dy" => {
            let days = date_to_days(y, m, d);
            let jan1 = date_to_days(y, 1, 1);
            ((days - jan1) + 1) as i32
        }
        "quarter" | "qq" | "q" => {
            ((m - 1) / 3 + 1) as i32
        }
        "millisecond" | "ms" => 0i32,
        "microsecond" | "mcs" => 0i32,
        "nanosecond" | "ns" => 0i32,
        _ => return Err(DbError::Execution(format!("unknown datepart '{}'", part))),
    };

    Ok(Value::Int(result))
}

pub(crate) fn eval_datename(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("DATENAME expects 2 arguments".into()));
    }

    let part = extract_datepart(&args[0])?;
    let date_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let date_str = date_val.to_string_value();
    let (y, m, d, _, _, _) = parse_datetime_parts(&date_str)?;

    let result = match part.as_str() {
        "year" | "yy" | "yyyy" => format!("{}", y),
        "month" | "mm" | "m" => {
            let months = [
                "", "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December",
            ];
            months.get(m as usize).unwrap_or(&"").to_string()
        }
        "day" | "dd" | "d" => format!("{}", d),
        "dayofweek" | "dw" | "weekday" | "w" => {
            let dow = day_of_week_from_date(y, m, d);
            let datefirst = ctx.datefirst;
            let adjusted = ((dow - datefirst + 7) % 7) as usize;
            let day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
            day_names[adjusted].to_string()
        }
        "dayofyear" | "dy" => {
            let days = date_to_days(y, m, d);
            let jan1 = date_to_days(y, 1, 1);
            format!("{}", (days - jan1) + 1)
        }
        "quarter" | "qq" | "q" => format!("{}", ((m - 1) / 3 + 1)),
        "hour" | "hh" => {
            let time_part = date_str.split('T').nth(1).unwrap_or("00:00:00");
            let hour: i32 = time_part.split(':').next().unwrap_or("0").parse().unwrap_or(0);
            format!("{}", hour)
        }
        "minute" | "mi" | "n" => {
            let time_part = date_str.split('T').nth(1).unwrap_or("00:00:00");
            let parts: Vec<&str> = time_part.split(':').collect();
            let minute: i32 = parts.get(1).and_then(|v| v.parse().ok()).unwrap_or(0);
            format!("{}", minute)
        }
        "second" | "ss" | "s" => {
            let time_part = date_str.split('T').nth(1).unwrap_or("00:00:00");
            let parts: Vec<&str> = time_part.split(':').collect();
            let second: i32 = parts.get(2).and_then(|v| v.parse().ok()).unwrap_or(0);
            format!("{}", second)
        }
        "millisecond" | "ms" => "0".to_string(),
        _ => return Err(DbError::Execution(format!("unknown datepart '{}'", part))),
    };

    Ok(Value::VarChar(result))
}

pub(crate) fn eval_year(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("YEAR expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let date_str = val.to_string_value();
    match parse_datetime_parts(&date_str) {
        Ok((y, _, _, _, _, _)) => Ok(Value::Int(y)),
        Err(_) => Ok(Value::Null),
    }
}

pub(crate) fn eval_month(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("MONTH expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let date_str = val.to_string_value();
    match parse_datetime_parts(&date_str) {
        Ok((_, m, _, _, _, _)) => Ok(Value::Int(m)),
        Err(_) => Ok(Value::Null),
    }
}

pub(crate) fn eval_day(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("DAY expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let date_str = val.to_string_value();
    match parse_datetime_parts(&date_str) {
        Ok((_, _, d, _, _, _)) => Ok(Value::Int(d)),
        Err(_) => Ok(Value::Null),
    }
}

pub(crate) fn format_datetime_string(dt: &str, fmt: &str) -> String {
    match fmt.to_lowercase().as_str() {
        "yyyy" | "yyyy-mm-dd" => {
            if dt.len() >= 10 { dt[..10].to_string() } else { dt.to_string() }
        }
        "mm/dd/yyyy" => {
            if let Ok((y, m, d, _, _, _)) = parse_datetime_parts(dt) {
                format!("{:02}/{:02}/{}", m, d, y)
            } else {
                dt.to_string()
            }
        }
        "dd/mm/yyyy" => {
            if let Ok((y, m, d, _, _, _)) = parse_datetime_parts(dt) {
                format!("{:02}/{:02}/{}", d, m, y)
            } else {
                dt.to_string()
            }
        }
        "dd MMM yyyy" | "dd MMM yyyy hh:mi:ss" => {
            if let Ok((y, m, d, h, mi, s)) = parse_datetime_parts(dt) {
                let months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
                let mon = if m >= 1 && m <= 12 { months[m as usize] } else { "???" };
                if h > 0 || mi > 0 || s > 0 {
                    format!("{:02} {} {} {:02}:{:02}:{:02}", d, mon, y, h, mi, s)
                } else {
                    format!("{:02} {} {}", d, mon, y)
                }
            } else {
                dt.to_string()
            }
        }
        "hh:mi:ss" => {
            if let Ok((_, _, _, h, mi, s)) = parse_datetime_parts(dt) {
                format!("{:02}:{:02}:{:02}", h, mi, s)
            } else {
                dt.to_string()
            }
        }
        _ => dt.to_string(),
    }
}
