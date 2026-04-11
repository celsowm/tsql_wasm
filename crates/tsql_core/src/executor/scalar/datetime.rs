use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::date_time::{
    apply_dateadd, date_to_days, day_of_week_from_date, parse_datetime_parts,
};
use super::super::evaluator::eval_expr;
use super::super::model::ContextTable;

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

    let result = apply_dateadd(&part, num, &date_str, &ctx.options.dateformat)?;

    match date_val {
        Value::Date(_) => Ok(Value::Date(result.date())),
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

    let (sy, sm, sd, sh, smi, ss) = parse_datetime_parts(&start_str, &ctx.options.dateformat)?;
    let (ey, em, ed, ehi, emi, es) = parse_datetime_parts(&end_str, &ctx.options.dateformat)?;

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
    let (y, m, d, h, mi, s) = parse_datetime_parts(&date_str, &ctx.options.dateformat)?;

    let result = match part.as_str() {
        "year" | "yy" | "yyyy" => y,
        "month" | "mm" | "m" => m,
        "day" | "dd" | "d" => d,
        "hour" | "hh" => h,
        "minute" | "mi" | "n" => mi,
        "second" | "ss" | "s" => s,
        "weekday" | "dw" | "w" => {
            let dow = day_of_week_from_date(y, m, d);
            let datefirst = ctx.metadata.datefirst;
            (dow - datefirst + 7) % 7 + 1
        }
        "dayofweek" => {
            let dow = day_of_week_from_date(y, m, d);
            dow + 1
        }
        "dayofyear" | "dy" => {
            let days = date_to_days(y, m, d);
            let jan1 = date_to_days(y, 1, 1);
            ((days - jan1) + 1) as i32
        }
        "quarter" | "qq" | "q" => (m - 1) / 3 + 1,
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
    let (y, m, d, _, _, _) = parse_datetime_parts(&date_str, &ctx.options.dateformat)?;

    let result = match part.as_str() {
        "year" | "yy" | "yyyy" => format!("{}", y),
        "month" | "mm" | "m" => {
            let months = [
                "",
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            ];
            months.get(m as usize).unwrap_or(&"").to_string()
        }
        "day" | "dd" | "d" => format!("{}", d),
        "dayofweek" | "dw" | "weekday" | "w" => {
            let dow = day_of_week_from_date(y, m, d);
            let datefirst = ctx.metadata.datefirst;
            let adjusted = ((dow - datefirst + 7) % 7) as usize;
            let day_names = [
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ];
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
            let hour: i32 = time_part
                .split(':')
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
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
    match parse_datetime_parts(&date_str, &ctx.options.dateformat) {
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
    match parse_datetime_parts(&date_str, &ctx.options.dateformat) {
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
    match parse_datetime_parts(&date_str, &ctx.options.dateformat) {
        Ok((_, _, d, _, _, _)) => Ok(Value::Int(d)),
        Err(_) => Ok(Value::Null),
    }
}

pub(crate) fn eval_getdate(
    _args: &[Expr],
    _row: &[ContextTable],
    _ctx: &mut ExecutionContext,
    _catalog: &dyn Catalog,
    _storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    Ok(Value::DateTime(clock.now_datetime_literal()))
}

pub(crate) fn eval_current_timestamp(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    eval_getdate(args, row, ctx, catalog, storage, clock)
}

pub(crate) fn eval_current_date(
    _args: &[Expr],
    _row: &[ContextTable],
    _ctx: &mut ExecutionContext,
    _catalog: &dyn Catalog,
    _storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    Ok(Value::Date(clock.now_datetime_literal().date()))
}

pub(crate) fn eval_getutcdate(
    _args: &[Expr],
    _row: &[ContextTable],
    _ctx: &mut ExecutionContext,
    _catalog: &dyn Catalog,
    _storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    Ok(Value::DateTime(clock.now_datetime_literal())) // Simplified
}

pub(crate) fn eval_sysdatetime(
    _args: &[Expr],
    _row: &[ContextTable],
    _ctx: &mut ExecutionContext,
    _catalog: &dyn Catalog,
    _storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    Ok(Value::DateTime2(clock.now_datetime_literal()))
}

pub(crate) fn eval_sysutcdatetime(
    _args: &[Expr],
    _row: &[ContextTable],
    _ctx: &mut ExecutionContext,
    _catalog: &dyn Catalog,
    _storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    Ok(Value::DateTime2(clock.now_datetime_literal())) // Simplified
}

pub(crate) fn eval_sysdatetimeoffset(
    _args: &[Expr],
    _row: &[ContextTable],
    _ctx: &mut ExecutionContext,
    _catalog: &dyn Catalog,
    _storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    // Value doesn't have DateTimeOffset, returning DateTime2
    Ok(Value::DateTime2(clock.now_datetime_literal()))
}

pub(crate) fn eval_eomonth(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution("EOMONTH expects 1 or 2 arguments".into()));
    }
    let date_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if date_val.is_null() {
        return Ok(Value::Null);
    }
    let date_str = date_val.to_string_value();
    let (mut y, mut m, _, _, _, _) = parse_datetime_parts(&date_str, &ctx.options.dateformat)?;

    if args.len() == 2 {
        let add = eval_expr(&args[1], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(0);
        m += add as i32;
        while m > 12 {
            y += 1;
            m -= 12;
        }
        while m <= 0 {
            y -= 1;
            m += 12;
        }
    }

    let is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
    let days = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => if is_leap { 29 } else { 28 },
        _ => 30,
    };
    let last_day = format!("{:04}-{:02}-{:02}T00:00:00", y, m, days);
    let dt = chrono::NaiveDateTime::parse_from_str(&last_day, "%Y-%m-%dT%H:%M:%S")
        .map_err(|e| DbError::Execution(format!("Invalid date: {}", e)))?;
    Ok(Value::Date(dt.date()))
}

pub(crate) fn eval_isdate(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() {
        return Ok(Value::Int(0));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Int(0));
    }
    let s = val.to_string_value();
    let is_valid = parse_datetime_parts(&s, &ctx.options.dateformat).is_ok();
    Ok(Value::Int(if is_valid { 1 } else { 0 }))
}

pub(crate) fn eval_datediff_big(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match eval_datediff(args, row, ctx, catalog, storage, clock)? {
        Value::Int(v) => Ok(Value::BigInt(v as i64)),
        other => Ok(other),
    }
}
