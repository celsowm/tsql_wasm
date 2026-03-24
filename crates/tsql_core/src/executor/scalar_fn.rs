use crate::ast::Expr;
use crate::catalog::{Catalog, RoutineKind};
use crate::error::DbError;
use crate::types::Value;

use super::aggregates::{dispatch_aggregate, is_aggregate_function};
use super::clock::Clock;
use super::context::ExecutionContext;
use super::date_time::{apply_dateadd, day_of_week_from_date, date_to_days, parse_datetime_parts};
use super::evaluator::eval_expr;
use super::fuzzy;
use super::json;
use super::model::ContextTable;
use super::regexp;
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
    if is_aggregate_function(name) {
        if let Some(group) = ctx.current_group.clone() {
            if let Some(res) = dispatch_aggregate(name, args, &group, ctx, catalog, storage, clock) {
                return res;
            }
        }
    }

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
    } else if name.eq_ignore_ascii_case("DATEPART") {
        eval_datepart(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("DATENAME") {
        eval_datename(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("YEAR") {
        eval_year(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("MONTH") {
        eval_month(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("DAY") {
        eval_day(args, row, ctx, catalog, storage, clock)
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
    } else if name.eq_ignore_ascii_case("POWER") {
        eval_power(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("SQRT") {
        eval_sqrt(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("SIGN") {
        eval_sign(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("LEFT") {
        eval_left(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("RIGHT") {
        eval_right(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("CHARINDEX") {
        eval_charindex(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("JSON_VALUE") {
        eval_json_value(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("JSON_QUERY") {
        eval_json_query(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("JSON_MODIFY") {
        eval_json_modify(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("ISJSON") {
        eval_isjson(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("JSON_ARRAY_LENGTH") {
        eval_json_array_length(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("JSON_KEYS") {
        eval_json_keys(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("REGEXP_LIKE") {
        eval_regexp_like(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("REGEXP_REPLACE") {
        eval_regexp_replace(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("REGEXP_SUBSTR") {
        eval_regexp_substr(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("REGEXP_INSTR") {
        eval_regexp_instr(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("REGEXP_COUNT") {
        eval_regexp_count(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("CURRENT_DATE") {
        if !args.is_empty() {
            return Err(DbError::Execution("CURRENT_DATE expects no arguments".into()));
        }
        let dt = clock.now_datetime_literal();
        let date_str = if dt.len() >= 10 { &dt[..10] } else { "1970-01-01" };
        Ok(Value::Date(date_str.to_string()))
    } else if name.eq_ignore_ascii_case("UNISTR") {
        eval_unistr(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("EDIT_DISTANCE") {
        eval_edit_distance(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("EDIT_DISTANCE_SIMILARITY") {
        eval_edit_distance_similarity(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("JARO_WINKLER_DISTANCE") {
        eval_jaro_winkler_distance(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("JARO_WINKLER_SIMILARITY") {
        eval_jaro_winkler_similarity(args, row, ctx, catalog, storage, clock)
    } else if name.eq_ignore_ascii_case("NEWID") {
        if !args.is_empty() {
            return Err(DbError::Execution("NEWID expects no arguments".into()));
        }
        let uuid = deterministic_uuid(&mut *ctx.random_state);
        Ok(Value::UniqueIdentifier(uuid))
    } else if name.eq_ignore_ascii_case("RAND") {
        let val = deterministic_rand(&mut *ctx.random_state);
        Ok(Value::Decimal((val * 1_000_000_000.0) as i128, 9))
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
    } else if name.eq_ignore_ascii_case("@@VERSION") {
        Ok(Value::NVarChar(
            "Microsoft SQL Server 2022 (RTM) - 16.0.1000.6 (tsql_wasm emulator)".into(),
        ))
    } else if name.eq_ignore_ascii_case("@@SERVERNAME") {
        Ok(Value::NVarChar("localhost".into()))
    } else if name.eq_ignore_ascii_case("@@SERVICENAME") {
        Ok(Value::NVarChar("MSSQLSERVER".into()))
    } else if name.eq_ignore_ascii_case("@@SPID") {
        Ok(Value::SmallInt(1))
    } else if name.eq_ignore_ascii_case("@@TRANCOUNT") {
        Ok(Value::Int(0))
    } else if name.eq_ignore_ascii_case("@@ERROR") {
        Ok(Value::Int(0))
    } else if name.eq_ignore_ascii_case("@@FETCH_STATUS") {
        Ok(Value::Int(-1))
    } else if name.eq_ignore_ascii_case("@@LANGUAGE") {
        Ok(Value::NVarChar("us_english".into()))
    } else if name.eq_ignore_ascii_case("@@TEXTSIZE") {
        Ok(Value::Int(2147483647))
    } else if name.eq_ignore_ascii_case("@@MAX_PRECISION") {
        Ok(Value::TinyInt(38))
    } else if name.eq_ignore_ascii_case("@@DATEFIRST") {
        Ok(Value::TinyInt(ctx.datefirst as u8))
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
        Value::Float(bits) => f64::from_bits(*bits),
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
    let s = rounded.to_string();
    // Strip trailing ".0" for whole numbers
    let s = if s.ends_with(".0") { s[..s.len()-2].to_string() } else { s };
    Ok(Value::VarChar(s))
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
    let s = result.to_string();
    // Strip trailing ".0" for whole numbers (e.g., "5.0" -> "5")
    let s = if s.ends_with(".0") { s[..s.len()-2].to_string() } else { s };
    Ok(Value::VarChar(s))
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

pub(crate) fn eval_power(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("POWER expects 2 arguments".into()));
    }
    let base = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let exponent = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if base.is_null() || exponent.is_null() {
        return Ok(Value::Null);
    }

    let b = value_to_f64(&base)?;
    let e = value_to_f64(&exponent)?;
    let result = b.powf(e);
    Ok(Value::Float(result.to_bits()))
}

pub(crate) fn eval_sqrt(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("SQRT expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;

    if val.is_null() {
        return Ok(Value::Null);
    }

    let f = value_to_f64(&val)?;
    let result = f.sqrt();
    Ok(Value::Float(result.to_bits()))
}

pub(crate) fn eval_sign(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("SIGN expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;

    if val.is_null() {
        return Ok(Value::Null);
    }

    let f = value_to_f64(&val)?;
    let result = if f > 0.0 { 1 } else if f < 0.0 { -1 } else { 0 };
    Ok(Value::Int(result))
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

fn deterministic_uuid(state: &mut u64) -> String {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    let bytes = state.to_be_bytes();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[0] ^ bytes[4], bytes[1] ^ bytes[5],
        bytes[2] ^ bytes[6], bytes[3] ^ bytes[7],
        bytes[4] ^ bytes[0], bytes[5] ^ bytes[1],
        bytes[6] ^ bytes[2], bytes[7] ^ bytes[3]
    )
}

fn deterministic_rand(state: &mut u64) -> f64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    let bits = (*state >> 33) as u32;
    bits as f64 / (1u64 << 31) as f64
}

fn eval_json_value(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("JSON_VALUE expects 2 arguments".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let path_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if json_val.is_null() {
        return Ok(Value::Null);
    }

    let json_str = json_val.to_string_value();
    let path = path_val.to_string_value();

    json::json_value(&json_str, &path)
}

fn eval_json_query(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("JSON_QUERY expects 2 arguments".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let path_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if json_val.is_null() {
        return Ok(Value::Null);
    }

    let json_str = json_val.to_string_value();
    let path = path_val.to_string_value();

    json::json_query(&json_str, &path)
}

fn eval_json_modify(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("JSON_MODIFY expects 3 arguments".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let path_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let new_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;

    if json_val.is_null() {
        return Ok(Value::Null);
    }

    let json_str = json_val.to_string_value();
    let path = path_val.to_string_value();
    let new_value_str = new_val.to_string_value();

    json::json_modify(&json_str, &path, &new_value_str)
}

fn eval_isjson(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("ISJSON expects 1 argument".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;

    if json_val.is_null() {
        return Ok(Value::Null);
    }

    let json_str = json_val.to_string_value();
    json::is_json(&json_str)
}

fn eval_json_array_length(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("JSON_ARRAY_LENGTH expects 1 argument".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;

    if json_val.is_null() {
        return Ok(Value::Null);
    }

    let json_str = json_val.to_string_value();
    json::json_array_length(&json_str)
}

fn eval_json_keys(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution("JSON_KEYS expects 1 or 2 arguments".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;

    if json_val.is_null() {
        return Ok(Value::Null);
    }

    let json_str = json_val.to_string_value();
    let path = if args.len() == 2 {
        let path_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
        Some(path_val.to_string_value())
    } else {
        None
    };

    json::json_keys(&json_str, path.as_deref())
}

fn eval_regexp_like(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DbError::Execution("REGEXP_LIKE expects 2 or 3 arguments".into()));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if s_val.is_null() || p_val.is_null() {
        return Ok(Value::Null);
    }

    let s = s_val.to_string_value();
    let pattern = p_val.to_string_value();
    let flags = if args.len() == 3 {
        let f_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
        Some(f_val.to_string_value())
    } else {
        None
    };

    regexp::regexp_like(&s, &pattern, flags.as_deref())
}

fn eval_regexp_replace(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 3 || args.len() > 4 {
        return Err(DbError::Execution("REGEXP_REPLACE expects 3 or 4 arguments".into()));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let r_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;

    if s_val.is_null() || p_val.is_null() || r_val.is_null() {
        return Ok(Value::Null);
    }

    let s = s_val.to_string_value();
    let pattern = p_val.to_string_value();
    let replacement = r_val.to_string_value();
    let flags = if args.len() == 4 {
        let f_val = eval_expr(&args[3], row, ctx, catalog, storage, clock)?;
        Some(f_val.to_string_value())
    } else {
        None
    };

    regexp::regexp_replace(&s, &pattern, &replacement, flags.as_deref())
}

fn eval_regexp_substr(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 5 {
        return Err(DbError::Execution("REGEXP_SUBSTR expects 2 to 5 arguments".into()));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if s_val.is_null() || p_val.is_null() {
        return Ok(Value::Null);
    }

    let s = s_val.to_string_value();
    let pattern = p_val.to_string_value();
    let pos = if args.len() >= 3 {
        let pos_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
        pos_val.to_string_value().parse::<usize>().unwrap_or(1)
    } else {
        1
    };
    let occurrence = if args.len() >= 4 {
        let occ_val = eval_expr(&args[3], row, ctx, catalog, storage, clock)?;
        occ_val.to_string_value().parse::<usize>().unwrap_or(0)
    } else {
        0
    };
    let flags = if args.len() == 5 {
        let f_val = eval_expr(&args[4], row, ctx, catalog, storage, clock)?;
        Some(f_val.to_string_value())
    } else {
        None
    };

    regexp::regexp_substr(&s, &pattern, pos, occurrence, flags.as_deref())
}

fn eval_regexp_instr(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 6 {
        return Err(DbError::Execution("REGEXP_INSTR expects 2 to 6 arguments".into()));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if s_val.is_null() || p_val.is_null() {
        return Ok(Value::Null);
    }

    let s = s_val.to_string_value();
    let pattern = p_val.to_string_value();
    let pos = if args.len() >= 3 {
        let pos_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
        pos_val.to_string_value().parse::<usize>().unwrap_or(1)
    } else {
        1
    };
    let occurrence = if args.len() >= 4 {
        let occ_val = eval_expr(&args[3], row, ctx, catalog, storage, clock)?;
        occ_val.to_string_value().parse::<usize>().unwrap_or(0)
    } else {
        0
    };
    let return_opt = if args.len() >= 5 {
        let ret_val = eval_expr(&args[4], row, ctx, catalog, storage, clock)?;
        ret_val.to_string_value().parse::<usize>().unwrap_or(0)
    } else {
        0
    };
    let flags = if args.len() == 6 {
        let f_val = eval_expr(&args[5], row, ctx, catalog, storage, clock)?;
        Some(f_val.to_string_value())
    } else {
        None
    };

    regexp::regexp_instr(&s, &pattern, pos, occurrence, return_opt, flags.as_deref())
}

fn eval_regexp_count(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(DbError::Execution("REGEXP_COUNT expects 2 to 4 arguments".into()));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if s_val.is_null() || p_val.is_null() {
        return Ok(Value::Null);
    }

    let s = s_val.to_string_value();
    let pattern = p_val.to_string_value();
    let pos = if args.len() >= 3 {
        let pos_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
        pos_val.to_string_value().parse::<usize>().unwrap_or(1)
    } else {
        1
    };
    let flags = if args.len() == 4 {
        let f_val = eval_expr(&args[3], row, ctx, catalog, storage, clock)?;
        Some(f_val.to_string_value())
    } else {
        None
    };

    regexp::regexp_count(&s, &pattern, pos, flags.as_deref())
}

fn eval_unistr(
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
    let result = process_unicode_escapes(&s)?;
    Ok(Value::NVarChar(result))
}

fn process_unicode_escapes(s: &str) -> Result<String, DbError> {
    let mut result = String::new();
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(&next) = chars.peek() {
                if next == 'u' || next == 'U' {
                    chars.next();
                    let mut hex = String::new();
                    while let Some(&h) = chars.peek() {
                        if h.is_ascii_hexdigit() {
                            hex.push(h);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    if !hex.is_empty() {
                        let codepoint = u32::from_str_radix(&hex, 16)
                            .map_err(|_| DbError::Execution(format!("Invalid Unicode escape: \\{}", hex)))?;
                        if let Some(ch) = char::from_u32(codepoint) {
                            result.push(ch);
                        } else {
                            return Err(DbError::Execution(format!("Invalid Unicode codepoint: {}", codepoint)));
                        }
                    } else {
                        result.push('\\');
                        result.push(next);
                    }
                } else {
                    result.push(c);
                    result.push(next);
                    chars.next();
                }
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
        }
    }

    Ok(result)
}

fn eval_edit_distance(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("EDIT_DISTANCE expects 2 arguments".into()));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }

    let str1 = s1.to_string_value();
    let str2 = s2.to_string_value();

    Ok(Value::Int(fuzzy::edit_distance(&str1, &str2)))
}

fn eval_edit_distance_similarity(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("EDIT_DISTANCE_SIMILARITY expects 2 arguments".into()));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }

    let str1 = s1.to_string_value();
    let str2 = s2.to_string_value();

    let similarity = fuzzy::edit_distance_similarity(&str1, &str2);
    Ok(Value::Decimal((similarity * 1_000_000_000.0) as i128, 9))
}

fn eval_jaro_winkler_distance(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("JARO_WINKLER_DISTANCE expects 2 arguments".into()));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }

    let str1 = s1.to_string_value();
    let str2 = s2.to_string_value();

    let distance = fuzzy::jaro_winkler_distance(&str1, &str2);
    Ok(Value::Decimal((distance * 1_000_000_000.0) as i128, 9))
}

fn eval_jaro_winkler_similarity(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("JARO_WINKLER_SIMILARITY expects 2 arguments".into()));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }

    let str1 = s1.to_string_value();
    let str2 = s2.to_string_value();

    let similarity = fuzzy::jaro_winkler_similarity(&str1, &str2);
    Ok(Value::Decimal((similarity * 1_000_000_000.0) as i128, 9))
}
