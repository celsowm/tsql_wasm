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

pub(crate) fn eval_concat(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution("CONCAT requires at least one argument".into()));
    }
    let mut result = String::new();
    for arg in args {
        let val = eval_expr(arg, row, ctx, catalog, storage, clock)?;
        if !val.is_null() {
            result.push_str(&val.to_string_value());
        }
    }
    Ok(Value::NVarChar(result))
}

pub(crate) fn eval_concat_ws(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 {
        return Err(DbError::Execution("CONCAT_WS requires at least 2 arguments".into()));
    }
    let separator_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let separator = if separator_val.is_null() {
        return Ok(Value::Null);
    } else {
        separator_val.to_string_value()
    };
    let mut parts: Vec<String> = Vec::new();
    for arg in &args[1..] {
        let val = eval_expr(arg, row, ctx, catalog, storage, clock)?;
        if !val.is_null() {
            let s = val.to_string_value();
            if !s.is_empty() {
                parts.push(s);
            }
        }
    }
    Ok(Value::NVarChar(parts.join(&separator)))
}

pub(crate) fn eval_replicate(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("REPLICATE expects 2 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let count = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if val.is_null() || count.is_null() {
        return Ok(Value::Null);
    }
    let s = val.to_string_value();
    let n = count.to_integer_i64().unwrap_or(0).max(0) as usize;
    Ok(Value::VarChar(s.repeat(n)))
}

pub(crate) fn eval_reverse(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("REVERSE expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let s = val.to_string_value();
    Ok(Value::NVarChar(s.chars().rev().collect()))
}

pub(crate) fn eval_stuff(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 4 {
        return Err(DbError::Execution("STUFF expects 4 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let start = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let length = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
    let replacement = eval_expr(&args[3], row, ctx, catalog, storage, clock)?;

    if val.is_null() || start.is_null() || length.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let start_i = start.to_integer_i64().unwrap_or(1) as i32;
    let length_i = length.to_integer_i64().unwrap_or(0) as i32;
    let repl = if replacement.is_null() {
        String::new()
    } else {
        replacement.to_string_value()
    };

    let chars: Vec<char> = s.chars().collect();
    let start_idx = if start_i <= 0 { 0 } else { (start_i - 1) as usize };
    let delete_count = if length_i < 0 { 0 } else { length_i as usize };
    let end_idx = (start_idx + delete_count).min(chars.len());

    let mut result: String = chars[..start_idx.min(chars.len())].iter().collect();
    result.push_str(&repl);
    result.extend(chars[end_idx..].iter());

    Ok(Value::NVarChar(result))
}

pub(crate) fn eval_space(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("SPACE expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let n = val.to_integer_i64().unwrap_or(0).max(0) as usize;
    Ok(Value::VarChar(" ".repeat(n)))
}

pub(crate) fn eval_str(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 3 {
        return Err(DbError::Execution("STR expects 1 to 3 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let length = if args.len() >= 2 {
        eval_expr(&args[1], row, ctx, catalog, storage, clock)?
            .to_integer_i64().unwrap_or(10) as usize
    } else {
        10
    };
    let decimals = if args.len() == 3 {
        eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_integer_i64().unwrap_or(0) as usize
    } else {
        0
    };

    let f = match val {
        Value::Float(bits) => f64::from_bits(bits),
        Value::Int(v) => v as f64,
        Value::BigInt(v) => v as f64,
        Value::TinyInt(v) => v as f64,
        Value::SmallInt(v) => v as f64,
        Value::Decimal(raw, scale) => {
            let divisor = 10f64.powi(scale as i32);
            raw as f64 / divisor
        }
        _ => val.to_string_value().parse::<f64>().unwrap_or(0.0),
    };

    let formatted = if decimals > 0 {
        format!("{:.*}", decimals, f)
    } else {
        format!("{:.0}", f)
    };

    let trimmed = formatted.trim();
    if trimmed.len() >= length {
        Ok(Value::VarChar(trimmed.to_string()))
    } else {
        Ok(Value::VarChar(format!("{:>width$}", trimmed, width = length)))
    }
}

pub(crate) fn eval_translate(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("TRANSLATE expects 3 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let from = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let to = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;

    if val.is_null() || from.is_null() || to.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let from_chars: Vec<char> = from.to_string_value().chars().collect();
    let to_chars: Vec<char> = to.to_string_value().chars().collect();

    if from_chars.len() != to_chars.len() {
        return Err(DbError::Execution(
            "TRANSLATE: the second and third arguments must have the same length".into(),
        ));
    }

    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        if let Some(pos) = from_chars.iter().position(|&fc| fc == c) {
            result.push(to_chars[pos]);
        } else {
            result.push(c);
        }
    }
    Ok(Value::NVarChar(result))
}

pub(crate) fn eval_format(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DbError::Execution("FORMAT expects 2 or 3 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let format_str = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if val.is_null() {
        return Ok(Value::Null);
    }

    let fmt = format_str.to_string_value();

    match val {
        Value::DateTime(ref s) | Value::DateTime2(ref s) | Value::Date(ref s) => {
            Ok(Value::NVarChar(crate::executor::scalar::datetime::format_datetime_string(s, &fmt)))
        }
        Value::Int(v) => Ok(Value::NVarChar(format_integer(v, &fmt))),
        Value::BigInt(v) => Ok(Value::NVarChar(format_integer(v, &fmt))),
        Value::Float(bits) => {
            let f = f64::from_bits(bits);
            Ok(Value::NVarChar(format_float_value(f, &fmt)))
        }
        _ => Ok(Value::NVarChar(val.to_string_value())),
    }
}

fn format_integer<T: std::fmt::Display>(v: T, fmt: &str) -> String {
    match fmt {
        "N" | "n" => {
            let s = format!("{}", v);
            format_number_with_commas(&s)
        }
        "C" | "c" => format!("${}", v),
        "P" | "p" => format!("{}%", v),
        _ => format!("{}", v),
    }
}

fn format_float_value(f: f64, fmt: &str) -> String {
    if fmt.starts_with('N') || fmt.starts_with('n') {
        let decimals: usize = if fmt.len() > 1 { fmt[1..].parse().unwrap_or(2) } else { 2 };
        let s = format!("{:.*}", decimals, f);
        format_number_with_commas(&s)
    } else if fmt.starts_with('C') || fmt.starts_with('c') {
        let decimals: usize = if fmt.len() > 1 { fmt[1..].parse().unwrap_or(2) } else { 2 };
        format!("${:.*}", decimals, f)
    } else if fmt.starts_with('P') || fmt.starts_with('p') {
        let decimals: usize = if fmt.len() > 1 { fmt[1..].parse().unwrap_or(2) } else { 2 };
        format!("{:.*}%", decimals, f * 100.0)
    } else {
        format!("{}", f)
    }
}

fn format_number_with_commas(s: &str) -> String {
    let (negative, abs) = if s.starts_with('-') {
        (true, &s[1..])
    } else {
        (false, s)
    };
    let parts: Vec<&str> = abs.splitn(2, '.').collect();
    let int_part = parts[0];
    let frac_part = parts.get(1).copied();

    let mut result = String::new();
    let chars: Vec<char> = int_part.chars().collect();
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }
    if let Some(frac) = frac_part {
        result.push('.');
        result.push_str(frac);
    }
    if negative {
        format!("-{}", result)
    } else {
        result
    }
}

pub(crate) fn eval_patindex(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("PATINDEX expects 2 arguments".into()));
    }
    let pattern_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let target_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if pattern_val.is_null() || target_val.is_null() {
        return Ok(Value::Null);
    }

    let pattern = pattern_val.to_string_value();
    let target = target_val.to_string_value();

    let pat = pattern.trim_start_matches('%').trim_end_matches('%');
    let starts_with_wild = pattern.starts_with('%');
    let ends_with_wild = pattern.ends_with('%');

    let result = if starts_with_wild && ends_with_wild {
        target.find(pat).map(|pos| (pos + 1) as i64).unwrap_or(0)
    } else if starts_with_wild {
        target.rfind(pat).map(|pos| (pos + 1) as i64).unwrap_or(0)
    } else if ends_with_wild {
        target.find(pat).map(|pos| (pos + 1) as i64).unwrap_or(0)
    } else {
        if target == pat { 1 } else { 0 }
    };

    Ok(Value::Int(result as i32))
}

pub(crate) fn eval_soundex(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("SOUNDEX expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let s = val.to_string_value();
    Ok(Value::VarChar(soundex(&s)))
}

fn soundex(s: &str) -> String {
    let chars: Vec<char> = s.to_uppercase().chars().filter(|c| c.is_ascii_alphabetic()).collect();
    if chars.is_empty() {
        return "0000".to_string();
    }
    let mut result = String::with_capacity(4);
    result.push(chars[0]);

    let mut prev_code = soundex_code(chars[0]);
    for &c in &chars[1..] {
        let code = soundex_code(c);
        if code != '0' && code != prev_code {
            result.push(code);
            if result.len() == 4 { break; }
        }
        prev_code = code;
    }
    while result.len() < 4 {
        result.push('0');
    }
    result
}

fn soundex_code(c: char) -> char {
    match c {
        'B' | 'F' | 'P' | 'V' => '1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
        'D' | 'T' => '3',
        'L' => '4',
        'M' | 'N' => '5',
        'R' => '6',
        _ => '0',
    }
}

pub(crate) fn eval_difference(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("DIFFERENCE expects 2 arguments".into()));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }
    let sx1 = soundex(&s1.to_string_value());
    let sx2 = soundex(&s2.to_string_value());
    let matches = sx1.chars().zip(sx2.chars()).filter(|(a, b)| a == b).count();
    Ok(Value::Int(matches as i32))
}

pub(crate) fn eval_ascii(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("ASCII expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let s = val.to_string_value();
    match s.chars().next() {
        Some(c) => Ok(Value::Int(c as i32)),
        None => Ok(Value::Null),
    }
}

pub(crate) fn eval_char(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("CHAR expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let code = val.to_integer_i64().unwrap_or(0);
    if code < 0 || code > 255 {
        return Ok(Value::Null);
    }
    match char::from_u32(code as u32) {
        Some(c) => Ok(Value::VarChar(c.to_string())),
        None => Ok(Value::Null),
    }
}

pub(crate) fn eval_nchar(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("NCHAR expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let code = val.to_integer_i64().unwrap_or(0);
    if code < 0 || code > 0x10FFFF {
        return Ok(Value::Null);
    }
    match char::from_u32(code as u32) {
        Some(c) => Ok(Value::NVarChar(c.to_string())),
        None => Ok(Value::Null),
    }
}

pub(crate) fn eval_unicode(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("UNICODE expects 1 argument".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let s = val.to_string_value();
    match s.chars().next() {
        Some(c) => Ok(Value::Int(c as i32)),
        None => Ok(Value::Null),
    }
}

pub(crate) fn eval_string_escape(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("STRING_ESCAPE expects 2 arguments".into()));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let escape_type = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if val.is_null() || escape_type.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let typ = escape_type.to_string_value().to_uppercase();

    let result = match typ.as_str() {
        "JSON" => {
            let mut out = String::with_capacity(s.len() + 8);
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '/' => out.push_str("\\/"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    '\u{08}' => out.push_str("\\b"),
                    '\u{0C}' => out.push_str("\\f"),
                    _ if c < '\x20' => {
                        out.push_str(&format!("\\u{:04x}", c as u32));
                    }
                    _ => out.push(c),
                }
            }
            out
        }
        "HTML" => {
            s.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;")
                .replace('\'', "&#39;")
        }
        "XML" => {
            s.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;")
                .replace('\'', "&apos;")
        }
        _ => return Err(DbError::Execution(format!(
            "Unsupported escape type '{}'. Supported: JSON, HTML, XML", typ
        ))),
    };

    Ok(Value::NVarChar(result))
}
