use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::super::clock::Clock;
use super::super::super::context::ExecutionContext;
use super::super::super::evaluator::eval_expr;
use super::super::super::model::ContextTable;

pub(crate) fn eval_concat(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
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
    let sep_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let sep = sep_val.to_string_value();
    let mut parts = Vec::new();
    for arg in &args[1..] {
        let val = eval_expr(arg, row, ctx, catalog, storage, clock)?;
        if !val.is_null() {
            parts.push(val.to_string_value());
        }
    }
    Ok(Value::NVarChar(parts.join(&sep)))
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
        Value::DateTime(dt) | Value::DateTime2(dt) => {
            Ok(Value::NVarChar(dt.format(&fmt).to_string()))
        }
        Value::Date(d) => Ok(Value::NVarChar(d.format(&fmt).to_string())),
        Value::Int(v) => Ok(Value::NVarChar(format_integer(v, &fmt))),
        Value::BigInt(v) => Ok(Value::NVarChar(format_integer(v, &fmt))),
        Value::Float(bits) => {
            let f = f64::from_bits(bits);
            Ok(Value::NVarChar(format_float_value(f, &fmt)))
        }
        _ => Ok(Value::NVarChar(val.to_string_value())),
    }
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
            .to_integer_i64()
            .unwrap_or(10) as usize
    } else {
        10
    };
    let decimals = if args.len() == 3 {
        eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(0) as usize
    } else {
        0
    };

    let f = val.to_f64().unwrap_or(0.0);
    let formatted = format!("{:.*}", decimals, f);
    let trimmed = formatted.trim();
    if trimmed.len() >= length {
        Ok(Value::VarChar(trimmed.to_string()))
    } else {
        Ok(Value::VarChar(format!(
            "{:>width$}",
            trimmed,
            width = length
        )))
    }
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
    let repl = replacement.to_string_value();

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
                }
            }
        }
        result.push(ch);
    }

    Ok(Value::NVarChar(result))
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
    let type_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if val.is_null() {
        return Ok(Value::Null);
    }

    let s = val.to_string_value();
    let escape_type = type_val.to_string_value().to_uppercase();

    if escape_type == "JSON" {
        let mut result = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '"' => result.push_str("\\\""),
                '\\' => result.push_str("\\\\"),
                '\u{0008}' => result.push_str("\\b"),
                '\u{000C}' => result.push_str("\\f"),
                '\n' => result.push_str("\\n"),
                '\r' => result.push_str("\\r"),
                '\t' => result.push_str("\\t"),
                _ if c.is_control() => {
                    result.push_str(&format!("\\u{:04x}", c as u32));
                }
                _ => result.push(c),
            }
        }
        Ok(Value::NVarChar(result))
    } else {
        Err(DbError::Execution(format!(
            "STRING_ESCAPE: unsupported escape type '{}'",
            escape_type
        )))
    }
}


fn format_integer<T: std::fmt::Display>(v: T, fmt: &str) -> String {
    match fmt {
        "N" | "n" => format_number_with_commas(&format!("{}", v)),
        "C" | "c" => format!("${}", v),
        "P" | "p" => format!("{}%", v),
        _ => format!("{}", v),
    }
}

fn format_float_value(f: f64, fmt: &str) -> String {
    if fmt.starts_with('N') || fmt.starts_with('n') {
        let decimals: usize = if fmt.len() > 1 { fmt[1..].parse().unwrap_or(2) } else { 2 };
        format_number_with_commas(&format!("{:.*}", decimals, f))
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
    let (negative, abs) = if s.starts_with('-') { (true, &s[1..]) } else { (false, s) };
    let parts: Vec<&str> = abs.splitn(2, '.').collect();
    let int_part = parts[0];
    let frac_part = parts.get(1).copied();

    let mut result = String::new();
    let chars: Vec<char> = int_part.chars().collect();
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 { result.push(','); }
        result.push(*c);
    }
    if let Some(frac) = frac_part {
        result.push('.');
        result.push_str(frac);
    }
    if negative { format!("-{}", result) } else { result }
}
