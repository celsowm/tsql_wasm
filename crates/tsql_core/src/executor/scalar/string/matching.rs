use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::super::clock::Clock;
use super::super::super::context::ExecutionContext;
use super::super::super::evaluator::eval_expr;
use super::super::super::model::ContextTable;

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
    } else if target == pat { 1 } else { 0 };

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

    let snd1 = soundex(&s1.to_string_value());
    let snd2 = soundex(&s2.to_string_value());

    let mut diff = 0;
    let chars1: Vec<char> = snd1.chars().collect();
    let chars2: Vec<char> = snd2.chars().collect();

    for i in 0..4 {
        if chars1[i] == chars2[i] {
            diff += 1;
        }
    }
    Ok(Value::Int(diff))
}

fn soundex(s: &str) -> String {
    let chars: Vec<char> = s
        .to_uppercase()
        .chars()
        .filter(|c| c.is_ascii_alphabetic())
        .collect();
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
            if result.len() == 4 {
                break;
            }
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
