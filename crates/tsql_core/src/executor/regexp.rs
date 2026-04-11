use regex::Regex;

use crate::error::DbError;
use crate::types::Value;

pub fn regexp_like(s: &str, pattern: &str, flags: Option<&str>) -> Result<Value, DbError> {
    let re = compile_regex(pattern, flags)?;
    Ok(Value::Bit(re.is_match(s)))
}

pub fn eval_regexp_like(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DbError::Execution(
            "REGEXP_LIKE expects 2 or 3 arguments".into(),
        ));
    }
    let s = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let pattern =
        crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
            .to_string_value();
    let flags = if args.len() == 3 {
        Some(
            crate::executor::evaluator::eval_expr(&args[2], row, ctx, catalog, storage, clock)?
                .to_string_value(),
        )
    } else {
        None
    };
    regexp_like(&s, &pattern, flags.as_deref())
}

pub fn regexp_replace(
    s: &str,
    pattern: &str,
    replacement: &str,
    flags: Option<&str>,
) -> Result<Value, DbError> {
    let re = compile_regex(pattern, flags)?;
    let result = re.replace_all(s, replacement);
    Ok(Value::NVarChar(result.to_string()))
}

pub fn eval_regexp_replace(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() < 3 || args.len() > 4 {
        return Err(DbError::Execution(
            "REGEXP_REPLACE expects 3 or 4 arguments".into(),
        ));
    }
    let s = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let pattern =
        crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
            .to_string_value();
    let replacement =
        crate::executor::evaluator::eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_string_value();
    let flags = if args.len() == 4 {
        Some(
            crate::executor::evaluator::eval_expr(&args[3], row, ctx, catalog, storage, clock)?
                .to_string_value(),
        )
    } else {
        None
    };
    regexp_replace(&s, &pattern, &replacement, flags.as_deref())
}

pub fn regexp_substr(
    s: &str,
    pattern: &str,
    pos: usize,
    occurrence: usize,
    flags: Option<&str>,
) -> Result<Value, DbError> {
    let re = compile_regex(pattern, flags)?;

    let start = if pos > 0 { pos - 1 } else { 0 };
    if start >= s.len() {
        return Ok(Value::Null);
    }

    let substr = &s[start..];
    let matches: Vec<&str> = re.find_iter(substr).map(|m| m.as_str()).collect();

    if occurrence == 0 {
        return if matches.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(Value::NVarChar(matches[0].to_string()))
        };
    }

    if occurrence > matches.len() {
        return Ok(Value::Null);
    }

    Ok(Value::NVarChar(matches[occurrence - 1].to_string()))
}

pub fn eval_regexp_substr(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 5 {
        return Err(DbError::Execution(
            "REGEXP_SUBSTR expects 2 to 5 arguments".into(),
        ));
    }
    let s = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let pattern =
        crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
            .to_string_value();
    let pos = if args.len() >= 3 {
        crate::executor::evaluator::eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(1) as usize
    } else {
        1
    };
    let occurrence = if args.len() >= 4 {
        crate::executor::evaluator::eval_expr(&args[3], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(1) as usize
    } else {
        1
    };
    let flags = if args.len() == 5 {
        Some(
            crate::executor::evaluator::eval_expr(&args[4], row, ctx, catalog, storage, clock)?
                .to_string_value(),
        )
    } else {
        None
    };
    regexp_substr(&s, &pattern, pos, occurrence, flags.as_deref())
}

pub fn regexp_instr(
    s: &str,
    pattern: &str,
    pos: usize,
    occurrence: usize,
    return_opt: usize,
    flags: Option<&str>,
) -> Result<Value, DbError> {
    let re = compile_regex(pattern, flags)?;

    let start = if pos > 0 { pos - 1 } else { 0 };
    if start >= s.len() {
        return Ok(Value::Int(0));
    }

    let substr = &s[start..];
    let matches: Vec<(usize, usize)> = re.find_iter(substr).map(|m| (m.start(), m.end())).collect();

    if occurrence == 0 {
        return if matches.is_empty() {
            Ok(Value::Int(0))
        } else {
            let (match_start, match_end) = matches[0];
            let result = if return_opt == 0 {
                start + match_start + 1
            } else {
                start + match_end + 1
            };
            Ok(Value::Int(result as i32))
        };
    }

    if occurrence > matches.len() {
        return Ok(Value::Int(0));
    }

    let (match_start, match_end) = matches[occurrence - 1];
    let result = if return_opt == 0 {
        start + match_start + 1
    } else {
        start + match_end + 1
    };
    Ok(Value::Int(result as i32))
}

pub fn eval_regexp_instr(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 6 {
        return Err(DbError::Execution(
            "REGEXP_INSTR expects 2 to 6 arguments".into(),
        ));
    }
    let s = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let pattern =
        crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
            .to_string_value();
    let pos = if args.len() >= 3 {
        crate::executor::evaluator::eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(1) as usize
    } else {
        1
    };
    let occurrence = if args.len() >= 4 {
        crate::executor::evaluator::eval_expr(&args[3], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(1) as usize
    } else {
        1
    };
    let return_opt = if args.len() >= 5 {
        crate::executor::evaluator::eval_expr(&args[4], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(0) as usize
    } else {
        0
    };
    let flags = if args.len() == 6 {
        Some(
            crate::executor::evaluator::eval_expr(&args[5], row, ctx, catalog, storage, clock)?
                .to_string_value(),
        )
    } else {
        None
    };
    regexp_instr(&s, &pattern, pos, occurrence, return_opt, flags.as_deref())
}

pub fn regexp_count(
    s: &str,
    pattern: &str,
    pos: usize,
    flags: Option<&str>,
) -> Result<Value, DbError> {
    let re = compile_regex(pattern, flags)?;

    let start = if pos > 0 { pos - 1 } else { 0 };
    if start >= s.len() {
        return Ok(Value::Int(0));
    }

    let substr = &s[start..];
    let count = re.find_iter(substr).count();
    Ok(Value::Int(count as i32))
}

pub fn eval_regexp_count(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(DbError::Execution(
            "REGEXP_COUNT expects 2 to 4 arguments".into(),
        ));
    }
    let s = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let pattern =
        crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
            .to_string_value();
    let pos = if args.len() >= 3 {
        crate::executor::evaluator::eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_integer_i64()
            .unwrap_or(1) as usize
    } else {
        1
    };
    let flags = if args.len() == 4 {
        Some(
            crate::executor::evaluator::eval_expr(&args[3], row, ctx, catalog, storage, clock)?
                .to_string_value(),
        )
    } else {
        None
    };
    regexp_count(&s, &pattern, pos, flags.as_deref())
}

fn compile_regex(pattern: &str, flags: Option<&str>) -> Result<Regex, DbError> {
    let mut full_pattern = String::new();

    if let Some(f) = flags {
        if f.contains('i') {
            full_pattern.push_str("(?i)");
        }
        if f.contains('m') {
            full_pattern.push_str("(?m)");
        }
        if f.contains('s') {
            full_pattern.push_str("(?s)");
        }
        if f.contains('x') {
            full_pattern.push_str("(?x)");
        }
    }

    full_pattern.push_str(pattern);

    Regex::new(&full_pattern)
        .map_err(|e| DbError::Execution(format!("Invalid regex pattern: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regexp_like() {
        assert_eq!(
            regexp_like("Hello World", r"^Hello", None).unwrap(),
            Value::Bit(true)
        );
        assert_eq!(
            regexp_like("Hello World", r"^World", None).unwrap(),
            Value::Bit(false)
        );
    }

    #[test]
    fn test_regexp_like_case_insensitive() {
        assert_eq!(
            regexp_like("Hello World", r"^hello", Some("i")).unwrap(),
            Value::Bit(true)
        );
    }

    #[test]
    fn test_regexp_replace() {
        assert_eq!(
            regexp_replace("Hello World", r"World", "Rust", None).unwrap(),
            Value::NVarChar("Hello Rust".to_string())
        );
    }

    #[test]
    fn test_regexp_substr() {
        assert_eq!(
            regexp_substr("Hello 123 World", r"\d+", 1, 0, None).unwrap(),
            Value::NVarChar("123".to_string())
        );
    }

    #[test]
    fn test_regexp_count() {
        assert_eq!(
            regexp_count("aaa bbb aaa", r"aaa", 1, None).unwrap(),
            Value::Int(2)
        );
    }
}
