use regex::Regex;

use crate::error::DbError;
use crate::types::Value;

pub fn regexp_like(s: &str, pattern: &str, flags: Option<&str>) -> Result<Value, DbError> {
    let re = compile_regex(pattern, flags)?;
    Ok(Value::Bit(re.is_match(s)))
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
    let matches: Vec<(usize, usize)> = re
        .find_iter(substr)
        .map(|m| (m.start(), m.end()))
        .collect();

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
