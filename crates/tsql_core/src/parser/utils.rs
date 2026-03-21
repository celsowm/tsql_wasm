use crate::ast::{ObjectName, TableRef};
use crate::error::DbError;

pub(crate) fn parse_object_name(input: &str) -> ObjectName {
    let cleaned = input.trim().trim_matches('[').trim_matches(']');
    let parts = cleaned
        .split('.')
        .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
        .collect::<Vec<_>>();

    if parts.len() == 2 {
        ObjectName {
            schema: Some(parts[0].clone()),
            name: parts[1].clone(),
        }
    } else {
        ObjectName {
            schema: None,
            name: cleaned.to_string(),
        }
    }
}

pub(crate) fn parse_table_ref(input: &str) -> Result<TableRef, DbError> {
    let tokens = tokenize_preserving_parens(input);
    if tokens.is_empty() {
        return Err(DbError::Parse("missing table reference".into()));
    }

    let name = parse_object_name(&tokens[0]);
    let alias = if tokens.len() >= 3 && tokens[1].eq_ignore_ascii_case("AS") {
        Some(tokens[2].trim_matches('[').trim_matches(']').to_string())
    } else if tokens.len() >= 2 {
        Some(tokens[1].trim_matches('[').trim_matches(']').to_string())
    } else {
        None
    };

    Ok(TableRef { name, alias })
}

pub(crate) fn find_keyword_top_level(input: &str, keyword: &str) -> Option<usize> {
    let upper = input.to_uppercase();
    let keyword_upper = keyword.to_uppercase();
    let bytes = upper.as_bytes();
    let kw = keyword_upper.as_bytes();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut i = 0usize;

    while i + kw.len() <= bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            '\'' => {
                in_string = !in_string;
                i += 1;
                continue;
            }
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth = depth.saturating_sub(1),
            _ => {}
        }

        if !in_string && depth == 0 && &bytes[i..i + kw.len()] == kw {
            let prev_ok = i == 0 || !(bytes[i - 1] as char).is_ascii_alphanumeric();
            let next_ok = i + kw.len() == bytes.len()
                || !(bytes[i + kw.len()] as char).is_ascii_alphanumeric();
            if prev_ok && next_ok {
                return Some(i);
            }
        }

        i += 1;
    }

    None
}

pub(crate) fn split_csv_top_level(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut depth = 0usize;
    let mut in_string = false;

    for ch in input.chars() {
        match ch {
            '\'' => {
                in_string = !in_string;
                buf.push(ch);
            }
            '(' if !in_string => {
                depth += 1;
                buf.push(ch);
            }
            ')' if !in_string => {
                depth = depth.saturating_sub(1);
                buf.push(ch);
            }
            ',' if !in_string && depth == 0 => {
                out.push(buf.trim().to_string());
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }

    if !buf.trim().is_empty() {
        out.push(buf.trim().to_string());
    }

    out
}

pub(crate) fn tokenize_preserving_parens(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut depth = 0usize;
    let mut in_string = false;

    for ch in input.chars() {
        match ch {
            '\'' => {
                in_string = !in_string;
                buf.push(ch);
            }
            ' ' | '\t' if depth == 0 && !in_string => {
                if !buf.is_empty() {
                    out.push(buf.clone());
                    buf.clear();
                }
            }
            '(' if !in_string => {
                depth += 1;
                buf.push(ch);
            }
            ')' if !in_string => {
                depth = depth.saturating_sub(1);
                buf.push(ch);
            }
            _ => buf.push(ch),
        }
    }

    if !buf.is_empty() {
        out.push(buf);
    }

    out
}
