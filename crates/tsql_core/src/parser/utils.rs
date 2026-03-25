use crate::ast::{ObjectName, TableRef};
use crate::error::DbError;

fn is_alphanumeric(b: u8) -> bool {
    (b >= b'A' && b <= b'Z')
        || (b >= b'a' && b <= b'z')
        || (b >= b'0' && b <= b'9')
        || b == b'_'
        || b == b'['
        || b == b']'
}

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

pub(crate) fn find_top_level_begin(input: &str) -> Option<usize> {
    let upper = input.to_uppercase();
    let bytes = upper.as_bytes();
    let mut block_depth = 0usize;
    let mut in_string = false;
    let mut paren_depth = 0usize;
    let mut past_body = false;
    let mut i = 0usize;

    while i < bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            '\'' => {
                in_string = !in_string;
                i += 1;
                continue;
            }
            '(' if !in_string => {
                paren_depth += 1;
                i += 1;
                continue;
            }
            ')' if !in_string => {
                paren_depth = paren_depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ if !in_string && paren_depth == 0 => {}
            _ => {
                i += 1;
                continue;
            }
        }

        if !in_string && paren_depth == 0 {
            let remaining = bytes.len() - i;
            if remaining >= 5 && &bytes[i..i + 5] == b"BEGIN" {
                let prev_ok = i == 0 || !is_alphanumeric(bytes[i - 1]);
                let next_ok = i + 5 >= bytes.len() || !is_alphanumeric(bytes[i + 5]);
                if prev_ok && next_ok && block_depth == 0 && !past_body {
                    return Some(i);
                }
                if !past_body {
                    block_depth += 1;
                }
                i += 5;
                continue;
            }
            if remaining >= 3 && &bytes[i..i + 3] == b"END" {
                let prev_ok = i == 0 || !is_alphanumeric(bytes[i - 1]);
                let next_ok = i + 3 >= bytes.len() || !is_alphanumeric(bytes[i + 3]);
                if prev_ok && next_ok && block_depth > 0 {
                    block_depth -= 1;
                    if block_depth == 0 {
                        past_body = true;
                    }
                }
                i += 3;
                continue;
            }
        }
        i += 1;
    }
    None
}

pub(crate) fn find_if_blocks(input: &str) -> (Option<usize>, Option<usize>) {
    let upper = input.to_uppercase();
    let bytes = upper.as_bytes();
    let mut block_depth = 0usize;
    let mut nested_depth = 0usize;
    let mut in_string = false;
    let mut paren_depth = 0usize;
    let mut begin_idx: Option<usize> = None;
    let mut else_idx: Option<usize> = None;
    let mut past_body = false;
    let mut i = 0usize;

    while i < bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            '\'' => {
                in_string = !in_string;
                i += 1;
                continue;
            }
            '(' if !in_string => {
                paren_depth += 1;
                i += 1;
                continue;
            }
            ')' if !in_string => {
                paren_depth = paren_depth.saturating_sub(1);
                i += 1;
                continue;
            }
            _ if !in_string && paren_depth == 0 => {}
            _ => {
                i += 1;
                continue;
            }
        }

        if !in_string && paren_depth == 0 {
            let remaining = bytes.len() - i;
            if remaining >= 5 && &bytes[i..i + 5] == b"BEGIN" {
                let prev_ok = i == 0 || !is_alphanumeric(bytes[i - 1]);
                let next_ok = i + 5 >= bytes.len() || !is_alphanumeric(bytes[i + 5]);
                if prev_ok && next_ok {
                    if block_depth == 0 && !past_body {
                        begin_idx = Some(i);
                    }
                    if !past_body {
                        block_depth += 1;
                        if block_depth > 1 {
                            nested_depth += 1;
                        }
                    }
                    i += 5;
                    continue;
                }
            }
            if remaining >= 3 && &bytes[i..i + 3] == b"END" {
                let prev_ok = i == 0 || !is_alphanumeric(bytes[i - 1]);
                let next_ok = i + 3 >= bytes.len() || !is_alphanumeric(bytes[i + 3]);
                if prev_ok && next_ok {
                    if block_depth > 0 {
                        block_depth -= 1;
                        if block_depth > 0 {
                            nested_depth = nested_depth.saturating_sub(1);
                        }
                        if block_depth == 0 {
                            past_body = true;
                        }
                    }
                    i += 3;
                    continue;
                }
            }
            if block_depth == 1
                && nested_depth == 0
                && remaining >= 4
                && &bytes[i..i + 4] == b"ELSE"
            {
                let prev_ok = i == 0 || !is_alphanumeric(bytes[i - 1]);
                let next_ok = i + 4 >= bytes.len() || !is_alphanumeric(bytes[i + 4]);
                if prev_ok && next_ok {
                    else_idx = Some(i);
                    i += 4;
                    continue;
                }
            }
        }
        i += 1;
    }
    (begin_idx, else_idx)
}

pub(crate) fn find_keyword_top_level(input: &str, keyword: &str) -> Option<usize> {
    let upper = input.to_uppercase();
    let keyword_upper = keyword.to_uppercase();
    let bytes = upper.as_bytes();
    let kw = keyword_upper.as_bytes();
    let mut depth = 0usize;
    let mut block_depth = 0usize;
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
            'B' | 'E' if !in_string && depth == 0 => {
                let remaining = bytes.len() - i;
                if remaining >= 5 && &bytes[i..i + 5] == b"BEGIN" {
                    let prev_ok = i == 0 || !is_alphanumeric(bytes[i - 1]);
                    let next_ok = i + 5 >= bytes.len() || !is_alphanumeric(bytes[i + 5]);
                    if prev_ok && next_ok {
                        block_depth += 1;
                        i += 5;
                        continue;
                    }
                }
                if remaining >= 3 && &bytes[i..i + 3] == b"END" {
                    let prev_ok = i == 0 || !is_alphanumeric(bytes[i - 1]);
                    let next_ok = i + 3 >= bytes.len() || !is_alphanumeric(bytes[i + 3]);
                    if prev_ok && next_ok && block_depth > 0 {
                        block_depth -= 1;
                        i += 3;
                        continue;
                    }
                }
            }
            _ => {}
        }

        if !in_string && depth == 0 && block_depth == 0 && &bytes[i..i + kw.len()] == kw {
            let prev_ok = i == 0 || !is_alphanumeric(bytes[i - 1]);
            let next_ok = i + kw.len() == bytes.len() || !is_alphanumeric(bytes[i + kw.len()]);
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

pub(crate) fn find_matching_paren_index(input: &str, open_idx: usize) -> Option<usize> {
    let chars: Vec<char> = input.chars().collect();
    let mut depth = 0usize;
    let mut in_string = false;
    for (i, ch) in chars.iter().enumerate().skip(open_idx) {
        match *ch {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
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
