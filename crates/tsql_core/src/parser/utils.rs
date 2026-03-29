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
    let cleaned = input.trim().trim_matches('[').trim_matches(']').trim_matches('"');
    let parts = cleaned
        .split('.')
        .map(|s| s.trim().trim_matches('[').trim_matches(']').trim_matches('"').to_string())
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
    let trimmed = input.trim();
    
    let (name, rest) = if trimmed.starts_with('(') {
        let close = find_matching_paren_index(trimmed, 0)
            .ok_or_else(|| DbError::Parse("missing closing ')' for subquery in FROM".into()))?;
        let subquery_sql = &trimmed[1..close];
        let subquery_stmt = crate::parser::parse_sql(subquery_sql)?;
        let subquery = match subquery_stmt {
            crate::ast::Statement::Select(s) => s,
            _ => return Err(DbError::Parse("subquery in FROM must be a SELECT".into())),
        };
        (crate::ast::TableName::Subquery(Box::new(subquery)), &trimmed[close+1..])
    } else {
        let tokens = tokenize_preserving_parens(trimmed);
        if tokens.is_empty() {
            return Err(DbError::Parse("missing table reference".into()));
        }
        let obj_name = parse_object_name(&tokens[0]);
        let mut i = tokens[0].len();
        while i < trimmed.len() && trimmed.as_bytes()[i].is_ascii_whitespace() {
            i += 1;
        }
        (crate::ast::TableName::Object(obj_name), &trimmed[i..])
    };

    let mut alias = None;
    let mut pivot = None;
    let mut unpivot = None;
    let mut hints = Vec::new();

    let mut current = rest.trim();
    while !current.is_empty() {
        let upper = current.to_uppercase();
        if upper.starts_with("PIVOT") && (upper.len() == 5 || upper.as_bytes()[5].is_ascii_whitespace() || upper.as_bytes()[5] == b'(') {
            let pivot_content = &current["PIVOT".len()..].trim();
            if !pivot_content.starts_with('(') {
                return Err(DbError::Parse("PIVOT must be followed by '('".into()));
            }
            let close = find_matching_paren_index(pivot_content, 0)
                .ok_or_else(|| DbError::Parse("PIVOT missing closing ')'".into()))?;
            let inner = &pivot_content[1..close].trim();
            pivot = Some(Box::new(parse_pivot_spec(inner)?));
            current = &pivot_content[close+1..].trim();
        } else if upper.starts_with("UNPIVOT") && (upper.len() == 7 || upper.as_bytes()[7].is_ascii_whitespace() || upper.as_bytes()[7] == b'(') {
            let unpivot_content = &current["UNPIVOT".len()..].trim();
            if !unpivot_content.starts_with('(') {
                return Err(DbError::Parse("UNPIVOT must be followed by '('".into()));
            }
            let close = find_matching_paren_index(unpivot_content, 0)
                .ok_or_else(|| DbError::Parse("UNPIVOT missing closing ')'".into()))?;
            let inner = &unpivot_content[1..close].trim();
            unpivot = Some(Box::new(parse_unpivot_spec(inner)?));
            current = &unpivot_content[close+1..].trim();
        } else if upper.starts_with("WITH") && (upper.len() == 4 || upper.as_bytes()[4].is_ascii_whitespace() || upper.as_bytes()[4] == b'(') {
            let after_with = current["WITH".len()..].trim();
            if !after_with.starts_with('(') {
                // Could be WITH CTE, break out
                break;
            }
            let close = find_matching_paren_index(after_with, 0)
                .ok_or_else(|| DbError::Parse("table hints missing closing ')'".into()))?;
            let inner = &after_with[1..close];
            hints = split_csv_top_level(inner);
            current = &after_with[close+1..].trim();
        } else if upper.starts_with("AS ") {
            let after_as = current[3..].trim();
            let end = after_as.find(|c: char| c.is_whitespace()).unwrap_or(after_as.len());
            alias = Some(after_as[..end].trim_matches('[').trim_matches(']').to_string());
            current = &after_as[end..].trim();
        } else {
            // Check if it's a join keyword or something else that should stop us
            if upper.starts_with("JOIN") || upper.starts_with("INNER") || upper.starts_with("LEFT") || 
               upper.starts_with("RIGHT") || upper.starts_with("FULL") || upper.starts_with("CROSS") ||
               upper.starts_with("ON") || upper.starts_with("WHERE") || upper.starts_with("GROUP") ||
               upper.starts_with("HAVING") || upper.starts_with("ORDER") {
                break;
            }
            // Otherwise, it might be a plain alias
            let end = current.find(|c: char| c.is_whitespace()).unwrap_or(current.len());
            alias = Some(current[..end].trim_matches('[').trim_matches(']').to_string());
            current = &current[end..].trim();
        }
    }

    Ok(TableRef { name, alias, pivot, unpivot, hints })
}

fn parse_pivot_spec(input: &str) -> Result<crate::ast::PivotSpec, DbError> {
    let upper = input.to_uppercase();
    let for_idx = find_keyword_top_level(&upper, "FOR")
        .ok_or_else(|| DbError::Parse("PIVOT missing FOR".into()))?;
    
    let agg_part = input[..for_idx].trim();
    let open_paren = agg_part.find('(').ok_or_else(|| DbError::Parse("PIVOT aggregate function must have '('".into()))?;
    let close_paren = agg_part.rfind(')').ok_or_else(|| DbError::Parse("PIVOT aggregate function must have ')'".into()))?;
    let agg_func = agg_part[..open_paren].trim().to_string();
    let agg_col = agg_part[open_paren+1..close_paren].trim().to_string();

    let rest = &input[for_idx + "FOR".len()..].trim();
    let in_idx = find_keyword_top_level(&rest.to_uppercase(), "IN")
        .ok_or_else(|| DbError::Parse("PIVOT missing IN".into()))?;
    
    let pivot_col = rest[..in_idx].trim().to_string();
    let values_part = rest[in_idx + "IN".len()..].trim();
    
    if !values_part.starts_with('(') || !values_part.ends_with(')') {
        return Err(DbError::Parse("PIVOT values must be in (...)".into()));
    }
    
    let values_inner = &values_part[1..values_part.len()-1];
    let pivot_values = split_csv_top_level(values_inner)
        .into_iter()
        .map(|v| v.trim().trim_matches('[').trim_matches(']').to_string())
        .collect();

    Ok(crate::ast::PivotSpec {
        aggregate_func: agg_func,
        aggregate_col: agg_col,
        pivot_col,
        pivot_values,
    })
}

fn parse_unpivot_spec(input: &str) -> Result<crate::ast::UnpivotSpec, DbError> {
    let upper = input.to_uppercase();
    let for_idx = find_keyword_top_level(&upper, "FOR")
        .ok_or_else(|| DbError::Parse("UNPIVOT missing FOR".into()))?;
    
    let value_col = input[..for_idx].trim().to_string();
    let rest = &input[for_idx + "FOR".len()..].trim();
    
    let in_idx = find_keyword_top_level(&rest.to_uppercase(), "IN")
        .ok_or_else(|| DbError::Parse("UNPIVOT missing IN".into()))?;
    
    let pivot_col = rest[..in_idx].trim().to_string();
    let cols_part = rest[in_idx + "IN".len()..].trim();
    
    if !cols_part.starts_with('(') || !cols_part.ends_with(')') {
        return Err(DbError::Parse("UNPIVOT columns must be in (...)".into()));
    }
    
    let cols_inner = &cols_part[1..cols_part.len()-1];
    let column_list = split_csv_top_level(cols_inner)
        .into_iter()
        .map(|v| v.trim().trim_matches('[').trim_matches(']').to_string())
        .collect();

    Ok(crate::ast::UnpivotSpec {
        value_col,
        pivot_col,
        column_list,
    })
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
