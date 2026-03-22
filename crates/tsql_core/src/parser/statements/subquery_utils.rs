use std::collections::HashMap;

use crate::ast::{Expr, SelectStmt, Statement};

pub(crate) fn extract_subqueries(input: &str) -> (String, HashMap<String, SelectStmt>) {
    let mut map = HashMap::new();
    let mut counter = 0usize;
    let result = input.to_string();
    let upper = result.to_uppercase();
    let chars: Vec<char> = result.chars().collect();
    let upper_chars: Vec<char> = upper.chars().collect();

    let mut i = 0;
    while i < chars.len() {
        if i + 6 <= chars.len() && upper_chars[i..i + 6] == ['E', 'X', 'I', 'S', 'T', 'S'] {
            let prev_ok = i == 0 || !chars[i - 1].is_ascii_alphanumeric();
            let next_ok = i + 6 >= chars.len() || !chars[i + 6].is_ascii_alphanumeric();
            if prev_ok && next_ok {
                let after_exists = result[i + 6..].trim_start();
                if after_exists.starts_with('(') {
                    let start_in_result = result.len() - after_exists.len();
                    if let Some((sql, _end)) = extract_paren_content_from(&chars, start_in_result) {
                        let upper_sql = sql.to_uppercase().trim().to_string();
                        if upper_sql.starts_with("SELECT") {
                            let placeholder = format!("__SUBQ_{}__", counter);
                            counter += 1;
                            if let Ok(Statement::Select(sel)) =
                                crate::parser::statements::select::parse_select(&sql)
                            {
                                map.insert(placeholder.clone(), sel);
                                let before: String = chars[..i].iter().collect();
                                let after_exists_str: String =
                                    chars[start_in_result..].iter().collect();
                                if let Some(paren_end) = find_matching_paren(&after_exists_str) {
                                    let new_expr = format!(
                                        "{}EXISTS {}{}",
                                        before,
                                        placeholder,
                                        &after_exists_str[paren_end + 1..]
                                    );
                                    return finalize_subquery_extraction(&new_expr, map, counter);
                                }
                            }
                        }
                    }
                }
            }
        }
        i += 1;
    }

    finalize_subquery_extraction(&result, map, counter)
}

fn find_matching_paren(input: &str) -> Option<usize> {
    let chars: Vec<char> = input.chars().collect();
    if chars.is_empty() || chars[0] != '(' {
        return None;
    }
    let mut depth = 1;
    let mut in_string = false;
    for i in 1..chars.len() {
        match chars[i] {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn finalize_subquery_extraction(
    input: &str,
    mut map: HashMap<String, SelectStmt>,
    mut counter: usize,
) -> (String, HashMap<String, SelectStmt>) {
    let mut result = input.to_string();

    loop {
        let chars: Vec<char> = result.chars().collect();
        let mut replaced = false;
        let mut i = 0;

        while i < chars.len() {
            if chars[i] == '(' {
                if let Some((sql, end)) = extract_paren_content_from(&chars, i) {
                    let upper_sql = sql.to_uppercase().trim().to_string();
                    if upper_sql.starts_with("SELECT") {
                        let placeholder = format!("__SUBQ_{}__", counter);
                        counter += 1;
                        if let Ok(Statement::Select(sel)) =
                            crate::parser::statements::select::parse_select(&sql)
                        {
                            map.insert(placeholder.clone(), sel);
                            let before: String = chars[..i].iter().collect();
                            let after: String = chars[end..].iter().collect();
                            result = format!("{}({}){}", before, placeholder, after);
                            replaced = true;
                            break;
                        }
                    }
                }
            }
            i += 1;
        }

        if !replaced {
            break;
        }
    }

    (result, map)
}

fn extract_paren_content_from(chars: &[char], start: usize) -> Option<(String, usize)> {
    if start >= chars.len() || chars[start] != '(' {
        return None;
    }
    let mut depth = 1usize;
    let mut in_string = false;
    let mut i = start + 1;

    while i < chars.len() {
        match chars[i] {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    let inner: String = chars[start + 1..i].iter().collect();
                    return Some((inner, i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

pub(crate) fn apply_subquery_map(expr: &mut Expr, _map: &HashMap<String, SelectStmt>) {
    match expr {
        Expr::InList { list, .. } => {
            for item in list.iter_mut() {
                apply_subquery_map(item, _map);
            }
        }
        Expr::Binary { left, right, .. } => {
            apply_subquery_map(left, _map);
            apply_subquery_map(right, _map);
        }
        Expr::Unary { expr: inner, .. } => {
            apply_subquery_map(inner, _map);
        }
        _ => {}
    }
}
