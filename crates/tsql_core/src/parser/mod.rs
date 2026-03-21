mod expression;
mod statements;
mod utils;

use crate::ast::{SetOpKind, SetOpStmt, Statement};
use crate::error::DbError;

pub use expression::parse_expr;

pub fn parse_batch(sql: &str) -> Result<Vec<Statement>, DbError> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(vec![]);
    }

    // Split by semicolons at the top level
    let parts = split_statements(trimmed);
    let mut statements = Vec::new();
    for part in &parts {
        let s = part.trim();
        if !s.is_empty() {
            statements.push(parse_sql(s)?);
        }
    }
    Ok(statements)
}

fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut paren_depth = 0usize;
    let mut block_depth = 0usize;
    let mut in_string = false;
    let upper = sql.to_uppercase();
    let chars: Vec<char> = sql.chars().collect();
    let upper_chars: Vec<char> = upper.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];
        match ch {
            '\'' => {
                in_string = !in_string;
                buf.push(ch);
            }
            '(' if !in_string => {
                paren_depth += 1;
                buf.push(ch);
            }
            ')' if !in_string => {
                paren_depth = paren_depth.saturating_sub(1);
                buf.push(ch);
            }
            ';' if !in_string && paren_depth == 0 && block_depth == 0 => {
                out.push(buf.trim().to_string());
                buf.clear();
            }
            _ => {
                if !in_string {
                    // Check for BEGIN keyword
                    if i + 5 <= upper_chars.len()
                        && &upper_chars[i..i + 5] == &['B', 'E', 'G', 'I', 'N']
                    {
                        let prev_ok = i == 0 || !chars[i - 1].is_ascii_alphanumeric();
                        let next_ok = i + 5 >= chars.len() || !chars[i + 5].is_ascii_alphanumeric();
                        if prev_ok && next_ok {
                            block_depth += 1;
                        }
                    }
                    // Check for END keyword
                    if i + 3 <= upper_chars.len() && &upper_chars[i..i + 3] == &['E', 'N', 'D'] {
                        let prev_ok = i == 0 || !chars[i - 1].is_ascii_alphanumeric();
                        let next_ok = i + 3 >= chars.len() || !chars[i + 3].is_ascii_alphanumeric();
                        if prev_ok && next_ok && block_depth > 0 {
                            block_depth -= 1;
                        }
                    }
                }
                buf.push(ch);
            }
        }
        i += 1;
    }

    if !buf.trim().is_empty() {
        out.push(buf.trim().to_string());
    }
    out
}

pub fn parse_sql(sql: &str) -> Result<Statement, DbError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();

    // Check for set operations at the top level
    if let Some((left_sql, op_kind, right_sql)) = find_set_operation(trimmed) {
        let left = parse_sql(left_sql)?;
        let right = parse_sql(right_sql)?;
        return Ok(Statement::SetOp(SetOpStmt {
            left: Box::new(left),
            op: op_kind,
            right: Box::new(right),
        }));
    }

    let upper = trimmed.to_uppercase();

    // Check for WITH CTE
    if upper.starts_with("WITH ") {
        return statements::parse_with_cte(trimmed);
    }

    // Control-of-flow and procedural statements
    if upper.starts_with("DECLARE ") {
        return statements::parse_declare(trimmed);
    }
    if upper.starts_with("SET ") {
        return statements::parse_set(trimmed);
    }
    if upper.starts_with("IF ") {
        return statements::parse_if(trimmed);
    }
    if upper.starts_with("WHILE ") {
        return statements::parse_while(trimmed);
    }
    if upper.starts_with("BEGIN") {
        return statements::parse_begin_end(trimmed);
    }
    if upper == "BREAK" {
        return Ok(Statement::Break);
    }
    if upper == "CONTINUE" {
        return Ok(Statement::Continue);
    }
    if upper == "RETURN" {
        return Ok(Statement::Return);
    }
    if upper.starts_with("EXEC ") || upper.starts_with("EXECUTE ") {
        return statements::parse_exec(trimmed);
    }

    if upper.starts_with("CREATE TABLE ") {
        statements::parse_create_table(trimmed)
    } else if upper.starts_with("CREATE SCHEMA ") {
        statements::parse_create_schema(trimmed)
    } else if upper.starts_with("DROP TABLE ") {
        statements::parse_drop_table(trimmed)
    } else if upper.starts_with("DROP SCHEMA ") {
        statements::parse_drop_schema(trimmed)
    } else if upper.starts_with("INSERT INTO ") {
        statements::parse_insert(trimmed)
    } else if upper.starts_with("SELECT ") {
        statements::parse_select(trimmed)
    } else if upper.starts_with("UPDATE ") {
        statements::parse_update(trimmed)
    } else if upper.starts_with("DELETE FROM ") {
        statements::parse_delete(trimmed)
    } else if upper.starts_with("TRUNCATE TABLE ") {
        statements::parse_truncate_table(trimmed)
    } else if upper.starts_with("ALTER TABLE ") {
        statements::parse_alter_table(trimmed)
    } else {
        Err(DbError::Parse("unsupported statement".into()))
    }
}

fn find_set_operation(sql: &str) -> Option<(&str, SetOpKind, &str)> {
    let upper = sql.to_uppercase();
    let bytes = upper.as_bytes();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut i = 0usize;

    let keywords: &[(&str, SetOpKind)] = &[
        ("UNION ALL", SetOpKind::UnionAll),
        ("UNION", SetOpKind::Union),
        ("INTERSECT", SetOpKind::Intersect),
        ("EXCEPT", SetOpKind::Except),
    ];

    while i < bytes.len() {
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

        if !in_string && depth == 0 {
            for &(kw, kind) in keywords {
                let kw_bytes = kw.as_bytes();
                let kw_len = kw_bytes.len();
                if i + kw_len <= bytes.len() && &bytes[i..i + kw_len] == kw_bytes {
                    let prev_ok = i == 0 || !(bytes[i - 1] as char).is_ascii_alphanumeric();
                    let next_ok = i + kw_len == bytes.len()
                        || !(bytes[i + kw_len] as char).is_ascii_alphanumeric();
                    if prev_ok && next_ok {
                        let left = sql[..i].trim();
                        let right = sql[i + kw_len..].trim();
                        return Some((left, kind, right));
                    }
                }
            }
        }
        i += 1;
    }

    None
}
