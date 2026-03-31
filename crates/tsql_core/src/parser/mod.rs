mod expression;
pub(crate) mod statements;
mod tokenizer;
pub(crate) mod utils;

use crate::ast::{SetOpKind, SetOpStmt, Statement};
use crate::error::DbError;

pub use expression::parse_expr;
pub use expression::parse_expr_with_quoted_ident;

pub fn parse_expr_subquery_aware(input: &str) -> Result<crate::ast::Expr, DbError> {
    parse_expr_subquery_aware_with_quoted_ident(input, true)
}

pub fn parse_expr_subquery_aware_with_quoted_ident(
    input: &str,
    quoted_identifier: bool,
) -> Result<crate::ast::Expr, DbError> {
    let (processed, subquery_map) = statements::extract_subqueries(input);
    let mut expr = expression::parse_expr_with_subqueries_and_quoted_ident(
        &processed,
        &subquery_map,
        quoted_identifier,
    )?;
    statements::apply_subquery_map(&mut expr, &subquery_map);
    Ok(expr)
}

pub fn parse_batch(sql: &str) -> Result<Vec<Statement>, DbError> {
    parse_batch_with_quoted_ident(sql, true)
}

pub fn parse_batch_with_quoted_ident(
    sql: &str,
    quoted_identifier: bool,
) -> Result<Vec<Statement>, DbError> {
    let processed_sql = preprocess_sql_for_quoted_ident(sql, quoted_identifier);
    let stripped = strip_comments(&processed_sql);
    let trimmed = stripped.trim();
    if trimmed.is_empty() {
        return Ok(vec![]);
    }

    let parts = split_statements(trimmed);
    let mut statements = Vec::new();
    for part in &parts {
        let s = part.trim();
        if !s.is_empty() {
            statements.push(parse_sql_with_quoted_ident(s, quoted_identifier)?);
        }
    }
    Ok(statements)
}

fn strip_comments(sql: &str) -> String {
    let mut out = String::new();
    let mut in_string = false;
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '\'' {
            in_string = !in_string;
            out.push(ch);
            i += 1;
            continue;
        }
        if !in_string && i + 1 < chars.len() && chars[i] == '-' && chars[i + 1] == '-' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        out.push(ch);
        i += 1;
    }
    out
}

fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut paren_depth = 0usize;
    let mut block_depth = 0usize;
    let mut in_try_catch = false;
    let mut in_string = false;
    let upper = sql.to_uppercase();
    let chars: Vec<char> = sql.chars().collect();
    let upper_chars: Vec<char> = upper.chars().collect();
    let mut i = 0usize;

    while i < chars.len() {
        let ch = chars[i];

        if ch == '\'' {
            in_string = !in_string;
            buf.push(ch);
            i += 1;
            continue;
        }

        if !in_string {
            if i + 5 <= upper_chars.len() && upper_chars[i..i + 5] == ['B', 'E', 'G', 'I', 'N'] {
                let prev_ok = i == 0 || !chars[i - 1].is_ascii_alphanumeric();
                let next_ok = i + 5 >= chars.len() || !chars[i + 5].is_ascii_alphanumeric();
                if prev_ok && next_ok && !is_begin_transaction(&upper_chars, chars.len(), i + 5) {
                    // Check if it's BEGIN TRY
                    let is_try = i + 9 <= upper_chars.len()
                        && upper_chars[i..i + 9] == ['B', 'E', 'G', 'I', 'N', ' ', 'T', 'R', 'Y'];
                    if is_try && block_depth == 0 && paren_depth == 0 {
                        // Let parse_try_catch handle it as a single unit
                        in_try_catch = true;
                    } else {
                        block_depth += 1;
                        buf.extend(chars[i..i + 5].iter());
                        i += 5;
                        continue;
                    }
                }
            }

            if i + 3 <= upper_chars.len() && upper_chars[i..i + 3] == ['E', 'N', 'D'] {
                let prev_ok = i == 0 || !chars[i - 1].is_ascii_alphanumeric();
                let next_ok = i + 3 >= chars.len() || !chars[i + 3].is_ascii_alphanumeric();
                if prev_ok && next_ok {
                    // Check if it's END CATCH
                    let is_end_catch = i + 9 <= upper_chars.len()
                        && upper_chars[i..i + 9] == ['E', 'N', 'D', ' ', 'C', 'A', 'T', 'C', 'H'];
                    if is_end_catch && block_depth == 0 && paren_depth == 0 {
                        // End of a TRY...CATCH block.
                        buf.extend(chars[i..i + 9].iter());
                        i += 9;
                        out.push(buf.trim().to_string());
                        buf.clear();
                        in_try_catch = false;
                        continue;
                    }

                    // Check if it's END TRY - skip over it without splitting
                    let is_end_try = i + 7 <= upper_chars.len()
                        && upper_chars[i..i + 7] == ['E', 'N', 'D', ' ', 'T', 'R', 'Y'];
                    if is_end_try && block_depth == 0 && paren_depth == 0 {
                        // Skip END TRY, continue scanning for BEGIN CATCH
                        buf.extend(chars[i..i + 7].iter());
                        i += 7;
                        continue;
                    }

                    if block_depth > 0 {
                        block_depth -= 1;
                    }
                    buf.extend(chars[i..i + 3].iter());
                    i += 3;

                    if paren_depth == 0 && block_depth == 0 {
                        let mut k = i;
                        while k < chars.len() && chars[k].is_ascii_whitespace() {
                            k += 1;
                        }
                        if k < chars.len() && is_statement_keyword_start(&upper_chars, &chars, k) {
                            out.push(buf.trim().to_string());
                            buf.clear();
                            i = k;
                        }
                    }
                    continue;
                }
            }

            if ch == '(' {
                paren_depth += 1;
            } else if ch == ')' {
                paren_depth = paren_depth.saturating_sub(1);
            } else if ch == ';' && paren_depth == 0 && block_depth == 0 && !in_try_catch {
                out.push(buf.trim().to_string());
                buf.clear();
                i += 1;
                continue;
            }
        }

        buf.push(ch);
        i += 1;
    }

    if !buf.trim().is_empty() {
        out.push(buf.trim().to_string());
    }
    out
}

fn is_statement_keyword_start(upper_chars: &[char], chars: &[char], start: usize) -> bool {
    let stmt_keywords = [
        "INSERT",
        "SELECT",
        "UPDATE",
        "DELETE",
        "SET",
        "DECLARE",
        "IF",
        "WHILE",
        "RETURN",
        "BREAK",
        "CONTINUE",
        "EXEC",
        "EXECUTE",
        "CREATE",
        "DROP",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVE",
        "PRINT",
        "OPEN",
        "FETCH",
        "CLOSE",
        "DEALLOCATE",
    ];

    for kw in &stmt_keywords {
        if start + kw.len() <= upper_chars.len() {
            let candidate: String = upper_chars[start..start + kw.len()].iter().collect();
            if candidate == *kw {
                let boundary_ok = start + kw.len() == chars.len()
                    || !chars[start + kw.len()].is_ascii_alphanumeric();
                if boundary_ok {
                    return true;
                }
            }
        }
    }

    false
}

pub fn parse_sql(sql: &str) -> Result<Statement, DbError> {
    parse_sql_with_quoted_ident(sql, true)
}

fn preprocess_sql_for_quoted_ident(sql: &str, quoted_identifier: bool) -> String {
    if quoted_identifier {
        return sql.to_string();
    }

    let mut result = String::new();
    let mut in_string = false;
    let mut in_bracket = false;
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];

        if ch == '\'' && !in_bracket {
            in_string = !in_string;
            result.push(ch);
            i += 1;
            continue;
        }

        if ch == '[' && !in_string {
            in_bracket = true;
            result.push(ch);
            i += 1;
            continue;
        }

        if ch == ']' && in_bracket {
            in_bracket = false;
            result.push(ch);
            i += 1;
            continue;
        }

        if ch == '"' && !in_string && !in_bracket {
            let start = i + 1;
            let mut end = start;
            while end < chars.len() && chars[end] != '"' {
                end += 1;
            }
            if end < chars.len() {
                let quoted_text: String = chars[start..end].iter().collect();
                result.push('\'');
                result.push_str(&quoted_text);
                result.push('\'');
                i = end + 1;
                continue;
            }
        }

        result.push(ch);
        i += 1;
    }

    result
}

pub fn parse_sql_with_quoted_ident(
    sql: &str,
    quoted_identifier: bool,
) -> Result<Statement, DbError> {
    let processed_sql = preprocess_sql_for_quoted_ident(sql, quoted_identifier);
    let trimmed = processed_sql.trim().trim_end_matches(';').trim();

    let upper = trimmed.to_uppercase();

    // Parse WITH CTE before set operations so the CTE body is kept together
    if upper.starts_with("WITH ") {
        return statements::parse_with_cte(trimmed);
    }

    if let Some((left_sql, op_kind, right_sql)) = find_set_operation(trimmed) {
        let left = parse_sql(left_sql)?;
        let right = parse_sql(right_sql)?;
        return Ok(Statement::SetOp(SetOpStmt {
            left: Box::new(left),
            op: op_kind,
            right: Box::new(right),
        }));
    }

    if upper.starts_with("BEGIN DISTRIBUTED TRANSACTION")
        || upper.starts_with("BEGIN DISTRIBUTED TRAN")
    {
        // Treat as regular BEGIN TRANSACTION for our purposes
        let stripped = if upper.starts_with("BEGIN DISTRIBUTED TRANSACTION") {
            format!(
                "BEGIN TRANSACTION{}",
                &trimmed["BEGIN DISTRIBUTED TRANSACTION".len()..]
            )
        } else {
            format!("BEGIN TRAN{}", &trimmed["BEGIN DISTRIBUTED TRAN".len()..])
        };
        return statements::parse_begin_transaction(&stripped);
    }
    if upper.starts_with("BEGIN TRANSACTION")
        || upper.starts_with("BEGIN TRAN ")
        || upper == "BEGIN TRAN"
    {
        return statements::parse_begin_transaction(trimmed);
    }
    if upper == "COMMIT"
        || upper.starts_with("COMMIT TRAN")
        || upper.starts_with("COMMIT TRANSACTION")
    {
        return statements::parse_commit_transaction(trimmed);
    }
    if upper == "ROLLBACK"
        || upper.starts_with("ROLLBACK TRAN")
        || upper.starts_with("ROLLBACK TRANSACTION")
    {
        return statements::parse_rollback_transaction(trimmed);
    }
    if upper.starts_with("SAVE TRAN") || upper.starts_with("SAVE TRANSACTION") {
        return statements::parse_save_transaction(trimmed);
    }
    if upper.starts_with("SET TRANSACTION ISOLATION LEVEL") {
        return statements::parse_set_transaction_isolation(trimmed);
    }

    if upper.starts_with("MERGE ") || upper.starts_with("MERGE INTO ") {
        return statements::parse_merge(trimmed);
    }

    if upper.starts_with("DECLARE ") {
        return statements::parse_declare(trimmed);
    }
    if upper.starts_with("SET ") {
        return statements::parse_set(trimmed);
    }
    if upper.starts_with("IF ") {
        return statements::parse_if(trimmed);
    }
    if upper.starts_with("BEGIN TRY") {
        return statements::parse_try_catch(trimmed);
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
        return Ok(Statement::Return(None));
    }
    if upper.starts_with("RETURN ") {
        let after = &trimmed[6..].trim();
        if after.is_empty() {
            return Ok(Statement::Return(None));
        }
        let expr = crate::parser::expression::parse_expr(after)?;
        return Ok(Statement::Return(Some(expr)));
    }
    if upper.starts_with("EXEC ") || upper.starts_with("EXECUTE ") {
        return statements::parse_exec(trimmed);
    }
    if upper.starts_with("CREATE PROCEDURE ") {
        return statements::parse_create_procedure(trimmed);
    }
    if upper.starts_with("DROP PROCEDURE ") {
        return statements::parse_drop_procedure(trimmed);
    }
    if upper.starts_with("CREATE FUNCTION ") {
        return statements::parse_create_function(trimmed);
    }
    if upper.starts_with("DROP FUNCTION ") {
        return statements::parse_drop_function(trimmed);
    }

    if upper.starts_with("PRINT ") {
        statements::parse_print(trimmed)
    } else if upper.starts_with("RAISERROR") {
        statements::parse_raiserror(trimmed)
    } else if upper.starts_with("OPEN ") {
        statements::parse_open_cursor(trimmed)
    } else if upper.starts_with("FETCH ") {
        statements::parse_fetch_cursor(trimmed)
    } else if upper.starts_with("CLOSE ") {
        statements::parse_close_cursor(trimmed)
    } else if upper.starts_with("DEALLOCATE ") {
        statements::parse_deallocate_cursor(trimmed)
    } else if upper.starts_with("CREATE TRIGGER ") {
        statements::parse_create_trigger(trimmed)
    } else if upper.starts_with("DROP TRIGGER ") {
        statements::parse_drop_trigger(trimmed)
    } else if upper.starts_with("CREATE TABLE ") {
        statements::parse_create_table(trimmed)
    } else if upper.starts_with("CREATE VIEW ") {
        statements::parse_create_view(trimmed)
    } else if upper.starts_with("CREATE TYPE ") {
        statements::parse_create_type(trimmed)
    } else if upper.starts_with("CREATE INDEX ") {
        statements::parse_create_index(trimmed)
    } else if upper.starts_with("CREATE SCHEMA ") {
        statements::parse_create_schema(trimmed)
    } else if upper.starts_with("DROP TABLE ") {
        statements::parse_drop_table(trimmed)
    } else if upper.starts_with("DROP VIEW ") {
        statements::parse_drop_view(trimmed)
    } else if upper.starts_with("DROP TYPE ") {
        statements::parse_drop_type(trimmed)
    } else if upper.starts_with("DROP INDEX ") {
        statements::parse_drop_index(trimmed)
    } else if upper.starts_with("DROP SCHEMA ") {
        statements::parse_drop_schema(trimmed)
    } else if upper.starts_with("INSERT INTO ") {
        statements::parse_insert(trimmed)
    } else if upper.starts_with("SELECT ") {
        statements::parse_select(trimmed)
    } else if upper.starts_with("UPDATE ") {
        statements::parse_update(trimmed)
    } else if upper.starts_with("DELETE ") {
        statements::parse_delete(trimmed)
    } else if upper.starts_with("TRUNCATE TABLE ") {
        statements::parse_truncate_table(trimmed)
    } else if upper.starts_with("ALTER TABLE ") {
        statements::parse_alter_table(trimmed)
    } else {
        Err(DbError::Parse("unsupported statement".into()))
    }
}

fn is_begin_transaction(upper_chars: &[char], total_len: usize, mut idx: usize) -> bool {
    while idx < total_len && upper_chars[idx].is_ascii_whitespace() {
        idx += 1;
    }
    if idx + 4 <= total_len {
        let w: String = upper_chars[idx..idx + 4].iter().collect();
        if w == "TRAN" {
            return true;
        }
    }
    if idx + 11 <= total_len {
        let w: String = upper_chars[idx..idx + 11].iter().collect();
        if w == "TRANSACTION" {
            return true;
        }
    }
    false
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
                    let prev_is_ident = i > 0
                        && ((bytes[i - 1] as char).is_ascii_alphanumeric() || bytes[i - 1] == b'_');
                    let next_is_ident = i + kw_len < bytes.len()
                        && ((bytes[i + kw_len] as char).is_ascii_alphanumeric()
                            || bytes[i + kw_len] == b'_');
                    if !prev_is_ident && !next_is_ident {
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
