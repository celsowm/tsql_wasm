use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::{find_if_blocks, find_top_level_begin};

pub(crate) fn parse_if(sql: &str) -> Result<Statement, DbError> {
    let after_if = sql["IF".len()..].trim();

    let (begin_idx, else_idx) = find_if_blocks(after_if);

    let (condition_str, body_str, else_str) = if let Some(bi) = begin_idx {
        let cond = after_if[..bi].trim();
        let else_pos = else_idx.filter(|&ei| ei > bi);
        let body = if let Some(ei) = else_pos {
            &after_if[bi..ei]
        } else {
            &after_if[bi..]
        };
        let else_body = else_pos.map(|ei| &after_if[ei + "ELSE".len()..]);
        (cond, body, else_body)
    } else if let Some(ei) = else_idx {
        let cond = after_if[..ei].trim();
        let body_start = cond.len();
        (
            cond,
            &after_if[body_start..ei],
            Some(&after_if[ei + "ELSE".len()..]),
        )
    } else {
        // IF without BEGIN: single statement body
        // The entire after_if is "condition statement [ELSE ...]"
        // We need to find where the condition ends and the statement starts.
        // Use a heuristic: find the first top-level DML/DDL keyword that begins a statement.
        let cond_end = find_condition_end(after_if);
        let cond = after_if[..cond_end].trim();
        let rest = after_if[cond_end..].trim();
        // Check for ELSE in the remainder
        let else_pos = crate::parser::utils::find_keyword_top_level(rest, "ELSE");
        let body = if let Some(ei) = else_pos {
            &rest[..ei]
        } else {
            rest
        };
        let else_body = else_pos.map(|ei| &rest[ei + "ELSE".len()..]);
        (cond, body, else_body)
    };

    let (processed_cond, cond_subquery_map) = crate::parser::statements::subquery_utils::extract_subqueries(condition_str);
    let mut condition = crate::parser::expression::parse_expr_with_subqueries(&processed_cond, &cond_subquery_map)?;
    crate::parser::statements::subquery_utils::apply_subquery_map(&mut condition, &cond_subquery_map);
    let then_body = if body_str.trim().to_uppercase().starts_with("BEGIN") {
        super::parse_begin_end_body_with_end(body_str, super::find_body_end)?
    } else {
        crate::parser::parse_batch(body_str)?
    };

    let else_body = else_str
        .map(|s| {
            let s = s.trim();
            if s.to_uppercase().starts_with("BEGIN") {
                super::parse_begin_end_body(s)
            } else {
                crate::parser::parse_batch(s)
            }
        })
        .transpose()?;

    Ok(Statement::If(IfStmt {
        condition,
        then_body,
        else_body,
    }))
}

fn find_condition_end(input: &str) -> usize {
    let upper = input.to_uppercase();
    let bytes = upper.as_bytes();
    let mut paren_depth = 0usize;
    let mut in_string = false;
    let mut i = 0usize;

    let stmt_keywords: &[&[u8]] = &[
        b"SELECT", b"INSERT", b"UPDATE", b"DELETE", b"SET", b"DECLARE",
        b"EXEC", b"EXECUTE", b"PRINT", b"RETURN", b"BREAK", b"CONTINUE",
        b"RAISERROR", b"THROW", b"BEGIN", b"IF", b"WHILE",
    ];

    while i < bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            '\'' => { in_string = !in_string; i += 1; continue; }
            '(' if !in_string => { paren_depth += 1; i += 1; continue; }
            ')' if !in_string => { paren_depth = paren_depth.saturating_sub(1); i += 1; continue; }
            _ => {}
        }
        if in_string || paren_depth > 0 {
            i += 1;
            continue;
        }
        for kw in stmt_keywords {
            let kw_len = kw.len();
            if i + kw_len <= bytes.len() && &bytes[i..i + kw_len] == *kw {
                let prev_ok = i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
                let next_ok = i + kw_len >= bytes.len() || !(bytes[i + kw_len].is_ascii_alphanumeric() || bytes[i + kw_len] == b'_');
                if prev_ok && next_ok {
                    return i;
                }
            }
        }
        i += 1;
    }
    input.len()
}

pub(crate) fn parse_while(sql: &str) -> Result<Statement, DbError> {
    let after_while = sql["WHILE".len()..].trim();
    let begin_idx = find_top_level_begin(after_while)
        .ok_or_else(|| DbError::Parse("WHILE requires BEGIN...END body".into()))?;
    let condition_str = after_while[..begin_idx].trim();
    let body_str = &after_while[begin_idx..];

    let (processed_cond, cond_subquery_map) = crate::parser::statements::subquery_utils::extract_subqueries(condition_str);
    let mut condition = crate::parser::expression::parse_expr_with_subqueries(&processed_cond, &cond_subquery_map)?;
    crate::parser::statements::subquery_utils::apply_subquery_map(&mut condition, &cond_subquery_map);
    let body = super::parse_begin_end_body(body_str)?;

    Ok(Statement::While(WhileStmt { condition, body }))
}

pub(crate) fn parse_begin_end(sql: &str) -> Result<Statement, DbError> {
    let body = super::parse_begin_end_body(sql)?;
    Ok(Statement::BeginEnd(body))
}

pub(crate) fn parse_raiserror(sql: &str) -> Result<Statement, DbError> {
    let after = sql["RAISERROR".len()..].trim();
    if !after.starts_with('(') || !after.ends_with(')') {
        return Err(DbError::Parse("RAISERROR expects (message, severity, state)".into()));
    }
    let inner = &after[1..after.len() - 1];
    let parts = crate::parser::utils::split_csv_top_level(inner);
    if parts.len() < 3 {
        return Err(DbError::Parse("RAISERROR expects at least 3 arguments".into()));
    }

    let message = crate::parser::expression::parse_expr(parts[0].trim())?;
    let severity = crate::parser::expression::parse_expr(parts[1].trim())?;
    let state = crate::parser::expression::parse_expr(parts[2].trim())?;

    Ok(Statement::Raiserror(RaiserrorStmt {
        message,
        severity,
        state,
    }))
}

pub(crate) fn parse_try_catch(sql: &str) -> Result<Statement, DbError> {
    let upper = sql.to_uppercase();
    if !upper.starts_with("BEGIN TRY") {
        return Err(DbError::Parse("expected BEGIN TRY".into()));
    }

    let after_try = &sql["BEGIN TRY".len()..].trim();
    let (try_end_idx, is_end_try) = super::find_matching_end_try_catch(after_try)?;
    let try_body_str = after_try[..try_end_idx].trim();
    let try_body = crate::parser::parse_batch(try_body_str)?;

    let skip_len = if is_end_try {
        if after_try[try_end_idx..].to_uppercase().starts_with("END TRY") {
            7
        } else {
            3
        }
    } else {
        10
    };
    let rest = after_try[try_end_idx + skip_len..].trim();
    let upper_rest = rest.to_uppercase();
    if !upper_rest.starts_with("BEGIN CATCH") {
        return Err(DbError::Parse("expected BEGIN CATCH after END TRY".into()));
    }

    let after_catch = &rest["BEGIN CATCH".len()..].trim();
    let (catch_end_idx, _) = super::find_matching_end_try_catch(after_catch)?;
    let catch_body_str = after_catch[..catch_end_idx].trim();
    let catch_body = crate::parser::parse_batch(catch_body_str)?;

    Ok(Statement::TryCatch(TryCatchStmt {
        try_body,
        catch_body,
    }))
}
