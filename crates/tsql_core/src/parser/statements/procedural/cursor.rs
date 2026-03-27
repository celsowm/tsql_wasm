use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::split_csv_top_level;

pub(crate) fn parse_open_cursor(sql: &str) -> Result<Statement, DbError> {
    let name = sql["OPEN".len()..].trim().to_string();
    Ok(Statement::OpenCursor(name))
}

pub(crate) fn parse_fetch_cursor(sql: &str) -> Result<Statement, DbError> {
    let after = sql["FETCH".len()..].trim();
    let upper = after.to_uppercase();

    let mut direction = FetchDirection::Next;
    let mut name_start = 0;

    if upper.starts_with("NEXT FROM ") {
        name_start = "NEXT FROM ".len();
    } else if upper.starts_with("PRIOR FROM ") {
        direction = FetchDirection::Prior;
        name_start = "PRIOR FROM ".len();
    } else if upper.starts_with("FIRST FROM ") {
        direction = FetchDirection::First;
        name_start = "FIRST FROM ".len();
    } else if upper.starts_with("LAST FROM ") {
        direction = FetchDirection::Last;
        name_start = "LAST FROM ".len();
    } else if upper.starts_with("ABSOLUTE ") {
        let after_abs = &after["ABSOLUTE".len()..].trim();
        let from_idx = after_abs.to_uppercase().find(" FROM ")
            .ok_or_else(|| DbError::Parse("FETCH ABSOLUTE missing FROM".into()))?;
        let expr_str = &after_abs[..from_idx].trim();
        direction = FetchDirection::Absolute(crate::parser::expression::parse_expr(expr_str)?);
        name_start = "ABSOLUTE ".len() + from_idx + " FROM ".len();
    } else if upper.starts_with("RELATIVE ") {
        let after_rel = &after["RELATIVE".len()..].trim();
        let from_idx = after_rel.to_uppercase().find(" FROM ")
            .ok_or_else(|| DbError::Parse("FETCH RELATIVE missing FROM".into()))?;
        let expr_str = &after_rel[..from_idx].trim();
        direction = FetchDirection::Relative(crate::parser::expression::parse_expr(expr_str)?);
        name_start = "RELATIVE ".len() + from_idx + " FROM ".len();
    } else if upper.starts_with("FROM ") {
        name_start = "FROM ".len();
    }

    let rest = &after[name_start..].trim();
    let into_pos = rest.to_uppercase().find(" INTO ");
    let (name, into) = if let Some(pos) = into_pos {
        let name = rest[..pos].trim().to_string();
        let vars_raw = rest[pos + " INTO ".len()..].trim();
        let vars = split_csv_top_level(vars_raw)
            .into_iter()
            .map(|v| v.trim().to_string())
            .collect();
        (name, Some(vars))
    } else {
        (rest.to_string(), None)
    };
    Ok(Statement::FetchCursor(FetchCursorStmt { name, direction, into }))
}

pub(crate) fn parse_close_cursor(sql: &str) -> Result<Statement, DbError> {
    let name = sql["CLOSE".len()..].trim().to_string();
    Ok(Statement::CloseCursor(name))
}

pub(crate) fn parse_deallocate_cursor(sql: &str) -> Result<Statement, DbError> {
    let name = sql["DEALLOCATE".len()..].trim().to_string();
    Ok(Statement::DeallocateCursor(name))
}
