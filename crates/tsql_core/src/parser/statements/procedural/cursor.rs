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
    let name_start = if upper.starts_with("NEXT FROM ") {
        "NEXT FROM ".len()
    } else if upper.starts_with("FROM ") {
        "FROM ".len()
    } else {
        0
    };
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
    Ok(Statement::FetchCursor(FetchCursorStmt { name, into }))
}

pub(crate) fn parse_close_cursor(sql: &str) -> Result<Statement, DbError> {
    let name = sql["CLOSE".len()..].trim().to_string();
    Ok(Statement::CloseCursor(name))
}

pub(crate) fn parse_deallocate_cursor(sql: &str) -> Result<Statement, DbError> {
    let name = sql["DEALLOCATE".len()..].trim().to_string();
    Ok(Statement::DeallocateCursor(name))
}
