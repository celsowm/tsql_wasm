pub(crate) mod control_flow;
pub(crate) mod cursor;
pub(crate) mod execute;
pub(crate) mod print;
pub(crate) mod routine;
pub(crate) mod variable;

pub(crate) use control_flow::*;
pub(crate) use cursor::*;
pub(crate) use execute::*;
pub(crate) use print::*;
pub(crate) use routine::*;
pub(crate) use variable::*;

use crate::ast::{DataTypeSpec, Statement};
use crate::error::DbError;

pub(crate) fn parse_data_type_inline(input: &str) -> Result<DataTypeSpec, DbError> {
    crate::parser::statements::ddl::parse_data_type(input)
}

pub(crate) fn parse_begin_end_body(sql: &str) -> Result<Vec<Statement>, DbError> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("BEGIN") {
        return Err(DbError::Parse("expected BEGIN".into()));
    }
    let rest = trimmed["BEGIN".len()..].trim();
    let end_idx = find_matching_end(rest)?;
    let body_str = rest[..end_idx].trim();

    // Replace pseudo-tables with mapped versions if needed?
    // Actually better to handle this in name resolution.

    crate::parser::parse_batch(body_str)
}

pub(crate) fn parse_begin_end_body_with_end<F>(sql: &str, end_fn: F) -> Result<Vec<Statement>, DbError>
where
    F: Fn(&str) -> Result<usize, DbError>,
{
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("BEGIN") {
        return Err(DbError::Parse("expected BEGIN".into()));
    }
    let rest = trimmed["BEGIN".len()..].trim();
    let end_idx = end_fn(rest)?;
    let body_str = rest[..end_idx].trim();
    crate::parser::parse_batch(body_str)
}

pub(crate) fn find_matching_end(input: &str) -> Result<usize, DbError> {
    let upper = input.to_uppercase();
    let mut depth = 0usize;
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let remaining = chars.len() - i;
        if remaining >= 5
            && &upper[i..i + 5] == "BEGIN"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 5 == chars.len() || !chars[i + 5].is_alphanumeric())
        {
            depth += 1;
            i += 5;
            continue;
        }
        if remaining >= 3
            && &upper[i..i + 3] == "END"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 3 == chars.len() || !chars[i + 3].is_alphanumeric())
        {
            if depth == 0 {
                return Ok(i);
            }
            depth -= 1;
            i += 3;
            continue;
        }
        i += 1;
    }

    Err(DbError::Parse("missing END".into()))
}

pub(crate) fn find_matching_end_try_catch(input: &str) -> Result<(usize, bool), DbError> {
    let upper = input.to_uppercase();
    let mut depth = 0usize;
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let remaining = chars.len() - i;
        if remaining >= 5
            && &upper[i..i + 5] == "BEGIN"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 5 == chars.len() || !chars[i + 5].is_alphanumeric())
        {
            depth += 1;
            i += 5;
            continue;
        }
        if remaining >= 7
            && &upper[i..i + 7] == "END TRY"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 7 == chars.len() || !chars[i + 7].is_alphanumeric())
        {
            if depth == 0 {
                return Ok((i, true));
            }
            depth -= 1;
            i += 7;
            continue;
        }
        if remaining >= 10
            && &upper[i..i + 10] == "END CATCH"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 10 == chars.len() || !chars[i + 10].is_alphanumeric())
        {
            if depth == 0 {
                return Ok((i, false));
            }
            depth -= 1;
            i += 10;
            continue;
        }
        if remaining >= 3
            && &upper[i..i + 3] == "END"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 3 == chars.len() || !chars[i + 3].is_alphanumeric())
        {
            if depth == 0 {
                return Ok((i, true));
            }
            depth -= 1;
            i += 3;
            continue;
        }
        i += 1;
    }

    Err(DbError::Parse("missing END".into()))
}

pub(crate) fn find_body_end(input: &str) -> Result<usize, DbError> {
    let upper = input.to_uppercase();
    let mut depth = 0usize;
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let remaining = chars.len() - i;
        if remaining >= 5
            && &upper[i..i + 5] == "BEGIN"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 5 == chars.len() || !chars[i + 5].is_alphanumeric())
        {
            depth += 1;
            i += 5;
            continue;
        }
        if remaining >= 3
            && &upper[i..i + 3] == "END"
            && (i == 0 || !chars[i - 1].is_alphanumeric())
            && (i + 3 == chars.len() || !chars[i + 3].is_alphanumeric())
        {
            if depth == 0 {
                return Ok(i);
            }
            depth -= 1;
            i += 3;
            continue;
        }
        i += 1;
    }

    Err(DbError::Parse("missing END".into()))
}
