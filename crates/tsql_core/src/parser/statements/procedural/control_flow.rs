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
        return Err(DbError::Parse(
            "IF requires BEGIN...END blocks (use: IF condition BEGIN ... END)".into(),
        ));
    };

    let condition = crate::parser::expression::parse_expr(condition_str)?;
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

pub(crate) fn parse_while(sql: &str) -> Result<Statement, DbError> {
    let after_while = sql["WHILE".len()..].trim();
    let begin_idx = find_top_level_begin(after_while)
        .ok_or_else(|| DbError::Parse("WHILE requires BEGIN...END body".into()))?;
    let condition_str = after_while[..begin_idx].trim();
    let body_str = &after_while[begin_idx..];

    let condition = crate::parser::expression::parse_expr(condition_str)?;
    let body = super::parse_begin_end_body(body_str)?;

    Ok(Statement::While(WhileStmt { condition, body }))
}

pub(crate) fn parse_begin_end(sql: &str) -> Result<Statement, DbError> {
    let body = super::parse_begin_end_body(sql)?;
    Ok(Statement::BeginEnd(body))
}
