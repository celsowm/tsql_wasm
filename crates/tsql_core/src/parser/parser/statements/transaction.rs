use crate::parser::ast::*;
use crate::parser::parser::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};

pub fn parse_begin_transaction<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("TRAN") || k.eq_ignore_ascii_case("TRANSACTION") {
            let _ = next_token(input);
        }
    }
    let name = if let Some(Token::Identifier(id)) = peek_token(input) {
        let name = id.clone();
        let _ = next_token(input);
        Some(name)
    } else {
        None
    };
    Ok(Statement::BeginTransaction(name))
}

pub fn parse_commit_transaction<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("TRAN") || k.eq_ignore_ascii_case("TRANSACTION") {
            let _ = next_token(input);
        }
    }
    let name = if let Some(Token::Identifier(id)) = peek_token(input) {
        let name = id.clone();
        let _ = next_token(input);
        Some(name)
    } else {
        None
    };
    Ok(Statement::CommitTransaction(name))
}

pub fn parse_rollback_transaction<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("TRAN") || k.eq_ignore_ascii_case("TRANSACTION") {
            let _ = next_token(input);
        }
    }
    let name = if let Some(Token::Identifier(id)) = peek_token(input) {
        let name = id.clone();
        let _ = next_token(input);
        Some(name)
    } else {
        None
    };
    Ok(Statement::RollbackTransaction(name))
}

pub fn parse_save_transaction<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("TRAN") || k.eq_ignore_ascii_case("TRANSACTION") {
            let _ = next_token(input);
        }
    }
    if let Some(Token::Identifier(id)) = next_token(input) {
        Ok(Statement::SaveTransaction(id.clone()))
    } else {
        Err(ErrMode::Backtrack(ContextError::new()))
    }
}
