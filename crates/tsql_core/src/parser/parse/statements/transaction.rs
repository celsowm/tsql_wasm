use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};

pub fn parse_begin_transaction<'a>(parser: &mut Parser<'a>) -> ParseResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = parser.peek() {
        if matches!(k, Keyword::Tran | Keyword::Transaction) {
            let _ = parser.next();
        }
    }
    let name = if let Some(Token::Identifier(id)) = parser.peek() {
        let name = id.clone();
        let _ = parser.next();
        Some(name)
    } else {
        None
    };

    if parser.at_keyword(Keyword::With) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Mark)?;
        if parser.at_keyword(Keyword::As) {
            let _ = parser.next();
        }
        if matches!(parser.peek(), Some(Token::String(_))) {
            let _ = parser.next();
        }
    }

    Ok(Statement::Transaction(TransactionStatement::Begin(name)))
}

pub fn parse_commit_transaction<'a>(parser: &mut Parser<'a>) -> ParseResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = parser.peek() {
        if matches!(k, Keyword::Tran | Keyword::Transaction) {
            let _ = parser.next();
        }
    }
    let name = if let Some(Token::Identifier(id)) = parser.peek() {
        let name = id.clone();
        let _ = parser.next();
        Some(name)
    } else {
        None
    };
    Ok(Statement::Transaction(TransactionStatement::Commit(name)))
}

pub fn parse_rollback_transaction<'a>(parser: &mut Parser<'a>) -> ParseResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = parser.peek() {
        if matches!(k, Keyword::Tran | Keyword::Transaction) {
            let _ = parser.next();
        }
    }
    let name = if let Some(Token::Identifier(id)) = parser.peek() {
        let name = id.clone();
        let _ = parser.next();
        Some(name)
    } else {
        None
    };
    Ok(Statement::Transaction(TransactionStatement::Rollback(name)))
}

pub fn parse_save_transaction<'a>(parser: &mut Parser<'a>) -> ParseResult<Statement<'a>> {
    if let Some(Token::Keyword(k)) = parser.peek() {
        if matches!(k, Keyword::Tran | Keyword::Transaction) {
            let _ = parser.next();
        }
    }
    if let Some(Token::Identifier(id)) = parser.next() {
        Ok(Statement::Transaction(TransactionStatement::Save(id.clone())))
    } else {
        parser.backtrack(Expected::Description("identifier"))
    }
}
