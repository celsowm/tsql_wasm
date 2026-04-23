use crate::parser::ast::*;
use crate::parser::error::{Expected, ParseResult};
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

pub fn parse_begin_transaction(parser: &mut Parser) -> ParseResult<Statement> {
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

pub fn parse_commit_transaction(parser: &mut Parser) -> ParseResult<Statement> {
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

pub fn parse_rollback_transaction(parser: &mut Parser) -> ParseResult<Statement> {
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

pub fn parse_save_transaction(parser: &mut Parser) -> ParseResult<Statement> {
    if let Some(Token::Keyword(k)) = parser.peek() {
        if matches!(k, Keyword::Tran | Keyword::Transaction) {
            let _ = parser.next();
        }
    }
    match parser.next() {
        Some(Token::Identifier(id)) => Ok(Statement::Transaction(TransactionStatement::Save(
            id.clone(),
        ))),
        Some(Token::Keyword(k)) => Ok(Statement::Transaction(TransactionStatement::Save(
            k.to_string().to_lowercase(),
        ))),
        _ => parser.backtrack(Expected::Description("identifier")),
    }
}
