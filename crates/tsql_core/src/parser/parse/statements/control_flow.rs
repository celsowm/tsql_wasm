use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};

pub fn parse_if(parser: &mut Parser) -> ParseResult<Statement> {
    let condition = crate::parser::parse::expressions::parse_expr(parser)?;
    let then_stmt = crate::parser::parse::parse_statement(parser)?;
    let else_stmt = if let Some(Token::Keyword(Keyword::Else)) = parser.peek() {
        let _ = parser.next();
        Some(crate::parser::parse::parse_statement(parser)?)
    } else {
        None
    };

    Ok(Statement::Procedural(ProceduralStatement::If {
        condition,
        then_stmt: Box::new(then_stmt),
        else_stmt: else_stmt.map(Box::new),
    }))
}

pub fn parse_begin_end(parser: &mut Parser) -> ParseResult<Statement> {
    let mut statements = Vec::new();
    loop {
        while matches!(parser.peek(), Some(Token::Semicolon)) {
            let _ = parser.next();
        }
        if let Some(Token::Keyword(Keyword::End)) = parser.peek() {
            let _ = parser.next();
            break;
        }
        if parser.is_empty() {
             return parser.backtrack(Expected::Description("statement"));
        }
        statements.push(crate::parser::parse::parse_statement(parser)?);
    }
    Ok(Statement::Procedural(ProceduralStatement::BeginEnd(statements)))
}

pub fn parse_try_catch(parser: &mut Parser) -> ParseResult<Statement> {
    let mut try_body = Vec::new();
    loop {
        while matches!(parser.peek(), Some(Token::Semicolon)) {
            let _ = parser.next();
        }
        if matches!(parser.peek(), Some(Token::Keyword(Keyword::End))) {
            if matches!(parser.peek_at(1), Some(Token::Keyword(Keyword::Try))) {
                let _ = parser.next();
                let _ = parser.next();
                break;
            }
        }
        try_body.push(crate::parser::parse::parse_statement(parser)?);
    }

    parser.expect_keyword(Keyword::Begin)?;
    parser.expect_keyword(Keyword::Catch)?;
    let mut catch_body = Vec::new();
    loop {
        while matches!(parser.peek(), Some(Token::Semicolon)) {
            let _ = parser.next();
        }
        if matches!(parser.peek(), Some(Token::Keyword(Keyword::End))) {
            if matches!(parser.peek_at(1), Some(Token::Keyword(Keyword::Catch))) {
                let _ = parser.next();
                let _ = parser.next();
                break;
            }
        }
        catch_body.push(crate::parser::parse::parse_statement(parser)?);
    }

    Ok(Statement::Procedural(ProceduralStatement::TryCatch { try_body, catch_body }))
}
