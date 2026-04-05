use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};

pub fn parse_open_cursor(parser: &mut Parser) -> ParseResult<Statement> {
    if let Some(Token::Identifier(id)) = parser.next() {
        Ok(Statement::Cursor(CursorStatement::Open(id.clone())))
    } else {
        parser.backtrack(Expected::Description("cursor name"))
    }
}

pub fn parse_close_cursor(parser: &mut Parser) -> ParseResult<Statement> {
    if let Some(Token::Identifier(id)) = parser.next() {
        Ok(Statement::Cursor(CursorStatement::Close(id.clone())))
    } else {
        parser.backtrack(Expected::Description("cursor name"))
    }
}

pub fn parse_deallocate_cursor(parser: &mut Parser) -> ParseResult<Statement> {
    if let Some(Token::Identifier(id)) = parser.next() {
        Ok(Statement::Cursor(CursorStatement::Deallocate(id.clone())))
    } else {
        parser.backtrack(Expected::Description("cursor name"))
    }
}

pub fn parse_fetch_cursor(parser: &mut Parser) -> ParseResult<Statement> {
    let mut direction = FetchDirection::Next;
    if let Some(Token::Keyword(k)) = parser.peek() {
        match *k {
            Keyword::Next => { let _ = parser.next(); direction = FetchDirection::Next; }
            Keyword::Prior => { let _ = parser.next(); direction = FetchDirection::Prior; }
            Keyword::First => { let _ = parser.next(); direction = FetchDirection::First; }
            Keyword::Last => { let _ = parser.next(); direction = FetchDirection::Last; }
            Keyword::Absolute => {
                let _ = parser.next();
                direction = FetchDirection::Absolute(crate::parser::parse::expressions::parse_expr(parser)?);
            }
            Keyword::Relative => {
                let _ = parser.next();
                direction = FetchDirection::Relative(crate::parser::parse::expressions::parse_expr(parser)?);
            }
            _ => {}
        }
    }
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::From))) {
        let _ = parser.next();
    }
    let name = if let Some(Token::Identifier(id)) = parser.next() {
        id.clone()
    } else {
        return parser.backtrack(Expected::Description("cursor name"));
    };
    let mut into_vars = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Into))) {
        let _ = parser.next();
        into_vars = Some(crate::parser::parse::expressions::parse_comma_list(parser, |p| {
            if let Some(Token::Variable(v)) = p.next() {
                Ok(v.clone())
            } else {
                p.backtrack(Expected::Description("variable"))
            }
        })?);
    }
    Ok(Statement::Cursor(CursorStatement::Fetch { name, direction, into_vars }))
}
