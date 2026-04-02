use crate::parser::v2::ast::*;
use crate::parser::v2::parser::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};

pub fn parse_declare_cursor<'a>(input: &mut &'a [Token<'a>], name: Cow<'a, str>) -> ModalResult<Statement<'a>> {
    expect_keyword(input, "CURSOR")?;
    expect_keyword(input, "FOR")?;
    let query = parse_select(input)?;
    Ok(Statement::DeclareCursor { name, query })
}

pub fn parse_open_cursor<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    if let Some(Token::Identifier(id)) = next_token(input) {
        Ok(Statement::OpenCursor(id.clone()))
    } else {
        Err(ErrMode::Backtrack(ContextError::new()))
    }
}

pub fn parse_close_cursor<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    if let Some(Token::Identifier(id)) = next_token(input) {
        Ok(Statement::CloseCursor(id.clone()))
    } else {
        Err(ErrMode::Backtrack(ContextError::new()))
    }
}

pub fn parse_deallocate_cursor<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    if let Some(Token::Identifier(id)) = next_token(input) {
        Ok(Statement::DeallocateCursor(id.clone()))
    } else {
        Err(ErrMode::Backtrack(ContextError::new()))
    }
}

pub fn parse_fetch_cursor<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    let mut direction = FetchDirection::Next;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        match k.to_uppercase().as_str() {
            "NEXT" => { let _ = next_token(input); direction = FetchDirection::Next; }
            "PRIOR" => { let _ = next_token(input); direction = FetchDirection::Prior; }
            "FIRST" => { let _ = next_token(input); direction = FetchDirection::First; }
            "LAST" => { let _ = next_token(input); direction = FetchDirection::Last; }
            "ABSOLUTE" => {
                let _ = next_token(input);
                direction = FetchDirection::Absolute(parse_expr(input)?);
            }
            "RELATIVE" => {
                let _ = next_token(input);
                direction = FetchDirection::Relative(parse_expr(input)?);
            }
            _ => {}
        }
    }
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("FROM")) {
        let _ = next_token(input);
    }
    let name = if let Some(Token::Identifier(id)) = next_token(input) {
        id.clone()
    } else {
        return Err(ErrMode::Backtrack(ContextError::new()));
    };
    let mut into_vars = None;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("INTO")) {
        let _ = next_token(input);
        into_vars = Some(parse_comma_list(input, |i| {
            if let Some(Token::Variable(v)) = next_token(i) {
                Ok(v.clone())
            } else {
                Err(ErrMode::Backtrack(ContextError::new()))
            }
        })?);
    }
    Ok(Statement::FetchCursor { name, direction, into_vars })
}
