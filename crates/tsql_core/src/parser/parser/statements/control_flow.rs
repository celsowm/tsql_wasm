use crate::parser::ast::*;
use crate::parser::parser::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};

pub fn parse_if<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    let condition = parse_expr(input)?;
    let then_stmt = parse_statement(input)?;
    let else_stmt = if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("ELSE") {
            let _ = next_token(input);
            Some(parse_statement(input)?)
        } else {
            None
        }
    } else {
        None
    };

    Ok(Statement::If {
        condition,
        then_stmt: Box::new(then_stmt),
        else_stmt: else_stmt.map(Box::new),
    })
}

pub fn parse_while<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    let condition = parse_expr(input)?;
    let stmt = parse_statement(input)?;
    Ok(Statement::While { condition, stmt: Box::new(stmt) })
}

pub fn parse_begin_end<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    let mut statements = Vec::new();
    loop {
        while matches!(peek_token(input), Some(Token::Semicolon)) {
            let _ = next_token(input);
        }
        if let Some(Token::Keyword(k)) = peek_token(input) {
            if k.eq_ignore_ascii_case("END") {
                let _ = next_token(input);
                break;
            }
        }
        if input.is_empty() {
             return Err(ErrMode::Backtrack(ContextError::new()));
        }
        statements.push(parse_statement(input)?);
    }
    Ok(Statement::BeginEnd(statements))
}

pub fn parse_try_catch<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    // TRY was already consumed
    let mut try_body = Vec::new();
    loop {
        while matches!(peek_token(input), Some(Token::Semicolon)) {
            let _ = next_token(input);
        }
        if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("END")) {
            let mut temp = *input;
            let _ = next_token(&mut temp);
            if matches!(peek_token(&temp), Some(Token::Keyword(k2)) if k2.eq_ignore_ascii_case("TRY")) {
                let _ = next_token(input); // END
                let _ = next_token(input); // TRY
                break;
            }
        }
        try_body.push(parse_statement(input)?);
    }

    expect_keyword(input, "BEGIN")?;
    expect_keyword(input, "CATCH")?;
    let mut catch_body = Vec::new();
    loop {
        while matches!(peek_token(input), Some(Token::Semicolon)) {
            let _ = next_token(input);
        }
        if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("END")) {
            let mut temp = *input;
            let _ = next_token(&mut temp);
            if matches!(peek_token(&temp), Some(Token::Keyword(k2)) if k2.eq_ignore_ascii_case("CATCH")) {
                let _ = next_token(input); // END
                let _ = next_token(input); // CATCH
                break;
            }
        }
        catch_body.push(parse_statement(input)?);
    }

    Ok(Statement::TryCatch { try_body, catch_body })
}

pub fn parse_return<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    let expr = if !input.is_empty() && !matches!(peek_token(input), Some(Token::Semicolon) | Some(Token::Go) | Some(Token::Keyword(_))) {
         // This check for Keyword(_) is a bit heuristic to see if next statement started
         let mut temp = *input;
         if let Ok(_) = parse_expr(&mut temp) {
              Some(parse_expr(input)?)
         } else {
              None
         }
    } else {
         None
    };
    Ok(Statement::Return(expr))
}
