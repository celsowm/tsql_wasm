use crate::parser::v2::ast::*;
use crate::parser::v2::parser::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};
use std::borrow::Cow;

pub fn parse_declare<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Vec<DeclareVar<'a>>> {
    let mut vars = Vec::new();
    loop {
        match next_token(input) {
            Some(Token::Variable(name)) => {
                let name = name.clone();
                let data_type = parse_data_type(input)?;
                let initial_value = if let Some(Token::Operator(op)) = peek_token(input) {
                    if op.as_ref() == "=" {
                        let _ = next_token(input);
                        Some(parse_expr(input)?)
                    } else {
                        None
                    }
                } else {
                    None
                };
                vars.push(DeclareVar { name, data_type, initial_value });
            }
            _ => break,
        }
        match peek_token(input) {
            Some(Token::Comma) => { let _ = next_token(input); }
            _ => break,
        }
    }
    Ok(vars)
}

pub fn parse_set<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    match next_token(input) {
        Some(Token::Variable(variable)) => {
            let variable = variable.clone();
            if let Some(Token::Operator(op)) = next_token(input) {
                if op.as_ref() != "=" {
                     return Err(ErrMode::Backtrack(ContextError::new()));
                }
            } else {
                return Err(ErrMode::Backtrack(ContextError::new()));
            }
            let expr = parse_expr(input)?;
            Ok(Statement::Set { variable, expr })
        }
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}

pub fn parse_if<'a, S>(input: &mut &'a [Token<'a>], mut parse_statement_fn: S) -> ModalResult<Statement<'a>> 
where S: FnMut(&mut &'a [Token<'a>]) -> ModalResult<Statement<'a>>
{
    let condition = parse_expr(input)?;
    let then_stmt = parse_statement_fn(input)?;
    let else_stmt = if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("ELSE") {
            let _ = next_token(input);
            Some(parse_statement_fn(input)?)
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

pub fn parse_begin_end<'a, S>(input: &mut &'a [Token<'a>], mut parse_statement_fn: S) -> ModalResult<Statement<'a>>
where S: FnMut(&mut &'a [Token<'a>]) -> ModalResult<Statement<'a>>
{
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
        statements.push(parse_statement_fn(input)?);
    }
    Ok(Statement::BeginEnd(statements))
}

pub fn parse_exec_dispatch<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    match peek_token(input) {
        Some(Token::LParen) => {
             let _ = next_token(input);
             let sql_expr = parse_expr(input)?;
             expect_punctuation(input, Token::RParen)?;
             Ok(Statement::ExecDynamic { sql_expr })
        }
        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) | Some(Token::Variable(id)) => {
             let id_str = match peek_token(input).unwrap() {
                 Token::Identifier(id) | Token::Keyword(id) | Token::Variable(id) => id.clone(),
                 _ => unreachable!(),
             };
             
             // Check for sp_executesql
             if id_str.eq_ignore_ascii_case("sp_executesql") {
                 let _ = next_token(input);
                 let sql_expr = parse_expr(input)?;
                 let mut params_def = None;
                 if matches!(peek_token(input), Some(Token::Comma)) {
                     let _ = next_token(input);
                     params_def = Some(parse_expr(input)?);
                 }
                 let mut args = Vec::new();
                 while matches!(peek_token(input), Some(Token::Comma)) {
                     let _ = next_token(input);
                     args.push(parse_exec_arg(input)?);
                 }
                 return Ok(Statement::SpExecuteSql { sql_expr, params_def, args });
             }

             let name = multipart_name(input)?;
             let mut args = Vec::new();
             if !input.is_empty() && !matches!(peek_token(input), Some(Token::Semicolon) | Some(Token::Go)) {
                 args = parse_comma_list(input, parse_exec_arg)?;
             }
             Ok(Statement::ExecProcedure { name, args })
        }
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}

fn parse_exec_arg<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<ExecArg<'a>> {
    let mut name = None;
    if let Some(Token::Variable(v)) = peek_token(input) {
        let v = v.clone();
        let mut temp = *input;
        let _ = next_token(&mut temp);
        if let Some(Token::Operator(op)) = peek_token(&temp) {
            if op.as_ref() == "=" {
                let _ = next_token(input); // variable
                let _ = next_token(input); // =
                name = Some(v);
            }
        }
    }
    let expr = parse_expr(input)?;
    let mut is_output = false;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("OUTPUT") || k.eq_ignore_ascii_case("OUT") {
            let _ = next_token(input);
            is_output = true;
        }
    }
    Ok(ExecArg { name, expr, is_output })
}

pub fn parse_try_catch<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    // BEGIN and TRY were already consumed
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

pub fn parse_exec<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    parse_exec_dispatch(input)
}

pub fn parse_expr<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    crate::parser::v2::parser::expressions::parse_expr(input)
}

pub fn parse_data_type<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<DataType<'a>> {
    crate::parser::v2::parser::expressions::parse_data_type(input)
}

pub fn expect_punctuation<'a>(input: &mut &'a [Token<'a>], expected: Token<'a>) -> ModalResult<()> {
    crate::parser::v2::parser::expressions::expect_punctuation(input, expected)
}

pub fn expect_keyword<'a>(input: &mut &'a [Token<'a>], expected: &str) -> ModalResult<()> {
    crate::parser::v2::parser::expressions::expect_keyword(input, expected)
}

pub fn parse_comma_list<'a, P, R>(input: &mut &'a [Token<'a>], parser: P) -> ModalResult<Vec<R>>
where P: FnMut(&mut &'a [Token<'a>]) -> ModalResult<R>
{
    crate::parser::v2::parser::expressions::parse_comma_list(input, parser)
}

pub fn parse_statement<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    crate::parser::v2::parser::parse_statement(input)
}

pub fn multipart_name<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Vec<Cow<'a, str>>> {
    crate::parser::v2::parser::statements::query::parse_multipart_name(input)
}
