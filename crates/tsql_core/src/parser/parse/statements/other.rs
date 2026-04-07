use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};

fn is_statement_starter(tok: Option<&Token>) -> bool {
    match tok {
        Some(Token::Keyword(k)) => matches!(
            k,
            Keyword::Select
                | Keyword::Insert
                | Keyword::Update
                | Keyword::Delete
                | Keyword::Create
                | Keyword::Drop
                | Keyword::Alter
                | Keyword::Declare
                | Keyword::Set
                | Keyword::If
                | Keyword::While
                | Keyword::Exec
                | Keyword::Execute
                | Keyword::Print
                | Keyword::Begin
                | Keyword::Commit
                | Keyword::Rollback
                | Keyword::Save
                | Keyword::Return
                | Keyword::Break
                | Keyword::Continue
                | Keyword::Merge
                | Keyword::Truncate
                | Keyword::Open
                | Keyword::Close
                | Keyword::Deallocate
                | Keyword::Fetch
                | Keyword::With
        ),
        _ => false,
    }
}

// Re-export canonical implementations from control_flow.rs
pub use super::control_flow::{parse_if, parse_begin_end, parse_try_catch};

pub fn parse_declare(parser: &mut Parser) -> ParseResult<Vec<DeclareVar>> {
    let mut vars = Vec::new();
    loop {
        match parser.next() {
            Some(Token::Variable(name)) => {
                let name = name.clone();
                let data_type = crate::parser::parse::expressions::parse_data_type(parser)?;
                let initial_value = if let Some(Token::Operator(op)) = parser.peek() {
                    if *op == "=" {
                        let _ = parser.next();
                        Some(crate::parser::parse::expressions::parse_expr(parser)?)
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
        match parser.peek() {
            Some(Token::Comma) => { let _ = parser.next(); }
            _ => break,
        }
    }
    Ok(vars)
}

pub fn parse_set(parser: &mut Parser) -> ParseResult<Statement> {
    match parser.next() {
        Some(Token::Variable(variable)) => {
            let variable = variable.clone();
            if let Some(Token::Operator(op)) = parser.next() {
                if *op != "=" {
                     return parser.backtrack(Expected::Description("="));
                }
            } else {
                return parser.backtrack(Expected::Description("="));
            }
            let expr = crate::parser::parse::expressions::parse_expr(parser)?;
            Ok(Statement::Procedural(ProceduralStatement::Set { variable, expr }))
        }
        _ => parser.backtrack(Expected::Description("variable")),
    }
}

pub fn parse_exec_dispatch(parser: &mut Parser) -> ParseResult<Statement> {
    match parser.peek() {
        Some(Token::LParen) => {
             let _ = parser.next();
             let sql_expr = crate::parser::parse::expressions::parse_expr(parser)?;
             parser.expect_rparen()?;
             Ok(Statement::Procedural(ProceduralStatement::ExecDynamic { sql_expr }))
        }
        Some(Token::Identifier(_)) | Some(Token::Keyword(_)) | Some(Token::Variable(_)) => {
            let mut return_variable = None;
            if let Some(Token::Variable(v)) = parser.peek() {
                if matches!(parser.peek_at(1), Some(Token::Operator(op)) if *op == "=") {
                    return_variable = Some(v.clone());
                    let _ = parser.next();
                    let _ = parser.next();
                }
            }

            let id_str = match parser.peek() {
                Some(Token::Identifier(id)) => id.clone(),
                Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
                Some(Token::Variable(v)) => v.clone(),
                _ => unreachable!(),
            };
             
             if id_str.eq_ignore_ascii_case("sp_executesql") {
                 let _ = parser.next();
                 let sql_expr = crate::parser::parse::expressions::parse_expr(parser)?;
                 let mut params_def = None;
                 if matches!(parser.peek(), Some(Token::Comma)) {
                     let _ = parser.next();
                     params_def = Some(crate::parser::parse::expressions::parse_expr(parser)?);
                 }
                 let mut args = Vec::new();
                 while matches!(parser.peek(), Some(Token::Comma)) {
                     let _ = parser.next();
                     args.push(parse_exec_arg(parser)?);
                 }
                 return Ok(Statement::Procedural(ProceduralStatement::SpExecuteSql { sql_expr, params_def, args }));
             }

              let name = super::parse_multipart_name(parser)?;
              let mut args = Vec::new();
              if !parser.is_empty()
                  && !matches!(parser.peek(), Some(Token::Semicolon) | Some(Token::Go))
                  && !is_statement_starter(parser.peek())
              {
                  args = crate::parser::parse::expressions::parse_comma_list(parser, parse_exec_arg)?;
              }
              Ok(Statement::Procedural(ProceduralStatement::ExecProcedure { return_variable, name, args }))
        }
        _ => parser.backtrack(Expected::Description("procedure name or expression")),
    }
}

fn parse_exec_arg(parser: &mut Parser) -> ParseResult<ExecArg> {
    let mut name = None;
    if let Some(Token::Variable(v)) = parser.peek() {
        let v = v.clone();
        if matches!(parser.peek_at(1), Some(Token::Operator(op)) if *op == "=") {
            let _ = parser.next();
            let _ = parser.next();
            name = Some(v);
        }
    }
    let expr = crate::parser::parse::expressions::parse_expr(parser)?;
    let mut is_output = false;
    if let Some(Token::Keyword(k)) = parser.peek() {
        if matches!(k, Keyword::Output | Keyword::Out) {
            let _ = parser.next();
            is_output = true;
        }
    }
    Ok(ExecArg { name, expr, is_output })
}
