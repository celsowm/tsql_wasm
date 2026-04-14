use crate::parser::ast::*;
use crate::parser::error::{Expected, ParseResult};
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

pub fn parse_create_table(parser: &mut Parser) -> ParseResult<CreateStmt> {
    let name = super::parse_multipart_name(parser)?;
    parser.expect_lparen()?;
    let (columns, constraints) = super::ddl::parse_table_body(parser)?;
    parser.expect_rparen()?;
    Ok(CreateStmt::Table {
        name,
        columns,
        constraints,
    })
}

pub fn parse_create_view(parser: &mut Parser) -> ParseResult<CreateStmt> {
    let name = super::parse_multipart_name(parser)?;
    parser.expect_keyword(Keyword::As)?;
    let query = crate::parser::parse::statements::query::parse_select(parser)?;
    Ok(CreateStmt::View { name, query })
}

pub fn parse_create_procedure(parser: &mut Parser) -> ParseResult<CreateStmt> {
    let name = super::parse_multipart_name(parser)?;
    let mut params = Vec::new();
    if matches!(parser.peek(), Some(Token::Variable(_))) {
        params = crate::parser::parse::parse_comma_list(
            parser,
            crate::parser::parse::parse_routine_param,
        )?;
    }
    parser.expect_keyword(Keyword::As)?;
    let body = if parser.at_keyword(Keyword::Begin) {
        let _ = parser.next();
        match super::other::parse_begin_end(parser)? {
            Statement::Procedural(ProceduralStatement::BeginEnd(stmts)) => stmts,
            _ => unreachable!(),
        }
    } else {
        vec![crate::parser::parse::parse_statement(parser)?]
    };
    Ok(CreateStmt::Procedure { name, params, body })
}

pub fn parse_create_function(parser: &mut Parser) -> ParseResult<CreateStmt> {
    let name = super::parse_multipart_name(parser)?;
    let mut params = Vec::new();
    if matches!(parser.peek(), Some(Token::LParen)) {
        let _ = parser.next();
        if !matches!(parser.peek(), Some(Token::RParen)) {
            params = crate::parser::parse::parse_comma_list(
                parser,
                crate::parser::parse::parse_routine_param,
            )?;
        }
        parser.expect_rparen()?;
    }

    parser.expect_keyword(Keyword::Returns)?;
    let mut returns = None;
    let mut is_table_return = false;
    if parser.at_keyword(Keyword::Table) {
        let _ = parser.next();
        is_table_return = true;
    } else {
        returns = Some(crate::parser::parse::expressions::parse_data_type(parser)?);
    }

    parser.expect_keyword(Keyword::As)?;

    let body = if parser.at_keyword(Keyword::Begin) {
        let _ = parser.next();
        match super::other::parse_begin_end(parser)? {
            Statement::Procedural(ProceduralStatement::BeginEnd(stmts)) => {
                FunctionBody::Block(stmts)
            }
            _ => unreachable!(),
        }
    } else if is_table_return {
        parser.expect_keyword(Keyword::Return)?;
        parser.expect_lparen()?;
        let query = crate::parser::parse::statements::query::parse_select(parser)?;
        parser.expect_rparen()?;
        FunctionBody::Table(query)
    } else if parser.at_keyword(Keyword::Return) {
        let _ = parser.next();
        let expr = crate::parser::parse::expressions::parse_expr(parser)?;
        FunctionBody::ScalarReturn(expr)
    } else {
        parser.expect_keyword(Keyword::Table)?;
        parser.expect_keyword(Keyword::Return)?;
        parser.expect_lparen()?;
        let query = crate::parser::parse::statements::query::parse_select(parser)?;
        parser.expect_rparen()?;
        FunctionBody::Table(query)
    };
    Ok(CreateStmt::Function {
        name,
        params,
        returns,
        body,
    })
}

pub fn parse_create_trigger(parser: &mut Parser) -> ParseResult<CreateStmt> {
    let name = super::parse_multipart_name(parser)?;
    parser.expect_keyword(Keyword::On)?;
    let table = super::parse_multipart_name(parser)?;

    let mut is_instead_of = false;
    if parser.at_keyword(Keyword::Instead) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Of)?;
        is_instead_of = true;
    } else if matches!(parser.peek(), Some(Token::Keyword(k)) if matches!(k, Keyword::After | Keyword::For))
    {
        let _ = parser.next();
    }

    let events = crate::parser::parse::expressions::parse_comma_list(parser, |p| match p.next() {
        Some(Token::Keyword(k)) => match *k {
            Keyword::Insert => Ok(crate::parser::ast::TriggerEvent::Insert),
            Keyword::Update => Ok(crate::parser::ast::TriggerEvent::Update),
            Keyword::Delete => Ok(crate::parser::ast::TriggerEvent::Delete),
            _ => p.backtrack(Expected::Description("INSERT, UPDATE, or DELETE")),
        },
        _ => p.backtrack(Expected::Description("INSERT, UPDATE, or DELETE")),
    })?;

    parser.expect_keyword(Keyword::As)?;
    let body = if parser.at_keyword(Keyword::Begin) {
        let _ = parser.next();
        match super::other::parse_begin_end(parser)? {
            Statement::Procedural(ProceduralStatement::BeginEnd(stmts)) => stmts,
            _ => unreachable!(),
        }
    } else {
        vec![crate::parser::parse::parse_statement(parser)?]
    };
    Ok(CreateStmt::Trigger {
        name,
        table,
        events,
        is_instead_of,
        body,
    })
}
