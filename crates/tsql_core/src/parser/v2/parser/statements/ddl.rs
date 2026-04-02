use crate::parser::v2::ast::*;
use crate::parser::v2::parser::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};
use std::borrow::Cow;

pub fn parse_create<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<CreateStmt<'a>> {
    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TABLE")) {
        let _ = next_token(input);
        let name = multipart_name(input)?;
        expect_punctuation(input, Token::LParen)?;
        let (columns, _constraints) = parse_table_body(input)?;
        expect_punctuation(input, Token::RParen)?;
        Ok(CreateStmt::Table { name, columns })
    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("VIEW")) {
        let _ = next_token(input);
        let name = multipart_name(input)?;
        expect_keyword(input, "AS")?;
        let query = parse_select(input)?;
        Ok(CreateStmt::View { name, query })
    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("PROCEDURE") || kw.eq_ignore_ascii_case("PROC")) {
        let _ = next_token(input);
        let name = multipart_name(input)?;
        let mut params = Vec::new();
        if matches!(peek_token(input), Some(Token::Variable(_))) {
            params = parse_comma_list(input, parse_routine_param)?;
        }
        expect_keyword(input, "AS")?;
        let body = if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("BEGIN")) {
             let _ = next_token(input);
             match parse_begin_end(input, parse_statement)? {
                  Statement::BeginEnd(stmts) => stmts,
                  _ => unreachable!(),
             }
        } else {
             vec![parse_statement(input)?]
        };
        Ok(CreateStmt::Procedure { name, params, body })
    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("FUNCTION")) {
        let _ = next_token(input);
        let name = multipart_name(input)?;
        let mut params = Vec::new();
        if matches!(peek_token(input), Some(Token::LParen)) {
             let _ = next_token(input);
             if !matches!(peek_token(input), Some(Token::RParen)) {
                 params = parse_comma_list(input, parse_routine_param)?;
             }
             expect_punctuation(input, Token::RParen)?;
        }
        
        expect_keyword(input, "RETURNS")?;
        let mut returns = None;
        if !matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TABLE")) {
            returns = Some(parse_data_type(input)?);
        }
        
        expect_keyword(input, "AS")?;
        
        let body = if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("BEGIN")) {
             let _ = next_token(input);
             match parse_begin_end(input, parse_statement)? {
                  Statement::BeginEnd(stmts) => FunctionBody::Block(stmts),
                  _ => unreachable!(),
             }
        } else if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("RETURN")) {
             let _ = next_token(input);
             let expr = parse_expr(input)?;
             FunctionBody::ScalarReturn(expr)
        } else {
             expect_keyword(input, "TABLE")?;
             expect_keyword(input, "RETURN")?;
             expect_punctuation(input, Token::LParen)?;
             let query = parse_select(input)?;
             expect_punctuation(input, Token::RParen)?;
             FunctionBody::Table(query)
        };
        Ok(CreateStmt::Function { name, params, returns, body })
    } else if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("TRIGGER")) {
        let _ = next_token(input);
        let name = multipart_name(input)?;
        expect_keyword(input, "ON")?;
        let table = multipart_name(input)?;
        
        let mut is_instead_of = false;
        if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("INSTEAD")) {
            let _ = next_token(input);
            expect_keyword(input, "OF")?;
            is_instead_of = true;
        } else {
            if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("AFTER") || k.eq_ignore_ascii_case("FOR")) {
                let _ = next_token(input);
            }
        }

        let events = parse_comma_list(input, |i| {
            match next_token(i) {
                Some(Token::Keyword(k)) => match k.to_uppercase().as_str() {
                    "INSERT" => Ok(crate::ast::TriggerEvent::Insert),
                    "UPDATE" => Ok(crate::ast::TriggerEvent::Update),
                    "DELETE" => Ok(crate::ast::TriggerEvent::Delete),
                    _ => Err(ErrMode::Backtrack(ContextError::new())),
                }
                _ => Err(ErrMode::Backtrack(ContextError::new())),
            }
        })?;

        expect_keyword(input, "AS")?;
        let body = if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("BEGIN")) {
             let _ = next_token(input);
             match parse_begin_end(input, parse_statement)? {
                  Statement::BeginEnd(stmts) => stmts,
                  _ => unreachable!(),
             }
        } else {
             vec![parse_statement(input)?]
        };
        Ok(CreateStmt::Trigger { name, table, events, is_instead_of, body })
    } else {
        Err(ErrMode::Backtrack(ContextError::new()))
    }
}

pub fn parse_column_def<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<ColumnDef<'a>> {
    let name = if let Some(tok) = next_token(input) {
        match tok {
            Token::Identifier(id) | Token::Keyword(id) => id.clone(),
            _ => return Err(ErrMode::Backtrack(ContextError::new())),
        }
    } else {
        return Err(ErrMode::Backtrack(ContextError::new()));
    };
    let data_type = parse_data_type(input)?;
    
    let mut is_nullable = None;
    let mut is_identity = false;
    let mut identity_spec = None;
    let mut is_primary_key = false;
    let mut is_unique = false;
    let mut default_expr = None;
    let mut default_constraint_name = None;
    let mut check_expr = None;
    let mut check_constraint_name = None;
    let mut computed_expr = None;
    let mut foreign_key = None;

    while let Some(Token::Keyword(k)) = peek_token(input) {
        let k_upper = k.to_uppercase();
        match k_upper.as_str() {
            "NULL" => { let _ = next_token(input); is_nullable = Some(true); }
            "NOT" => {
                let _ = next_token(input);
                expect_keyword(input, "NULL")?;
                is_nullable = Some(false);
            }
            "IDENTITY" => {
                let _ = next_token(input);
                if matches!(peek_token(input), Some(Token::LParen)) {
                    let _ = next_token(input);
                    let seed = if let Some(Token::Number(n)) = next_token(input) { *n as i64 } else { 1 };
                    expect_punctuation(input, Token::Comma)?;
                    let inc = if let Some(Token::Number(n)) = next_token(input) { *n as i64 } else { 1 };
                    expect_punctuation(input, Token::RParen)?;
                    identity_spec = Some((seed, inc));
                }
                is_identity = true;
            }
            "PRIMARY" => {
                let _ = next_token(input);
                expect_keyword(input, "KEY")?;
                is_primary_key = true;
            }
            "UNIQUE" => {
                let _ = next_token(input);
                is_unique = true;
            }
            "DEFAULT" => {
                let _ = next_token(input);
                default_expr = Some(parse_expr(input)?);
            }
            "CHECK" => {
                let _ = next_token(input);
                expect_punctuation(input, Token::LParen)?;
                check_expr = Some(parse_expr(input)?);
                expect_punctuation(input, Token::RParen)?;
            }
            "CONSTRAINT" => {
                let _ = next_token(input);
                let constraint_name = match next_token(input) {
                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                };
                match next_token(input) {
                    Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("DEFAULT") => {
                        default_expr = Some(parse_expr(input)?);
                        default_constraint_name = Some(constraint_name);
                    }
                    Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("CHECK") => {
                        expect_punctuation(input, Token::LParen)?;
                        check_expr = Some(parse_expr(input)?);
                        expect_punctuation(input, Token::RParen)?;
                        check_constraint_name = Some(constraint_name);
                    }
                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                }
            }
            "REFERENCES" => {
                let _ = next_token(input);
                let ref_table = multipart_name(input)?;
                let mut ref_columns = Vec::new();
                if matches!(peek_token(input), Some(Token::LParen)) {
                    let _ = next_token(input);
                    ref_columns = parse_comma_list(input, |i| {
                        match next_token(i) {
                            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                            _ => Err(ErrMode::Backtrack(ContextError::new())),
                        }
                    })?;
                    expect_punctuation(input, Token::RParen)?;
                }
                foreign_key = Some(ForeignKeyRef {
                    ref_table,
                    ref_columns,
                    on_delete: None,
                    on_update: None,
                });
            }
            "AS" => {
                let _ = next_token(input);
                computed_expr = Some(parse_expr(input)?);
            }
            _ => break,
        }
    }

    Ok(ColumnDef {
        name,
        data_type,
        is_nullable,
        is_identity,
        identity_spec,
        is_primary_key,
        is_unique,
        default_expr,
        default_constraint_name,
        check_expr,
        check_constraint_name,
        computed_expr,
        foreign_key,
    })
}

pub fn parse_table_body<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<(Vec<ColumnDef<'a>>, Vec<TableConstraint<'a>>)> {
    let mut columns = Vec::new();
    let mut constraints = Vec::new();

    loop {
        // Check if it's a constraint
        let mut is_constraint = false;
        if let Some(Token::Keyword(kw)) = peek_token(input) {
            let kw_upper = kw.to_uppercase();
            if matches!(kw_upper.as_str(), "CONSTRAINT" | "PRIMARY" | "UNIQUE" | "FOREIGN" | "CHECK") {
                is_constraint = true;
            }
        }

        if is_constraint {
            constraints.push(parse_table_constraint(input)?);
        } else {
            columns.push(parse_column_def(input)?);
        }

        if matches!(peek_token(input), Some(Token::Comma)) {
            let _ = next_token(input);
            continue;
        }
        break;
    }

    Ok((columns, constraints))
}

fn parse_table_constraint<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<TableConstraint<'a>> {
    let mut name = None;
    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("CONSTRAINT")) {
        let _ = next_token(input);
        name = Some(match next_token(input) {
            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
            _ => return Err(ErrMode::Backtrack(ContextError::new())),
        });
    }

    let kw = match next_token(input) {
        Some(Token::Keyword(kw)) => kw.to_uppercase(),
        _ => return Err(ErrMode::Backtrack(ContextError::new())),
    };

    match kw.as_str() {
        "PRIMARY" => {
            expect_keyword(input, "KEY")?;
            expect_punctuation(input, Token::LParen)?;
            let columns = parse_comma_list(input, |i| {
                match next_token(i) {
                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                    _ => Err(ErrMode::Backtrack(ContextError::new())),
                }
            })?;
            expect_punctuation(input, Token::RParen)?;
            Ok(TableConstraint::PrimaryKey { name, columns })
        }
        "UNIQUE" => {
            expect_punctuation(input, Token::LParen)?;
            let columns = parse_comma_list(input, |i| {
                match next_token(i) {
                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                    _ => Err(ErrMode::Backtrack(ContextError::new())),
                }
            })?;
            expect_punctuation(input, Token::RParen)?;
            Ok(TableConstraint::Unique { name, columns })
        }
        "FOREIGN" => {
            expect_keyword(input, "KEY")?;
            expect_punctuation(input, Token::LParen)?;
            let columns = parse_comma_list(input, |i| {
                match next_token(i) {
                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                    _ => Err(ErrMode::Backtrack(ContextError::new())),
                }
            })?;
            expect_punctuation(input, Token::RParen)?;
            expect_keyword(input, "REFERENCES")?;
            let ref_table = multipart_name(input)?;
            let mut ref_columns = Vec::new();
            if matches!(peek_token(input), Some(Token::LParen)) {
                let _ = next_token(input);
                ref_columns = parse_comma_list(input, |i| {
                    match next_token(i) {
                        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                        _ => Err(ErrMode::Backtrack(ContextError::new())),
                    }
                })?;
                expect_punctuation(input, Token::RParen)?;
            }
            Ok(TableConstraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
                on_delete: None,
                on_update: None,
            })
        }
        "CHECK" => {
            expect_punctuation(input, Token::LParen)?;
            let expr = parse_expr(input)?;
            expect_punctuation(input, Token::RParen)?;
            Ok(TableConstraint::Check { name, expr })
        }
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}

pub fn parse_create_index<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    let name = multipart_name(input)?;
    expect_keyword(input, "ON")?;
    let table = multipart_name(input)?;
    expect_punctuation(input, Token::LParen)?;
    let columns = parse_comma_list(input, |i| {
        match next_token(i) {
            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
            _ => Err(ErrMode::Backtrack(ContextError::new())),
        }
    })?;
    expect_punctuation(input, Token::RParen)?;
    Ok(Statement::CreateIndex { name, table, columns })
}

pub fn parse_create_type<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    let name = multipart_name(input)?;
    expect_keyword(input, "AS")?;
    expect_keyword(input, "TABLE")?;
    expect_punctuation(input, Token::LParen)?;
    let (columns, _constraints) = parse_table_body(input)?;
    expect_punctuation(input, Token::RParen)?;
    Ok(Statement::CreateType { name, columns })
}

pub fn parse_create_schema<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Statement<'a>> {
    let name = match next_token(input) {
        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
        _ => return Err(ErrMode::Backtrack(ContextError::new())),
    };
    Ok(Statement::CreateSchema(name))
}
