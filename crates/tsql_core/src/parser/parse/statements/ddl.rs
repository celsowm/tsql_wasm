use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseError, ParseResult, Expected};
use std::borrow::Cow;

pub use super::create::{parse_create_table, parse_create_view, parse_create_procedure, parse_create_function, parse_create_trigger};

pub fn parse_create<'a>(parser: &mut Parser<'a>) -> ParseResult<CreateStmt<'a>> {
    if parser.at_keyword(Keyword::Table) {
        let _ = parser.next();
        parse_create_table(parser)
    } else if parser.at_keyword(Keyword::View) {
        let _ = parser.next();
        parse_create_view(parser)
    } else if matches!(parser.peek(), Some(Token::Keyword(kw)) if matches!(kw, Keyword::Procedure | Keyword::Proc)) {
        let _ = parser.next();
        parse_create_procedure(parser)
    } else if parser.at_keyword(Keyword::Function) {
        let _ = parser.next();
        parse_create_function(parser)
    } else if parser.at_keyword(Keyword::Trigger) {
        let _ = parser.next();
        parse_create_trigger(parser)
    } else {
        parser.backtrack(Expected::Description("TABLE, VIEW, PROCEDURE, FUNCTION, or TRIGGER"))
    }
}

pub fn parse_column_def<'a>(parser: &mut Parser<'a>) -> ParseResult<ColumnDef<'a>> {
    let name = if let Some(tok) = parser.next() {
        match tok {
            Token::Identifier(id) => id.clone(),
            Token::Keyword(k) => Cow::Owned(k.as_ref().to_string()),
            _ => return parser.backtrack(Expected::Description("column name")),
        }
    } else {
        return parser.backtrack(Expected::Description("column name"));
    };
    let data_type = if matches!(parser.peek(), Some(Token::Keyword(Keyword::As))) {
        DataType::SqlVariant
    } else {
        crate::parser::parse::expressions::parse_data_type(parser)?
    };
    
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

    while let Some(Token::Keyword(k)) = parser.peek() {
        match *k {
            Keyword::Null => { let _ = parser.next(); is_nullable = Some(true); }
            Keyword::Not => {
                let _ = parser.next();
                parser.expect_keyword(Keyword::Null)?;
                is_nullable = Some(false);
            }
            Keyword::Identity => {
                let _ = parser.next();
                if matches!(parser.peek(), Some(Token::LParen)) {
                    let _ = parser.next();
                    let seed = if let Some(Token::Number(n)) = parser.next() { *n as i64 } else { 1 };
                    parser.expect_comma()?;
                    let inc = if let Some(Token::Number(n)) = parser.next() { *n as i64 } else { 1 };
                    parser.expect_rparen()?;
                    identity_spec = Some((seed, inc));
                }
                is_identity = true;
            }
            Keyword::Primary => {
                let _ = parser.next();
                parser.expect_keyword(Keyword::Key)?;
                is_primary_key = true;
            }
            Keyword::Unique => {
                let _ = parser.next();
                is_unique = true;
            }
            Keyword::Default => {
                let _ = parser.next();
                default_expr = Some(crate::parser::parse::expressions::parse_expr(parser)?);
            }
            Keyword::Check => {
                let _ = parser.next();
                parser.expect_lparen()?;
                check_expr = Some(crate::parser::parse::expressions::parse_expr(parser)?);
                parser.expect_rparen()?;
            }
            Keyword::Constraint => {
                let _ = parser.next();
                let constraint_name = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                    _ => return parser.backtrack(Expected::Description("constraint name")),
                };
                match parser.next() {
                    Some(Token::Keyword(Keyword::Default)) => {
                        default_expr = Some(crate::parser::parse::expressions::parse_expr(parser)?);
                        default_constraint_name = Some(constraint_name);
                    }
                    Some(Token::Keyword(Keyword::Check)) => {
                        parser.expect_lparen()?;
                        check_expr = Some(crate::parser::parse::expressions::parse_expr(parser)?);
                        parser.expect_rparen()?;
                        check_constraint_name = Some(constraint_name);
                    }
                    _ => return parser.backtrack(Expected::Description("DEFAULT or CHECK")),
                }
            }
            Keyword::References => {
                let _ = parser.next();
                let ref_table = parse_multipart_name(parser)?;
                let mut ref_columns = Vec::new();
                if matches!(parser.peek(), Some(Token::LParen)) {
                    let _ = parser.next();
                    ref_columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                        match p.next() {
                            Some(Token::Identifier(id)) => Ok(id.clone()),
                            Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                            _ => p.backtrack(Expected::Description("column name")),
                        }
                    })?;
                    parser.expect_rparen()?;
                }
                foreign_key = Some(ForeignKeyRef {
                    ref_table,
                    ref_columns,
                    on_delete: None,
                    on_update: None,
                });
            }
            Keyword::As => {
                let _ = parser.next();
                computed_expr = Some(crate::parser::parse::expressions::parse_expr(parser)?);
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

pub fn parse_table_body<'a>(parser: &mut Parser<'a>) -> ParseResult<(Vec<ColumnDef<'a>>, Vec<TableConstraint<'a>>)> {
    let mut columns = Vec::new();
    let mut constraints = Vec::new();

    loop {
        let mut is_constraint = false;
        if let Some(Token::Keyword(kw)) = parser.peek() {
            if matches!(kw, Keyword::Constraint | Keyword::Primary | Keyword::Unique | Keyword::Foreign | Keyword::Check) {
                is_constraint = true;
            }
        }

        if is_constraint {
            constraints.push(parse_table_constraint(parser)?);
        } else {
            columns.push(parse_column_def(parser)?);
        }

        if matches!(parser.peek(), Some(Token::Comma)) {
            let _ = parser.next();
            continue;
        }
        break;
    }

    Ok((columns, constraints))
}

pub fn parse_table_constraint<'a>(parser: &mut Parser<'a>) -> ParseResult<TableConstraint<'a>> {
    let mut name = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Constraint))) {
        let _ = parser.next();
        name = Some(match parser.next() {
            Some(Token::Identifier(id)) => id.clone(),
            Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
            _ => return parser.backtrack(Expected::Description("constraint name")),
        });
    }

    let kw = match parser.next() {
        Some(Token::Keyword(kw)) => *kw,
        _ => return parser.backtrack(Expected::Description("constraint keyword")),
    };

    match kw {
        Keyword::Primary => {
            parser.expect_keyword(Keyword::Key)?;
            parser.expect_lparen()?;
            let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                match p.next() {
                    Some(Token::Identifier(id)) => Ok(id.clone()),
                    Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                    _ => p.backtrack(Expected::Description("column name")),
                }
            })?;
            parser.expect_rparen()?;
            Ok(TableConstraint::PrimaryKey { name, columns })
        }
        Keyword::Unique => {
            parser.expect_lparen()?;
            let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                match p.next() {
                    Some(Token::Identifier(id)) => Ok(id.clone()),
                    Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                    _ => p.backtrack(Expected::Description("column name")),
                }
            })?;
            parser.expect_rparen()?;
            Ok(TableConstraint::Unique { name, columns })
        }
        Keyword::Foreign => {
            parser.expect_keyword(Keyword::Key)?;
            parser.expect_lparen()?;
            let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                match p.next() {
                    Some(Token::Identifier(id)) => Ok(id.clone()),
                    Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                    _ => p.backtrack(Expected::Description("column name")),
                }
            })?;
            parser.expect_rparen()?;
            parser.expect_keyword(Keyword::References)?;
            let ref_table = parse_multipart_name(parser)?;
            let mut ref_columns = Vec::new();
            if matches!(parser.peek(), Some(Token::LParen)) {
                let _ = parser.next();
                ref_columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                    match p.next() {
                        Some(Token::Identifier(id)) => Ok(id.clone()),
                        Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                        _ => p.backtrack(Expected::Description("column name")),
                    }
                })?;
                parser.expect_rparen()?;
            }
            let mut on_delete = None;
            let mut on_update = None;
            while let Some(Token::Keyword(kw)) = parser.peek() {
                if *kw == Keyword::On {
                    let saved = parser.save();
                    let _ = parser.next();
                    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Delete))) {
                        let _ = parser.next();
                        on_delete = Some(parse_referential_action(parser)?);
                    } else if matches!(parser.peek(), Some(Token::Keyword(Keyword::Update))) {
                        let _ = parser.next();
                        on_update = Some(parse_referential_action(parser)?);
                    } else {
                        parser.restore(saved);
                        break;
                    }
                } else {
                    break;
                }
            }
            Ok(TableConstraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
            })
        }
        Keyword::Check => {
            parser.expect_lparen()?;
            let expr = crate::parser::parse::expressions::parse_expr(parser)?;
            parser.expect_rparen()?;
            Ok(TableConstraint::Check { name, expr })
        }
        Keyword::Default => {
            let expr = crate::parser::parse::expressions::parse_expr(parser)?;
            parser.expect_keyword(Keyword::For)?;
            let column = match parser.next() {
                Some(Token::Identifier(id)) => id.clone(),
                Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                _ => return parser.backtrack(Expected::Description("column name")),
            };
            Ok(TableConstraint::Default { name, column, expr })
        }
        _ => parser.backtrack(Expected::Description("constraint type")),
    }
}

pub fn parse_create_index<'a>(parser: &mut Parser<'a>) -> ParseResult<Statement<'a>> {
    let name = parse_multipart_name(parser)?;
    parser.expect_keyword(Keyword::On)?;
    let table = parse_multipart_name(parser)?;
    parser.expect_lparen()?;
    let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
        match p.next() {
            Some(Token::Identifier(id)) => Ok(id.clone()),
            Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
            _ => p.backtrack(Expected::Description("column name")),
        }
    })?;
    parser.expect_rparen()?;
    Ok(Statement::Ddl(DdlStatement::CreateIndex { name, table, columns }))
}

pub fn parse_create_type<'a>(parser: &mut Parser<'a>) -> ParseResult<Statement<'a>> {
    let name = parse_multipart_name(parser)?;
    parser.expect_keyword(Keyword::As)?;
    parser.expect_keyword(Keyword::Table)?;
    parser.expect_lparen()?;
    let (columns, _constraints) = parse_table_body(parser)?;
    parser.expect_rparen()?;
    Ok(Statement::Ddl(DdlStatement::CreateType { name, columns }))
}

pub fn parse_create_schema<'a>(parser: &mut Parser<'a>) -> ParseResult<Statement<'a>> {
    let name = match parser.next() {
        Some(Token::Identifier(id)) => id.clone(),
        Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
        _ => return parser.backtrack(Expected::Description("schema name")),
    };
    Ok(Statement::Ddl(DdlStatement::CreateSchema(name)))
}

fn parse_referential_action<'a>(parser: &mut Parser<'a>) -> ParseResult<ReferentialAction> {
    match parser.next() {
        Some(Token::Keyword(k)) => match *k {
            Keyword::No => {
                parser.expect_keyword(Keyword::Action)?;
                Ok(ReferentialAction::NoAction)
            }
            Keyword::Cascade => Ok(ReferentialAction::Cascade),
            Keyword::Set => {
                if matches!(parser.peek(), Some(Token::Keyword(Keyword::Null))) {
                    let _ = parser.next();
                    Ok(ReferentialAction::SetNull)
                } else if matches!(parser.peek(), Some(Token::Keyword(Keyword::Default))) {
                    let _ = parser.next();
                    Ok(ReferentialAction::SetDefault)
                } else {
                    parser.backtrack(Expected::Description("NULL or DEFAULT"))
                }
            }
            _ => parser.backtrack(Expected::Description("referential action")),
        },
        _ => parser.backtrack(Expected::Description("referential action")),
    }
}

fn parse_begin_end<'a>(parser: &mut Parser<'a>) -> ParseResult<Statement<'a>> {
    crate::parser::parse::statements::other::parse_begin_end(parser)
}

fn parse_multipart_name<'a>(parser: &mut Parser<'a>) -> ParseResult<Vec<Cow<'a, str>>> {
    crate::parser::parse::statements::query::parse_multipart_name(parser)
}

pub fn parse_alter_table_add_constraint<'a>(parser: &mut Parser<'a>) -> ParseResult<TableConstraint<'a>> {
    let constraint_name = match parser.next() {
        Some(Token::Identifier(id)) => id.clone(),
        Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
        _ => return parser.backtrack(Expected::Description("constraint name")),
    };
    let constraint = if parser.at_keyword(Keyword::Primary) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Key)?;
        parser.expect_lparen()?;
        let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
            match p.next() {
                Some(Token::Identifier(id)) => Ok(id.clone()),
                Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                _ => p.backtrack(Expected::Description("column name")),
            }
        })?;
        parser.expect_rparen()?;
        TableConstraint::PrimaryKey {
            name: Some(constraint_name),
            columns,
        }
    } else if parser.at_keyword(Keyword::Foreign) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Key)?;
        parser.expect_lparen()?;
        let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
            match p.next() {
                Some(Token::Identifier(id)) => Ok(id.clone()),
                Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                _ => p.backtrack(Expected::Description("column name")),
            }
        })?;
        parser.expect_rparen()?;
        parser.expect_keyword(Keyword::References)?;
        let ref_table = parse_multipart_name(parser)?;
        let mut ref_columns = Vec::new();
        if matches!(parser.peek(), Some(Token::LParen)) {
            let _ = parser.next();
            ref_columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                match p.next() {
                    Some(Token::Identifier(id)) => Ok(id.clone()),
                    Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                    _ => p.backtrack(Expected::Description("column name")),
                }
            })?;
            parser.expect_rparen()?;
        }
        let mut on_delete = None;
        let mut on_update = None;
        while let Some(Token::Keyword(kw)) = parser.peek() {
            if *kw == Keyword::On {
                let _ = parser.next();
                if parser.at_keyword(Keyword::Delete) {
                    let _ = parser.next();
                    on_delete = Some(parse_referential_action(parser)?);
                } else if parser.at_keyword(Keyword::Update) {
                    let _ = parser.next();
                    on_update = Some(parse_referential_action(parser)?);
                }
            } else {
                break;
            }
        }
        TableConstraint::ForeignKey {
            name: Some(constraint_name),
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        }
    } else if parser.at_keyword(Keyword::Check) {
        let _ = parser.next();
        parser.expect_lparen()?;
        let expr = crate::parser::parse::expressions::parse_expr(parser)?;
        parser.expect_rparen()?;
        TableConstraint::Check {
            name: Some(constraint_name),
            expr,
        }
    } else if parser.at_keyword(Keyword::Unique) {
        let _ = parser.next();
        parser.expect_lparen()?;
        let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
            match p.next() {
                Some(Token::Identifier(id)) => Ok(id.clone()),
                Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                _ => p.backtrack(Expected::Description("column name")),
            }
        })?;
        parser.expect_rparen()?;
        TableConstraint::Unique {
            name: Some(constraint_name),
            columns,
        }
    } else {
        return parser.backtrack(Expected::Description("constraint type"));
    };
    Ok(constraint)
}
