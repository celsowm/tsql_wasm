use crate::parser::ast::*;
use crate::parser::error::{Expected, ParseResult};
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

pub use super::create::{
    parse_create_function, parse_create_procedure, parse_create_table, parse_create_trigger,
    parse_create_view,
};

pub fn parse_create(parser: &mut Parser) -> ParseResult<CreateStmt> {
    if parser.at_keyword(Keyword::Table) {
        let _ = parser.next();
        parse_create_table(parser)
    } else if parser.at_keyword(Keyword::View) {
        let _ = parser.next();
        parse_create_view(parser)
    } else if matches!(parser.peek(), Some(Token::Keyword(kw)) if matches!(kw, Keyword::Procedure | Keyword::Proc))
    {
        let _ = parser.next();
        parse_create_procedure(parser)
    } else if parser.at_keyword(Keyword::Function) {
        let _ = parser.next();
        parse_create_function(parser)
    } else if parser.at_keyword(Keyword::Trigger) {
        let _ = parser.next();
        parse_create_trigger(parser)
    } else {
        parser.backtrack(Expected::Description(
            "TABLE, VIEW, PROCEDURE, FUNCTION, or TRIGGER",
        ))
    }
}

pub fn parse_column_def(parser: &mut Parser) -> ParseResult<ColumnDef> {
    let name = if let Some(tok) = parser.next() {
        match tok {
            Token::Identifier(id) => id.clone(),
            Token::Keyword(k) => k.as_ref().to_string(),
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
    let mut collation = None;
    let mut is_clustered = false;

    while let Some(Token::Keyword(k)) = parser.peek() {
        match *k {
            Keyword::Null => {
                let _ = parser.next();
                is_nullable = Some(true);
            }
            Keyword::Not => {
                let _ = parser.next();
                parser.expect_keyword(Keyword::Null)?;
                is_nullable = Some(false);
            }
            Keyword::Identity => {
                let _ = parser.next();
                if matches!(parser.peek(), Some(Token::LParen)) {
                    let _ = parser.next();
                    let seed = if let Some(Token::Number { value: n, .. }) = parser.next() {
                        *n as i64
                    } else {
                        1
                    };
                    parser.expect_comma()?;
                    let inc = if let Some(Token::Number { value: n, .. }) = parser.next() {
                        *n as i64
                    } else {
                        1
                    };
                    parser.expect_rparen()?;
                    identity_spec = Some((seed, inc));
                }
                is_identity = true;
            }
            Keyword::Primary => {
                let _ = parser.next();
                parser.expect_keyword(Keyword::Key)?;
                if parser.at_keyword(Keyword::Clustered) {
                    let _ = parser.next();
                    is_clustered = true;
                } else if parser.at_keyword(Keyword::Nonclustered) {
                    let _ = parser.next();
                    is_clustered = false;
                }
                is_primary_key = true;
            }
            Keyword::Unique => {
                let _ = parser.next();
                if parser.at_keyword(Keyword::Clustered) {
                    let _ = parser.next();
                    is_clustered = true;
                } else if parser.at_keyword(Keyword::Nonclustered) {
                    let _ = parser.next();
                    is_clustered = false;
                }
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
                    Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
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
                let ref_table = super::parse_multipart_name(parser)?;
                let mut ref_columns = Vec::new();
                if matches!(parser.peek(), Some(Token::LParen)) {
                    let _ = parser.next();
                    ref_columns =
                        crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                            match p.next() {
                                Some(Token::Identifier(id)) => Ok(id.clone()),
                                Some(Token::Keyword(kw)) => Ok(kw.as_ref().to_string()),
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
            Keyword::Collate => {
                let _ = parser.next();
                let collation_name = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
                    _ => return parser.backtrack(Expected::Description("collation name")),
                };
                collation = Some(collation_name);
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
        collation,
        is_clustered,
    })
}

pub fn parse_table_body(
    parser: &mut Parser,
) -> ParseResult<(Vec<ColumnDef>, Vec<TableConstraint>)> {
    let mut columns = Vec::new();
    let mut constraints = Vec::new();

    loop {
        let mut is_constraint = false;
        if let Some(Token::Keyword(kw)) = parser.peek() {
            if matches!(
                kw,
                Keyword::Constraint
                    | Keyword::Primary
                    | Keyword::Unique
                    | Keyword::Foreign
                    | Keyword::Check
            ) {
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

pub fn parse_table_constraint(parser: &mut Parser) -> ParseResult<TableConstraint> {
    let mut name = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Constraint))) {
        let _ = parser.next();
        name = Some(match parser.next() {
            Some(Token::Identifier(id)) => id.clone(),
            Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
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
            let mut is_clustered = false;
            if parser.at_keyword(Keyword::Clustered) {
                let _ = parser.next();
                is_clustered = true;
            } else if parser.at_keyword(Keyword::Nonclustered) {
                let _ = parser.next();
                is_clustered = false;
            }
            parser.expect_lparen()?;
            let columns =
                crate::parser::parse::expressions::parse_comma_list(parser, parse_index_column)?;
            parser.expect_rparen()?;
            Ok(TableConstraint::PrimaryKey {
                name,
                columns,
                is_clustered,
            })
        }
        Keyword::Unique => {
            let mut is_clustered = false;
            if parser.at_keyword(Keyword::Clustered) {
                let _ = parser.next();
                is_clustered = true;
            } else if parser.at_keyword(Keyword::Nonclustered) {
                let _ = parser.next();
                is_clustered = false;
            }
            parser.expect_lparen()?;
            let columns =
                crate::parser::parse::expressions::parse_comma_list(parser, parse_index_column)?;
            parser.expect_rparen()?;
            Ok(TableConstraint::Unique {
                name,
                columns,
                is_clustered,
            })
        }
        Keyword::Foreign => {
            parser.expect_keyword(Keyword::Key)?;
            parser.expect_lparen()?;
            let columns =
                crate::parser::parse::expressions::parse_comma_list(parser, |p| match p.next() {
                    Some(Token::Identifier(id)) => Ok(id.clone()),
                    Some(Token::Keyword(kw)) => Ok(kw.as_ref().to_string()),
                    _ => p.backtrack(Expected::Description("column name")),
                })?;
            parser.expect_rparen()?;
            parser.expect_keyword(Keyword::References)?;
            let ref_table = super::parse_multipart_name(parser)?;
            let mut ref_columns = Vec::new();
            if matches!(parser.peek(), Some(Token::LParen)) {
                let _ = parser.next();
                ref_columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                    match p.next() {
                        Some(Token::Identifier(id)) => Ok(id.clone()),
                        Some(Token::Keyword(kw)) => Ok(kw.as_ref().to_string()),
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
                Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
                _ => return parser.backtrack(Expected::Description("column name")),
            };
            Ok(TableConstraint::Default { name, column, expr })
        }
        _ => parser.backtrack(Expected::Description("constraint type")),
    }
}

pub fn parse_create_index(
    parser: &mut Parser,
    is_unique: bool,
    is_clustered: bool,
) -> ParseResult<CreateIndexStmt> {
    let name = super::parse_multipart_name(parser)?;
    parser.expect_keyword(Keyword::On)?;
    let table = super::parse_multipart_name(parser)?;
    parser.expect_lparen()?;
    let columns = crate::parser::parse::expressions::parse_comma_list(parser, parse_index_column)?;
    parser.expect_rparen()?;

    let mut options = Vec::new();
    if parser.at_keyword(Keyword::With) {
        let _ = parser.next();
        parser.expect_lparen()?;
        options = crate::parser::parse::expressions::parse_comma_list(parser, parse_index_option)?;
        parser.expect_rparen()?;
    }

    Ok(CreateIndexStmt {
        name,
        table,
        is_unique,
        is_clustered,
        columns,
        options,
    })
}

fn parse_index_column(parser: &mut Parser) -> ParseResult<IndexColumn> {
    let name = match parser.next() {
        Some(Token::Identifier(id)) => id.clone(),
        Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
        _ => return parser.backtrack(Expected::Description("column name")),
    };
    let mut is_desc = false;
    if parser.at_keyword(Keyword::Desc) {
        let _ = parser.next();
        is_desc = true;
    } else if parser.at_keyword(Keyword::Asc) {
        let _ = parser.next();
        is_desc = false;
    }
    Ok(IndexColumn { name, is_desc })
}

fn parse_index_option(parser: &mut Parser) -> ParseResult<IndexOption> {
    if parser.at_keyword(Keyword::Fillfactor) {
        let _ = parser.next();
        if let Some(Token::Operator(op)) = parser.next() {
            if op != "=" {
                return parser.backtrack(Expected::Description("="));
            }
        } else {
            return parser.backtrack(Expected::Description("="));
        }
        if let Some(Token::Number { value: n, .. }) = parser.next() {
            Ok(IndexOption::FillFactor(*n as u8))
        } else {
            parser.backtrack(Expected::Description("number"))
        }
    } else {
        parser.backtrack(Expected::Description("index option"))
    }
}

pub fn parse_create_type(parser: &mut Parser) -> ParseResult<Statement> {
    let name = super::parse_multipart_name(parser)?;
    parser.expect_keyword(Keyword::As)?;
    parser.expect_keyword(Keyword::Table)?;
    parser.expect_lparen()?;
    let (columns, _constraints) = parse_table_body(parser)?;
    parser.expect_rparen()?;
    Ok(Statement::Ddl(DdlStatement::CreateType { name, columns }))
}

pub fn parse_create_schema(parser: &mut Parser) -> ParseResult<Statement> {
    let name = match parser.next() {
        Some(Token::Identifier(id)) => id.clone(),
        Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
        _ => return parser.backtrack(Expected::Description("schema name")),
    };
    Ok(Statement::Ddl(DdlStatement::CreateSchema(name)))
}

pub fn parse_create_sequence(parser: &mut Parser) -> ParseResult<Statement> {
    let name = super::parse_multipart_name(parser)?;
    let mut data_type = None;
    let mut start_with = None;
    let mut increment_by = None;
    let mut min_value = None;
    let mut max_value = None;
    let mut cycle = false;

    while !parser.is_empty() {
        if matches!(parser.peek(), Some(Token::Semicolon) | Some(Token::Go)) {
            break;
        }

        if parser.at_keyword(Keyword::As) {
            let _ = parser.next();
            data_type = Some(crate::parser::parse::expressions::parse_data_type(parser)?);
        } else if parser.at_keyword(Keyword::Start) {
            let _ = parser.next();
            parser.expect_keyword(Keyword::With)?;
            start_with = Some(parse_signed_int(parser)?);
        } else if parser.at_keyword(Keyword::Increment) {
            let _ = parser.next();
            parser.expect_keyword(Keyword::By)?;
            increment_by = Some(parse_signed_int(parser)?);
        } else if parser.at_keyword(Keyword::Minvalue) {
            let _ = parser.next();
            min_value = Some(parse_signed_int(parser)?);
        } else if parser.at_keyword(Keyword::Maxvalue) {
            let _ = parser.next();
            max_value = Some(parse_signed_int(parser)?);
        } else if parser.at_keyword(Keyword::No) {
            let _ = parser.next();
            if parser.at_keyword(Keyword::Minvalue) {
                let _ = parser.next();
            } else if parser.at_keyword(Keyword::Maxvalue) {
                let _ = parser.next();
            } else if parser.at_keyword(Keyword::Cycle) {
                let _ = parser.next();
                cycle = false;
            } else {
                return parser.backtrack(Expected::Description("MINVALUE, MAXVALUE, or CYCLE"));
            }
        } else if parser.at_keyword(Keyword::Cycle) {
            let _ = parser.next();
            cycle = true;
        } else {
            break;
        }
    }

    Ok(Statement::Ddl(DdlStatement::CreateSequence {
        name,
        data_type,
        start_with,
        increment_by,
        min_value,
        max_value,
        cycle,
    }))
}

fn parse_signed_int(parser: &mut Parser) -> ParseResult<i64> {
    let sign = if matches!(parser.peek(), Some(Token::Operator(op)) if *op == "-") {
        let _ = parser.next();
        -1i64
    } else if matches!(parser.peek(), Some(Token::Operator(op)) if *op == "+") {
        let _ = parser.next();
        1i64
    } else {
        1i64
    };

    if let Some(Token::Number { value: n, .. }) = parser.next() {
        Ok(sign * (*n as i64))
    } else {
        parser.backtrack(Expected::Description("number"))
    }
}

fn parse_referential_action(parser: &mut Parser) -> ParseResult<ReferentialAction> {
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

pub fn parse_alter_table_add_constraint(parser: &mut Parser) -> ParseResult<TableConstraint> {
    let constraint_name = match parser.next() {
        Some(Token::Identifier(id)) => id.clone(),
        Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
        _ => return parser.backtrack(Expected::Description("constraint name")),
    };
    let constraint = if parser.at_keyword(Keyword::Primary) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Key)?;
        let mut is_clustered = false;
        if parser.at_keyword(Keyword::Clustered) {
            let _ = parser.next();
            is_clustered = true;
        } else if parser.at_keyword(Keyword::Nonclustered) {
            let _ = parser.next();
            is_clustered = false;
        }
        parser.expect_lparen()?;
        let columns =
            crate::parser::parse::expressions::parse_comma_list(parser, parse_index_column)?;
        parser.expect_rparen()?;
        TableConstraint::PrimaryKey {
            name: Some(constraint_name),
            columns,
            is_clustered,
        }
    } else if parser.at_keyword(Keyword::Unique) {
        let _ = parser.next();
        let mut is_clustered = false;
        if parser.at_keyword(Keyword::Clustered) {
            let _ = parser.next();
            is_clustered = true;
        } else if parser.at_keyword(Keyword::Nonclustered) {
            let _ = parser.next();
            is_clustered = false;
        }
        parser.expect_lparen()?;
        let columns =
            crate::parser::parse::expressions::parse_comma_list(parser, parse_index_column)?;
        parser.expect_rparen()?;
        TableConstraint::Unique {
            name: Some(constraint_name),
            columns,
            is_clustered,
        }
    } else if parser.at_keyword(Keyword::Foreign) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Key)?;
        parser.expect_lparen()?;
        let columns =
            crate::parser::parse::expressions::parse_comma_list(parser, |p| match p.next() {
                Some(Token::Identifier(id)) => Ok(id.clone()),
                Some(Token::Keyword(kw)) => Ok(kw.as_ref().to_string()),
                _ => p.backtrack(Expected::Description("column name")),
            })?;
        parser.expect_rparen()?;
        parser.expect_keyword(Keyword::References)?;
        let ref_table = super::parse_multipart_name(parser)?;
        let mut ref_columns = Vec::new();
        if matches!(parser.peek(), Some(Token::LParen)) {
            let _ = parser.next();
            ref_columns =
                crate::parser::parse::expressions::parse_comma_list(parser, |p| match p.next() {
                    Some(Token::Identifier(id)) => Ok(id.clone()),
                    Some(Token::Keyword(kw)) => Ok(kw.as_ref().to_string()),
                    _ => p.backtrack(Expected::Description("column name")),
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
    } else {
        return parser.backtrack(Expected::Description("constraint type"));
    };
    Ok(constraint)
}
