use crate::parser::ast::*;
use crate::parser::error::{Expected, ParseResult};
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

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

pub fn parse_insert_dispatch(parser: &mut Parser) -> ParseResult<DmlStatement> {
    if let Some(Token::Keyword(Keyword::Bulk)) = parser.peek() {
        let _ = parser.next();
        return Ok(DmlStatement::InsertBulk(Box::new(parse_insert_bulk(
            parser,
        )?)));
    }
    Ok(DmlStatement::Insert(Box::new(parse_insert(parser)?)))
}

pub fn parse_insert(parser: &mut Parser) -> ParseResult<InsertStmt> {
    if let Some(Token::Keyword(Keyword::Into)) = parser.peek() {
        let _ = parser.next();
    }
    let table = super::parse_multipart_name(parser)?;

    let mut columns = Vec::new();
    if matches!(parser.peek(), Some(Token::LParen)) {
        let _ = parser.next();
        columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
            if let Some(tok) = p.next() {
                match tok {
                    Token::Identifier(id) => Ok(id.clone()),
                    Token::Keyword(kw) => Ok(kw.as_ref().to_string()),
                    _ => p.backtrack(Expected::Description("column name")),
                }
            } else {
                p.backtrack(Expected::Description("column name"))
            }
        })?;
        parser.expect_rparen()?;
    }

    let mut output = None;
    let mut output_into = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Output))) {
        let _ = parser.next();
        let (out_cols, out_into) = parse_output_clause(parser)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Default))) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Values)?;
        return Ok(InsertStmt {
            table,
            columns,
            source: InsertSource::DefaultValues,
            output,
            output_into,
        });
    }

    let k = match parser.next() {
        Some(Token::Keyword(k)) => *k,
        _ => return parser.backtrack(Expected::Description("VALUES, SELECT, or EXEC")),
    };

    let source = match k {
        Keyword::Values => {
            let rows = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                p.expect_lparen()?;
                let vals = crate::parser::parse::expressions::parse_comma_list(
                    p,
                    crate::parser::parse::expressions::parse_expr,
                )?;
                p.expect_rparen()?;
                Ok(vals)
            })?;
            InsertSource::Values(rows)
        }
        Keyword::Select => InsertSource::Select(Box::new(
            crate::parser::parse::statements::query::parse_select_body(parser)?,
        )),
        Keyword::Exec | Keyword::Execute => {
            let procedure = super::parse_multipart_name(parser)?;
            let args = if !parser.is_empty()
                && !matches!(parser.peek(), Some(Token::Semicolon) | Some(Token::Go))
                && !is_statement_starter(parser.peek())
            {
                crate::parser::parse::expressions::parse_comma_list(
                    parser,
                    crate::parser::parse::expressions::parse_expr,
                )?
            } else {
                Vec::new()
            };
            InsertSource::Exec { procedure, args }
        }
        _ => return parser.backtrack(Expected::Description("VALUES, SELECT, or EXEC")),
    };

    Ok(InsertStmt {
        table,
        columns,
        source,
        output,
        output_into,
    })
}

pub fn parse_update(parser: &mut Parser) -> ParseResult<UpdateStmt> {
    let mut top = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Top))) {
        let _ = parser.next();
        parser.expect_lparen()?;
        top = Some(crate::parser::parse::expressions::parse_expr(parser)?);
        parser.expect_rparen()?;
    }

    let table = crate::parser::parse::statements::query::parse_table_ref(parser)?;
    parser.expect_keyword(Keyword::Set)?;
    let assignments =
        crate::parser::parse::expressions::parse_comma_list(parser, parse_update_assignment)?;

    let mut output = None;
    let mut output_into = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Output))) {
        let _ = parser.next();
        let (out_cols, out_into) = parse_output_clause(parser)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    let mut from = None;
    let mut joins = Vec::new();
    if let Some(Token::Keyword(Keyword::From)) = parser.peek() {
        let _ = parser.next();
        from = Some(crate::parser::parse::expressions::parse_comma_list(
            parser,
            crate::parser::parse::statements::query::parse_table_ref,
        )?);
    }

    while let Some(join) = crate::parser::parse::statements::query::parse_join_clause(parser)? {
        joins.push(join);
    }

    let mut selection = None;
    if let Some(Token::Keyword(Keyword::Where)) = parser.peek() {
        let _ = parser.next();
        selection = Some(crate::parser::parse::expressions::parse_expr(parser)?);
    }

    Ok(UpdateStmt {
        table,
        assignments,
        top,
        from,
        joins,
        selection,
        output,
        output_into,
    })
}

fn parse_update_assignment(parser: &mut Parser) -> ParseResult<UpdateAssignment> {
    let parts = super::parse_multipart_name(parser)?;
    let column = parts
        .last()
        .cloned()
        .ok_or_else(|| parser.error(Expected::Description("column name")))?;
    if let Some(Token::Operator(op)) = parser.next() {
        if *op != "=" {
            return parser.backtrack(Expected::Description("="));
        }
    } else {
        return parser.backtrack(Expected::Description("="));
    }
    let expr = crate::parser::parse::expressions::parse_expr(parser)?;
    Ok(UpdateAssignment { column, expr })
}

pub fn parse_delete(parser: &mut Parser) -> ParseResult<DeleteStmt> {
    let mut top = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Top))) {
        let _ = parser.next();
        parser.expect_lparen()?;
        top = Some(crate::parser::parse::expressions::parse_expr(parser)?);
        parser.expect_rparen()?;
    }

    if matches!(parser.peek(), Some(Token::Keyword(Keyword::From))) {
        let _ = parser.next();
    }

    let target = crate::parser::parse::statements::query::parse_table_ref(parser)?;
    let table = target
        .name_as_object()
        .map(|name| {
            let mut parts = Vec::new();
            if let Some(schema) = &name.schema {
                parts.push(schema.clone());
            }
            parts.push(name.name.clone());
            parts
        })
        .ok_or_else(|| parser.error(Expected::Description("table name")))?;

    let mut from = vec![target.clone()];
    let mut joins = Vec::new();
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::From))) {
        let _ = parser.next();
        from.extend(crate::parser::parse::expressions::parse_comma_list(
            parser,
            crate::parser::parse::statements::query::parse_table_ref,
        )?);
    }

    while let Some(join) = crate::parser::parse::statements::query::parse_join_clause(parser)? {
        joins.push(join);
    }

    let mut output = None;
    let mut output_into = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Output))) {
        let _ = parser.next();
        let (out_cols, out_into) = parse_output_clause(parser)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    let mut selection = None;
    if let Some(Token::Keyword(Keyword::Where)) = parser.peek() {
        let _ = parser.next();
        selection = Some(crate::parser::parse::expressions::parse_expr(parser)?);
    }

    Ok(DeleteStmt {
        table,
        top,
        from,
        joins,
        selection,
        output,
        output_into,
    })
}

pub fn parse_output_clause(
    parser: &mut Parser,
) -> ParseResult<(Vec<OutputColumn>, Option<Vec<String>>)> {
    let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
        let source = match p.peek() {
            Some(Token::Keyword(Keyword::Inserted)) => {
                let _ = p.next();
                OutputSource::Inserted
            }
            Some(Token::Keyword(Keyword::Deleted)) => {
                let _ = p.next();
                OutputSource::Deleted
            }
            Some(Token::Identifier(k)) if k.eq_ignore_ascii_case("INSERTED") => {
                let _ = p.next();
                OutputSource::Inserted
            }
            Some(Token::Identifier(k)) if k.eq_ignore_ascii_case("DELETED") => {
                let _ = p.next();
                OutputSource::Deleted
            }
            _ => return p.backtrack(Expected::Description("INSERTED or DELETED")),
        };
        if !matches!(p.peek(), Some(Token::Dot)) {
            return p.backtrack(Expected::Description("."));
        }
        let _ = p.next();
        let (column, is_wildcard) = if matches!(p.peek(), Some(Token::Star)) {
            let _ = p.next();
            ("*".to_string(), true)
        } else {
            match p.next() {
                Some(Token::Identifier(id)) => (id.clone(), false),
                Some(Token::Keyword(kw)) => (kw.as_ref().to_string(), false),
                _ => return p.backtrack(Expected::Description("column name")),
            }
        };
        let alias = if let Some(Token::Keyword(Keyword::As)) = p.peek() {
            let _ = p.next();
            match p.next() {
                Some(Token::Identifier(id)) => Some(id.clone()),
                Some(Token::Keyword(kw)) => Some(kw.as_ref().to_string()),
                _ => return p.backtrack(Expected::Description("alias")),
            }
        } else {
            None
        };
        Ok(OutputColumn {
            source,
            column,
            alias,
            is_wildcard,
        })
    })?;
    let mut output_into = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Into))) {
        let _ = parser.next();
        output_into = Some(super::parse_multipart_name(parser)?);
    }
    Ok((columns, output_into))
}

pub fn parse_bulk_insert(parser: &mut Parser) -> ParseResult<BulkInsertStmt> {
    let table = super::parse_multipart_name(parser)?;
    parser.expect_keyword(Keyword::From)?;
    let from = match parser.next() {
        Some(Token::String(s)) | Some(Token::NString(s)) => s.clone(),
        _ => return parser.backtrack(Expected::Description("file path string")),
    };

    let mut options = Vec::new();
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::With))) {
        let _ = parser.next();
        parser.expect_lparen()?;
        options = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
            let opt_name = if let Some(tok) = p.next() {
                match tok {
                    Token::Identifier(id) => id.clone(),
                    Token::Keyword(kw) => kw.as_ref().to_string(),
                    _ => return p.backtrack(Expected::Description("option name")),
                }
            } else {
                return p.backtrack(Expected::Description("option name"));
            };

            match opt_name.to_uppercase().as_str() {
                "CHECK_CONSTRAINTS" => Ok(BulkInsertOption::CheckConstraints),
                "FIRE_TRIGGERS" => Ok(BulkInsertOption::FireTriggers),
                "KEEPIDENTITY" => Ok(BulkInsertOption::KeepIdentity),
                "KEEPNULLS" => Ok(BulkInsertOption::KeepNulls),
                "TABLOCK" => Ok(BulkInsertOption::TabLock),
                "FORMAT" => {
                    if let Some(Token::Operator(op)) = p.next() {
                        if op != "=" {
                            return p.backtrack(Expected::Description("="));
                        }
                    }
                    match p.next() {
                        Some(Token::String(s)) | Some(Token::NString(s)) => {
                            Ok(BulkInsertOption::Format(s.clone()))
                        }
                        _ => p.backtrack(Expected::Description("format string")),
                    }
                }
                "DATA_SOURCE" => {
                    // Placeholder for DATA_SOURCE, we'll just skip it for now or handle as part of FORMAT
                    if let Some(Token::Operator(op)) = p.next() {
                        if op != "=" {
                            return p.backtrack(Expected::Description("="));
                        }
                    }
                    let _ = p.next(); // skip value
                    Ok(BulkInsertOption::Format("DATA_SOURCE".to_string()))
                }
                "DATAFILETYPE" => {
                    if let Some(Token::Operator(op)) = p.next() {
                        if op != "=" {
                            return p.backtrack(Expected::Description("="));
                        }
                    }
                    match p.next() {
                        Some(Token::String(s)) | Some(Token::NString(s)) => {
                            Ok(BulkInsertOption::DataFiletype(s.clone()))
                        }
                        _ => p.backtrack(Expected::Description("datafiletype string")),
                    }
                }
                "FIELDTERMINATOR" => {
                    if let Some(Token::Operator(op)) = p.next() {
                        if op != "=" {
                            return p.backtrack(Expected::Description("="));
                        }
                    }
                    match p.next() {
                        Some(Token::String(s)) | Some(Token::NString(s)) => {
                            Ok(BulkInsertOption::FieldTerminator(s.clone()))
                        }
                        _ => p.backtrack(Expected::Description("fieldterminator string")),
                    }
                }
                "ROWTERMINATOR" => {
                    if let Some(Token::Operator(op)) = p.next() {
                        if op != "=" {
                            return p.backtrack(Expected::Description("="));
                        }
                    }
                    match p.next() {
                        Some(Token::String(s)) | Some(Token::NString(s)) => {
                            Ok(BulkInsertOption::RowTerminator(s.clone()))
                        }
                        _ => p.backtrack(Expected::Description("rowterminator string")),
                    }
                }
                "FIRSTROW" => {
                    if let Some(Token::Operator(op)) = p.next() {
                        if op != "=" {
                            return p.backtrack(Expected::Description("="));
                        }
                    }
                    match p.next() {
                        Some(Token::Number { value, .. }) => {
                            Ok(BulkInsertOption::FirstRow(*value as i64))
                        }
                        _ => p.backtrack(Expected::Description("firstrow number")),
                    }
                }
                "LASTROW" => {
                    if let Some(Token::Operator(op)) = p.next() {
                        if op != "=" {
                            return p.backtrack(Expected::Description("="));
                        }
                    }
                    match p.next() {
                        Some(Token::Number { value, .. }) => {
                            Ok(BulkInsertOption::LastRow(*value as i64))
                        }
                        _ => p.backtrack(Expected::Description("lastrow number")),
                    }
                }
                "ERRORFILE" => {
                    if let Some(Token::Operator(op)) = p.next() {
                        if op != "=" {
                            return p.backtrack(Expected::Description("="));
                        }
                    }
                    match p.next() {
                        Some(Token::String(s)) | Some(Token::NString(s)) => {
                            Ok(BulkInsertOption::ErrorFile(s.clone()))
                        }
                        _ => p.backtrack(Expected::Description("errorfile string")),
                    }
                }
                _ => p.backtrack(Expected::Description("supported BULK INSERT option")),
            }
        })?;
        parser.expect_rparen()?;
    }

    Ok(BulkInsertStmt {
        table,
        from,
        options,
    })
}

pub fn parse_insert_bulk(parser: &mut Parser) -> ParseResult<InsertBulkStmt> {
    let table = super::parse_multipart_name(parser)?;
    parser.expect_lparen()?;
    let columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
        let name = if let Some(tok) = p.next() {
            match tok {
                Token::Identifier(id) => id.clone(),
                Token::Keyword(kw) => kw.as_ref().to_string(),
                _ => return p.backtrack(Expected::Description("column name")),
            }
        } else {
            return p.backtrack(Expected::Description("column name"));
        };
        let data_type = crate::parser::parse::expressions::parse_data_type(p)?;
        // Handle optional NULL/NOT NULL
        let mut is_nullable = None;
        if matches!(p.peek(), Some(Token::Keyword(Keyword::Null))) {
            let _ = p.next();
            is_nullable = Some(true);
        } else if matches!(p.peek(), Some(Token::Keyword(Keyword::Not))) {
            let _ = p.next();
            p.expect_keyword(Keyword::Null)?;
            is_nullable = Some(false);
        }

        Ok(ColumnDef {
            name,
            data_type,
            is_nullable,
            is_identity: false,
            identity_spec: None,
            is_primary_key: false,
            is_unique: false,
            default_expr: None,
            default_constraint_name: None,
            check_expr: None,
            check_constraint_name: None,
            computed_expr: None,
            foreign_key: None,
            collation: None,
            is_clustered: false,
        })
    })?;
    parser.expect_rparen()?;

    Ok(InsertBulkStmt { table, columns })
}

pub fn parse_merge(parser: &mut Parser) -> ParseResult<MergeStmt> {
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Into))) {
        let _ = parser.next();
    }
    let target = crate::parser::parse::statements::query::parse_table_ref(parser)?;
    parser.expect_keyword(Keyword::Using)?;
    let source = crate::parser::parse::statements::query::parse_table_ref(parser)?;
    parser.expect_keyword(Keyword::On)?;
    let on_condition = crate::parser::parse::expressions::parse_expr(parser)?;

    let mut when_clauses = Vec::new();
    while let Some(Token::Keyword(Keyword::When)) = parser.peek() {
        let _ = parser.next();

        let when = if matches!(parser.peek(), Some(Token::Keyword(Keyword::Matched))) {
            let _ = parser.next();
            MergeWhen::Matched
        } else if matches!(parser.peek(), Some(Token::Keyword(Keyword::Not))) {
            let _ = parser.next();
            parser.expect_keyword(Keyword::Matched)?;
            if matches!(parser.peek(), Some(Token::Keyword(Keyword::By))) {
                let _ = parser.next();
                parser.expect_keyword(Keyword::Source)?;
                MergeWhen::NotMatchedBySource
            } else {
                MergeWhen::NotMatched
            }
        } else {
            return parser.backtrack(Expected::Description("MATCHED or NOT MATCHED"));
        };

        let mut condition = None;
        if matches!(parser.peek(), Some(Token::Keyword(Keyword::And))) {
            let _ = parser.next();
            condition = Some(crate::parser::parse::expressions::parse_expr(parser)?);
        }

        parser.expect_keyword(Keyword::Then)?;

        let action = match parser.peek() {
            Some(Token::Keyword(Keyword::Update)) => {
                let _ = parser.next();
                parser.expect_keyword(Keyword::Set)?;
                let assignments = crate::parser::parse::expressions::parse_comma_list(
                    parser,
                    parse_update_assignment,
                )?;
                MergeAction::Update { assignments }
            }
            Some(Token::Keyword(Keyword::Delete)) => {
                let _ = parser.next();
                MergeAction::Delete
            }
            Some(Token::Keyword(Keyword::Insert)) => {
                let _ = parser.next();
                let mut columns = Vec::new();
                if matches!(parser.peek(), Some(Token::LParen)) {
                    let _ = parser.next();
                    columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                        if let Some(tok) = p.next() {
                            match tok {
                                Token::Identifier(id) => Ok(id.clone()),
                                Token::Keyword(kw) => Ok(kw.as_ref().to_string()),
                                _ => p.backtrack(Expected::Description("column name")),
                            }
                        } else {
                            p.backtrack(Expected::Description("column name"))
                        }
                    })?;
                    parser.expect_rparen()?;
                }
                parser.expect_keyword(Keyword::Values)?;
                parser.expect_lparen()?;
                let values = crate::parser::parse::expressions::parse_comma_list(
                    parser,
                    crate::parser::parse::expressions::parse_expr,
                )?;
                parser.expect_rparen()?;
                MergeAction::Insert { columns, values }
            }
            _ => return parser.backtrack(Expected::Description("UPDATE, DELETE, or INSERT")),
        };

        when_clauses.push(MergeWhenClause {
            when,
            condition,
            action,
        });
    }

    let mut output = None;
    let mut output_into = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Output))) {
        let _ = parser.next();
        let (out_cols, out_into) = parse_output_clause(parser)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    Ok(MergeStmt {
        target,
        source,
        on_condition,
        when_clauses,
        output,
        output_into,
    })
}
