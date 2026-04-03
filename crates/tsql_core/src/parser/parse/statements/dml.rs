use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseError, ParseResult, Expected};
use std::borrow::Cow;

pub fn parse_insert<'a>(parser: &mut Parser<'a>) -> ParseResult<InsertStmt<'a>> {
    if let Some(Token::Keyword(Keyword::Into)) = parser.peek() {
        let _ = parser.next();
    }
    let table = parse_multipart_name(parser)?;
    
    let mut columns = Vec::new();
    if matches!(parser.peek(), Some(Token::LParen)) {
        let _ = parser.next();
        columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
            if let Some(tok) = p.next() {
                match tok {
                    Token::Identifier(id) => Ok(id.clone()),
                    Token::Keyword(kw) => Ok(Cow::Owned(kw.as_ref().to_string())),
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
        return Ok(InsertStmt { table, columns, source: InsertSource::DefaultValues, output, output_into });
    }

    let k = match parser.next() {
        Some(Token::Keyword(k)) => *k,
        _ => return parser.backtrack(Expected::Description("VALUES, SELECT, or EXEC")),
    };

    let source = match k {
        Keyword::Values => {
                let rows = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                    p.expect_lparen()?;
                    let vals = crate::parser::parse::expressions::parse_comma_list(p, crate::parser::parse::expressions::parse_expr)?;
                    p.expect_rparen()?;
                    Ok(vals)
                })?;
             InsertSource::Values(rows)
        }
        Keyword::Select => {
             InsertSource::Select(Box::new(crate::parser::parse::statements::query::parse_select_body(parser)?))
        }
        Keyword::Exec | Keyword::Execute => {
             let procedure = parse_multipart_name(parser)?;
             let args = if !parser.is_empty() && !matches!(parser.peek(), Some(Token::Semicolon)) {
                 crate::parser::parse::expressions::parse_comma_list(parser, crate::parser::parse::expressions::parse_expr)?
             } else {
                 Vec::new()
             };
             InsertSource::Exec { procedure, args }
        }
        _ => return parser.backtrack(Expected::Description("VALUES, SELECT, or EXEC")),
    };

    Ok(InsertStmt { table, columns, source, output, output_into })
}

pub fn parse_update<'a>(parser: &mut Parser<'a>) -> ParseResult<UpdateStmt<'a>> {
    let mut top = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Top))) {
        let _ = parser.next();
        parser.expect_lparen()?;
        top = Some(crate::parser::parse::expressions::parse_expr(parser)?);
        parser.expect_rparen()?;
    }

    let table = crate::parser::parse::statements::query::parse_table_ref(parser)?;
    parser.expect_keyword(Keyword::Set)?;
    let assignments = crate::parser::parse::expressions::parse_comma_list(parser, parse_update_assignment)?;
    
    let mut output = None;
    let mut output_into = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Output))) {
        let _ = parser.next();
        let (out_cols, out_into) = parse_output_clause(parser)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    let mut from = None;
    if let Some(Token::Keyword(Keyword::From)) = parser.peek() {
        let _ = parser.next();
        from = Some(crate::parser::parse::expressions::parse_comma_list(parser, crate::parser::parse::statements::query::parse_table_ref)?);
    }
    
    let mut selection = None;
    if let Some(Token::Keyword(Keyword::Where)) = parser.peek() {
        let _ = parser.next();
        selection = Some(crate::parser::parse::expressions::parse_expr(parser)?);
    }
    
    Ok(UpdateStmt { table, assignments, top, from, selection, output, output_into })
}

fn parse_update_assignment<'a>(parser: &mut Parser<'a>) -> ParseResult<UpdateAssignment<'a>> {
    let parts = parse_multipart_name(parser)?;
    let column = parts.last().unwrap().clone();
    if let Some(Token::Operator(op)) = parser.next() {
        if op.as_ref() != "=" {
             return parser.backtrack(Expected::Description("="));
        }
    } else {
        return parser.backtrack(Expected::Description("="));
    }
    let expr = crate::parser::parse::expressions::parse_expr(parser)?;
    Ok(UpdateAssignment { column, expr })
}

pub fn parse_delete<'a>(parser: &mut Parser<'a>) -> ParseResult<DeleteStmt<'a>> {
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
    let table = match &target {
        TableRef::Table { name, .. } => name.clone(),
        _ => return parser.backtrack(Expected::Description("table name")),
    };

    let mut from = vec![target.clone()];
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::From))) {
        let _ = parser.next();
        from.extend(crate::parser::parse::expressions::parse_comma_list(
            parser,
            crate::parser::parse::statements::query::parse_table_ref,
        )?);
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
    
    Ok(DeleteStmt { table, top, from, selection, output, output_into })
}

pub fn parse_output_clause<'a>(parser: &mut Parser<'a>) -> ParseResult<(Vec<OutputColumn<'a>>, Option<Vec<Cow<'a, str>>>)> {
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
            (Cow::Borrowed("*"), true)
        } else {
            match p.next() {
                Some(Token::Identifier(id)) => (id.clone(), false),
                Some(Token::Keyword(kw)) => (Cow::Owned(kw.as_ref().to_string()), false),
                _ => return p.backtrack(Expected::Description("column name")),
            }
        };
        let alias = if let Some(Token::Keyword(Keyword::As)) = p.peek() {
            let _ = p.next();
            match p.next() {
                Some(Token::Identifier(id)) => Some(id.clone()),
                Some(Token::Keyword(kw)) => Some(Cow::Owned(kw.as_ref().to_string())),
                _ => return p.backtrack(Expected::Description("alias")),
            }
        } else {
            None
        };
        Ok(OutputColumn { source, column, alias, is_wildcard })
    })?;
    let mut output_into = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Into))) {
        let _ = parser.next();
        output_into = Some(parse_multipart_name(parser)?);
    }
    Ok((columns, output_into))
}

pub fn parse_merge<'a>(parser: &mut Parser<'a>) -> ParseResult<MergeStmt<'a>> {
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
                let assignments = crate::parser::parse::expressions::parse_comma_list(parser, parse_update_assignment)?;
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
                                Token::Keyword(kw) => Ok(Cow::Owned(kw.as_ref().to_string())),
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
                let values = crate::parser::parse::expressions::parse_comma_list(parser, crate::parser::parse::expressions::parse_expr)?;
                parser.expect_rparen()?;
                MergeAction::Insert { columns, values }
            }
            _ => return parser.backtrack(Expected::Description("UPDATE, DELETE, or INSERT")),
        };

        when_clauses.push(MergeWhenClause { when, condition, action });
    }

    let mut output = None;
    let mut output_into = None;
    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Output))) {
        let _ = parser.next();
        let (out_cols, out_into) = parse_output_clause(parser)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    Ok(MergeStmt { target, source, on_condition, when_clauses, output, output_into })
}

fn parse_multipart_name<'a>(parser: &mut Parser<'a>) -> ParseResult<Vec<Cow<'a, str>>> {
    crate::parser::parse::statements::query::parse_multipart_name(parser)
}
