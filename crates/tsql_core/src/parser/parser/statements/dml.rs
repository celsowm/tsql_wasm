use crate::parser::ast::*;
use crate::parser::parser::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};
use std::borrow::Cow;

pub fn parse_insert<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<InsertStmt<'a>> {
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("INTO") {
            let _ = next_token(input);
        }
    }
    let table = multipart_name(input)?;
    
    let mut columns = Vec::new();
    if matches!(peek_token(input), Some(Token::LParen)) {
        let _ = next_token(input);
        columns = parse_comma_list(input, |input| {
            if let Some(tok) = next_token(input) {
                match tok {
                    Token::Identifier(id) | Token::Keyword(id) => Ok(id.clone()),
                    _ => Err(ErrMode::Backtrack(ContextError::new())),
                }
            } else {
                Err(ErrMode::Backtrack(ContextError::new()))
            }
        })?;
        expect_punctuation(input, Token::RParen)?;
    }

    let mut output = None;
    let mut output_into = None;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OUTPUT")) {
        let _ = next_token(input);
        let (out_cols, out_into) = parse_output_clause(input)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("DEFAULT")) {
        let _ = next_token(input);
        expect_keyword(input, "VALUES")?;
        return Ok(InsertStmt { table, columns, source: InsertSource::DefaultValues, output, output_into });
    }

    let k = match next_token(input) {
        Some(Token::Keyword(k)) => k.to_uppercase(),
        _ => return Err(ErrMode::Backtrack(ContextError::new())),
    };

    let source = match k.as_str() {
        "VALUES" => {
             let rows = parse_comma_list(input, |input| {
                 expect_punctuation(input, Token::LParen)?;
                 let vals = parse_comma_list(input, parse_expr)?;
                 expect_punctuation(input, Token::RParen)?;
                 Ok(vals)
             })?;
             InsertSource::Values(rows)
        }
        "SELECT" => {
             InsertSource::Select(Box::new(parse_select_body(input)?))
        }
        "EXEC" | "EXECUTE" => {
             let procedure = multipart_name(input)?;
             let args = if !input.is_empty() && !matches!(peek_token(input), Some(Token::Semicolon)) {
                 parse_comma_list(input, parse_expr)?
             } else {
                 Vec::new()
             };
             InsertSource::Exec { procedure, args }
        }
        _ => return Err(ErrMode::Backtrack(ContextError::new())),
    };

    Ok(InsertStmt { table, columns, source, output, output_into })
}

pub fn parse_update<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<UpdateStmt<'a>> {
    let mut top = None;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("TOP")) {
        let _ = next_token(input);
        expect_punctuation(input, Token::LParen)?;
        top = Some(parse_expr(input)?);
        expect_punctuation(input, Token::RParen)?;
    }

    let table = parse_table_ref(input)?;
    expect_keyword(input, "SET")?;
    let assignments = parse_comma_list(input, parse_update_assignment)?;
    
    let mut output = None;
    let mut output_into = None;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OUTPUT")) {
        let _ = next_token(input);
        let (out_cols, out_into) = parse_output_clause(input)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    let mut from = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("FROM") {
            let _ = next_token(input);
            from = Some(parse_comma_list(input, parse_table_ref)?);
        }
    }
    
    let mut selection = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("WHERE") {
            let _ = next_token(input);
            selection = Some(parse_expr(input)?);
        }
    }
    
    Ok(UpdateStmt { table, assignments, top, from, selection, output, output_into })
}

fn parse_update_assignment<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<UpdateAssignment<'a>> {
    let parts = multipart_name(input)?;
    let column = parts.last().unwrap().clone();
    if let Some(Token::Operator(op)) = next_token(input) {
        if op.as_ref() != "=" {
             return Err(ErrMode::Backtrack(ContextError::new()));
        }
    } else {
        return Err(ErrMode::Backtrack(ContextError::new()));
    }
    let expr = parse_expr(input)?;
    Ok(UpdateAssignment { column, expr })
}

pub fn parse_delete<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<DeleteStmt<'a>> {
    let mut top = None;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("TOP")) {
        let _ = next_token(input);
        expect_punctuation(input, Token::LParen)?;
        top = Some(parse_expr(input)?);
        expect_punctuation(input, Token::RParen)?;
    }

    let mut target_alias = None;
    if !matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("FROM")) {
        if let Some(Token::Identifier(id)) = next_token(input) {
            target_alias = Some(id.clone());
        }
    }
    
    expect_keyword(input, "FROM")?;
    let from = parse_comma_list(input, parse_table_ref)?;
    
    let mut output = None;
    let mut output_into = None;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OUTPUT")) {
        let _ = next_token(input);
        let (out_cols, out_into) = parse_output_clause(input)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    let mut selection = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("WHERE") {
            let _ = next_token(input);
            selection = Some(parse_expr(input)?);
        }
    }
    
    Ok(DeleteStmt { target_alias, top, from, selection, output, output_into })
}

pub fn parse_output_clause<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<(Vec<OutputColumn<'a>>, Option<Vec<Cow<'a, str>>>)> {
    let columns = parse_comma_list(input, |i| {
        let source = match peek_token(i) {
            Some(Token::Keyword(k) | Token::Identifier(k)) if k.eq_ignore_ascii_case("INSERTED") => {
                let _ = next_token(i);
                OutputSource::Inserted
            }
            Some(Token::Keyword(k) | Token::Identifier(k)) if k.eq_ignore_ascii_case("DELETED") => {
                let _ = next_token(i);
                OutputSource::Deleted
            }
            _ => return Err(ErrMode::Backtrack(ContextError::new())),
        };
        if !matches!(peek_token(i), Some(Token::Dot)) {
            return Err(ErrMode::Backtrack(ContextError::new()));
        }
        let _ = next_token(i);
        let (column, is_wildcard) = if matches!(peek_token(i), Some(Token::Star)) {
            let _ = next_token(i);
            (Cow::Borrowed("*"), true)
        } else {
            match next_token(i) {
                Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => (id.clone(), false),
                _ => return Err(ErrMode::Backtrack(ContextError::new())),
            }
        };
        let alias = if let Some(Token::Keyword(k)) = peek_token(i) {
            if k.eq_ignore_ascii_case("AS") {
                let _ = next_token(i);
                match next_token(i) {
                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Some(id.clone()),
                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                }
            } else {
                None
            }
        } else {
            None
        };
        Ok(OutputColumn { source, column, alias, is_wildcard })
    })?;
    let mut output_into = None;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("INTO")) {
        let _ = next_token(input);
        output_into = Some(multipart_name(input)?);
    }
    Ok((columns, output_into))
}

pub fn parse_merge<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<MergeStmt<'a>> {
    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("INTO")) {
        let _ = next_token(input);
    }
    let target = parse_table_ref(input)?;
    let _ = expect_keyword(input, "USING")?;
    let source = parse_table_ref(input)?;
    let _ = expect_keyword(input, "ON")?;
    let on_condition = parse_expr(input)?;

    let mut when_clauses = Vec::new();
    while let Some(Token::Keyword(kw)) = peek_token(input) {
        if !kw.eq_ignore_ascii_case("WHEN") { break; }
        let _ = next_token(input);

        let when = if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("MATCHED")) {
            let _ = next_token(input);
            MergeWhen::Matched
        } else if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("NOT")) {
            let _ = next_token(input);
            expect_keyword(input, "MATCHED")?;
            if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("BY")) {
                let _ = next_token(input);
                expect_keyword(input, "SOURCE")?;
                MergeWhen::NotMatchedBySource
            } else {
                MergeWhen::NotMatched
            }
        } else {
             return Err(ErrMode::Backtrack(ContextError::new()));
        };

        let mut condition = None;
        if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("AND")) {
            let _ = next_token(input);
            condition = Some(parse_expr(input)?);
        }

        let _ = expect_keyword(input, "THEN")?;

        let action = match peek_token(input) {
            Some(tok) if tok.eq_ignore_ascii_case("UPDATE") => {
                let _ = next_token(input);
                let _ = expect_keyword(input, "SET")?;
                let assignments = parse_comma_list(input, parse_update_assignment)?;
                MergeAction::Update { assignments }
            }
            Some(tok) if tok.eq_ignore_ascii_case("DELETE") => {
                let _ = next_token(input);
                MergeAction::Delete
            }
            Some(tok) if tok.eq_ignore_ascii_case("INSERT") => {
                let _ = next_token(input);
                let mut columns = Vec::new();
                if matches!(peek_token(input), Some(Token::LParen)) {
                    let _ = next_token(input);
                    columns = parse_comma_list(input, |i| {
                        if let Some(tok) = next_token(i) {
                            match tok {
                                Token::Identifier(id) | Token::Keyword(id) => Ok(id.clone()),
                                _ => Err(ErrMode::Backtrack(ContextError::new())),
                            }
                        } else {
                            Err(ErrMode::Backtrack(ContextError::new()))
                        }
                    })?;
                    expect_punctuation(input, Token::RParen)?;
                }
                expect_keyword(input, "VALUES")?;
                expect_punctuation(input, Token::LParen)?;
                let values = parse_comma_list(input, parse_expr)?;
                expect_punctuation(input, Token::RParen)?;
                MergeAction::Insert { columns, values }
            }
            _ => return Err(ErrMode::Backtrack(ContextError::new())),
        };

        when_clauses.push(MergeWhenClause { when, condition, action });
    }

    let mut output = None;
    let mut output_into = None;
    if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OUTPUT")) {
        let _ = next_token(input);
        let (out_cols, out_into) = parse_output_clause(input)?;
        output = Some(out_cols);
        output_into = out_into;
    }

    Ok(MergeStmt { target, source, on_condition, when_clauses, output, output_into })
}
