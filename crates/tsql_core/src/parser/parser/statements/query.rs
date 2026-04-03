use crate::parser::ast::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};

pub fn parse_select<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<SelectStmt<'a>> {
    let _ = expect_keyword(input, "SELECT")?;
    parse_select_body(input)
}

pub fn parse_select_body<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<SelectStmt<'a>> {
    let mut current = parse_single_select_body(input)?;

    loop {
        let kind = match peek_token(input) {
            Some(Token::Keyword(k)) => match k.to_uppercase().as_str() {
                "UNION" => {
                    let _ = next_token(input);
                    if matches!(peek_token(input), Some(Token::Keyword(k2)) if k2.eq_ignore_ascii_case("ALL")) {
                        let _ = next_token(input);
                        SetOpKind::UnionAll
                    } else {
                        SetOpKind::Union
                    }
                }
                "INTERSECT" => {
                    let _ = next_token(input);
                    SetOpKind::Intersect
                }
                "EXCEPT" => {
                    let _ = next_token(input);
                    SetOpKind::Except
                }
                _ => break,
            },
            _ => break,
        };

        let right = parse_single_select(input)?;
        
        let mut target = &mut current;
        while let Some(ref mut op) = target.set_op {
            target = &mut op.right;
        }
        target.set_op = Some(Box::new(SetOp { kind, right }));
    }

    Ok(current)
}

fn parse_single_select<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<SelectStmt<'a>> {
    let _ = expect_keyword(input, "SELECT")?;
    parse_single_select_body(input)
}

pub fn parse_single_select_body<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<SelectStmt<'a>> {
    let mut distinct = false;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("DISTINCT") {
            let _ = next_token(input);
            distinct = true;
        }
    }

    let mut top = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("TOP") {
            let _ = next_token(input);
            // Parse the value after TOP - must be a primary expression (number, variable, etc.)
            // Don't use parse_expr because that would consume trailing operators like *
            top = Some(crate::parser::parser::expressions::parse_primary(input)?);
        }
    }

    let projection = parse_projection(input)?;

    let mut into_table = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("INTO") {
            let _ = next_token(input);
            into_table = Some(parse_multipart_name(input)?);
        }
    }

    let mut from = None;
    if let Some(tok) = peek_token(input) {
        let is_from = match tok {
            Token::Keyword(k) => k.eq_ignore_ascii_case("FROM"),
            Token::Identifier(id) => id.eq_ignore_ascii_case("FROM"),
            _ => false,
        };
        if is_from {
            let _ = next_token(input);
            from = Some(parse_comma_list(input, parse_table_ref)?);
        }
    }

    let mut applies = Vec::new();
    loop {
        if let Some(Token::Keyword(k)) = peek_token(input) {
            let k_upper = k.to_uppercase();
            let apply_type = match k_upper.as_str() {
                "CROSS" => {
                    let saved = *input;
                    let _ = next_token(input);
                    if matches!(peek_token(input), Some(Token::Keyword(k2)) if k2.eq_ignore_ascii_case("APPLY")) {
                        let _ = next_token(input);
                        ApplyType::Cross
                    } else {
                        *input = saved;
                        break;
                    }
                }
                "OUTER" => {
                    let saved = *input;
                    let _ = next_token(input);
                    if matches!(peek_token(input), Some(Token::Keyword(k2)) if k2.eq_ignore_ascii_case("APPLY")) {
                        let _ = next_token(input);
                        ApplyType::Outer
                    } else {
                        *input = saved;
                        break;
                    }
                }
                _ => break,
            };
            expect_punctuation(input, Token::LParen)?;
            let subquery = Box::new(parse_select(input)?);
            expect_punctuation(input, Token::RParen)?;
            let alias = if let Some(Token::Keyword(k)) = peek_token(input) {
                if k.eq_ignore_ascii_case("AS") {
                    let _ = next_token(input);
                }
                match next_token(input) {
                    Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                }
            } else if let Some(Token::Identifier(id)) = peek_token(input) {
                let id = id.clone();
                let _ = next_token(input);
                id
            } else {
                return Err(ErrMode::Backtrack(ContextError::new()));
            };
            applies.push(ApplyClause { apply_type, subquery, alias });
        } else {
            break;
        }
    }

    let mut selection = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("WHERE") {
            let _ = next_token(input);
            selection = Some(parse_expr(input)?);
        }
    }

    let mut group_by = Vec::new();
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("GROUP") {
            let _ = next_token(input);
            if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("BY")) {
                let _ = next_token(input);
            }
            group_by = parse_comma_list(input, parse_expr)?;
        }
    }

    let mut having = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("HAVING") {
            let _ = next_token(input);
            having = Some(parse_expr(input)?);
        }
    }

    let mut order_by = Vec::new();
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("ORDER") {
            let _ = next_token(input);
            if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("BY")) {
                let _ = next_token(input);
            }
            order_by = parse_comma_list(input, parse_order_by_expr)?;
        }
    }

    let mut offset = None;
    let mut fetch = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("OFFSET") {
            let _ = next_token(input);
            offset = Some(parse_expr(input)?);
            expect_keyword(input, "ROWS")?;
            if let Some(Token::Keyword(k)) = peek_token(input) {
                if k.eq_ignore_ascii_case("FETCH") {
                    let _ = next_token(input);
                    expect_keyword(input, "NEXT")?;
                    fetch = Some(parse_expr(input)?);
                    expect_keyword(input, "ROWS")?;
                    expect_keyword(input, "ONLY")?;
                }
            }
        }
    }

    Ok(SelectStmt {
        distinct,
        top,
        projection,
        into_table,
        from,
        applies,
        selection,
        group_by,
        having,
        order_by,
        offset,
        fetch,
        set_op: None,
    })
}

fn parse_projection<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Vec<SelectItem<'a>>> {
    let mut items = Vec::new();
    loop {
        items.push(parse_select_item(input)?);
        if let Some(Token::Comma) = peek_token(input) {
            let _ = next_token(input);
            continue;
        }
        if let Some(Token::Keyword(k)) = peek_token(input) {
            let k_upper = k.to_uppercase();
            if matches!(k_upper.as_str(), "FROM" | "INTO" | "WHERE" | "GROUP" | "ORDER" | "HAVING" | "UNION" | "INTERSECT" | "EXCEPT") {
                break;
            }
        }
        break;
    }
    Ok(items)
}

pub fn parse_table_ref<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<TableRef<'a>> {
    let mut current = match peek_token(input) {
        Some(Token::LParen) => {
            let _ = next_token(input);
            let subquery = Box::new(parse_select(input)?);
            expect_punctuation(input, Token::RParen)?;
            let alias = if let Some(tok) = peek_token(input) {
                match tok {
                    Token::Keyword(k) if k.eq_ignore_ascii_case("AS") => {
                        let _ = next_token(input);
                        match next_token(input) {
                            Some(Token::Identifier(alias)) => alias.clone(),
                            _ => return Err(ErrMode::Backtrack(ContextError::new())),
                        }
                    }
                    Token::Keyword(k) => k.clone(),
                    Token::Identifier(id) => id.clone(),
                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                }
            } else {
                return Err(ErrMode::Backtrack(ContextError::new()));
            };
            TableRef::Subquery { subquery, alias }
        }
        Some(Token::Identifier(_)) | Some(Token::Keyword(_)) | Some(Token::Variable(_)) => {
            let name = parse_multipart_name(input)?;
            // Check if this is a table-valued function call
            if matches!(peek_token(input), Some(Token::LParen)) {
                let _ = next_token(input);
                let args = parse_comma_list(input, parse_expr)?;
                expect_punctuation(input, Token::RParen)?;
                let alias = if let Some(Token::Keyword(k)) = peek_token(input) {
                    if k.eq_ignore_ascii_case("AS") {
                        let _ = next_token(input);
                        match next_token(input) {
                            Some(Token::Identifier(alias)) => Some(alias.clone()),
                            _ => return Err(ErrMode::Backtrack(ContextError::new())),
                        }
                    } else if !is_stop_keyword(k) {
                        let next = next_token(input).unwrap();
                        if let Token::Identifier(id) = next {
                            Some(id.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else if let Some(Token::Identifier(alias)) = peek_token(input) {
                    let alias = alias.clone();
                    let _ = next_token(input);
                    Some(alias)
                } else {
                    None
                };
                TableRef::TableValuedFunction { name, args, alias }
            } else {
            let alias = if let Some(Token::Keyword(k)) = peek_token(input) {
                if k.eq_ignore_ascii_case("AS") {
                    let _ = next_token(input);
                    match next_token(input) {
                        Some(Token::Identifier(alias)) => Some(alias.clone()),
                        Some(Token::String(alias)) => Some(alias.clone()),
                        _ => return Err(ErrMode::Backtrack(ContextError::new())),
                    }
                } else if !is_stop_keyword(k) {
                    let next = next_token(input).unwrap();
                    if let Token::Identifier(id) = next {
                        Some(id.clone())
                    } else if let Token::Keyword(kw) = next {
                        Some(kw.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else if let Some(Token::Identifier(alias)) = peek_token(input) {
                if !is_stop_keyword(alias) {
                    let alias = alias.clone();
                    let _ = next_token(input);
                    Some(alias)
                } else {
                    None
                }
            } else if let Some(Token::String(alias)) = peek_token(input) {
                let alias = alias.clone();
                let _ = next_token(input);
                Some(alias)
            } else {
                None
            };
            let mut hints = Vec::new();
            if let Some(Token::Keyword(kw)) = peek_token(input) {
                if kw.eq_ignore_ascii_case("WITH") {
                    let saved = *input;
                    let _ = next_token(input);
                    if matches!(peek_token(input), Some(Token::LParen)) {
                        let _ = next_token(input);
                        hints = parse_comma_list(input, |i| {
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
                    } else {
                        *input = saved;
                    }
                }
            }
            TableRef::Table { name, alias, hints }
            } // end else (not a function call)
        }
        _ => return Err(ErrMode::Backtrack(ContextError::new())),
    };

    loop {
        if let Some(Token::Keyword(k)) = peek_token(input) {
            let k_upper = k.to_uppercase();
            match k_upper.as_str() {
                "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "JOIN" => {
                    let join_type = if k_upper == "JOIN" {
                        let _ = next_token(input);
                        JoinType::Inner
                    } else {
                        let jt = match k_upper.as_str() {
                            "INNER" => JoinType::Inner,
                            "LEFT" => JoinType::Left,
                            "RIGHT" => JoinType::Right,
                            "FULL" => JoinType::Full,
                            "CROSS" => {
                                let mut temp = *input;
                                let _ = next_token(&mut temp);
                                if matches!(peek_token(&temp), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("APPLY")) {
                                    break;
                                }
                                JoinType::Cross
                            }
                            _ => unreachable!(),
                        };
                        
                        if k_upper == "LEFT" || k_upper == "RIGHT" || k_upper == "FULL" {
                            let mut temp = *input;
                            let _ = next_token(&mut temp);
                            if matches!(peek_token(&temp), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("APPLY")) {
                                break;
                            }
                        }

                        let _ = next_token(input);
                        if matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OUTER")) {
                            let _ = next_token(input);
                        }
                        expect_keyword(input, "JOIN")?;
                        jt
                    };
                    let right = parse_table_ref(input)?;
                    let on = if join_type != JoinType::Cross {
                        expect_keyword(input, "ON")?;
                        Some(parse_expr(input)?)
                    } else {
                        None
                    };
                    current = TableRef::Join {
                        left: Box::new(current),
                        join_type,
                        right: Box::new(right),
                        on,
                    };
                }
                "PIVOT" => {
                    let _ = next_token(input);
                    expect_punctuation(input, Token::LParen)?;
                    let aggregate_func = match next_token(input) {
                        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                        _ => return Err(ErrMode::Backtrack(ContextError::new())),
                    };
                    expect_punctuation(input, Token::LParen)?;
                    let aggregate_col = match next_token(input) {
                        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                        _ => return Err(ErrMode::Backtrack(ContextError::new())),
                    };
                    expect_punctuation(input, Token::RParen)?;
                    expect_keyword(input, "FOR")?;
                    let pivot_col = match next_token(input) {
                        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                        _ => return Err(ErrMode::Backtrack(ContextError::new())),
                    };
                    expect_keyword(input, "IN")?;
                    expect_punctuation(input, Token::LParen)?;
                    let pivot_values = parse_comma_list(input, |i| {
                        match next_token(i) {
                            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                            _ => Err(ErrMode::Backtrack(ContextError::new())),
                        }
                    })?;
                    expect_punctuation(input, Token::RParen)?;
                    expect_punctuation(input, Token::RParen)?;
                    let alias = if let Some(Token::Keyword(k)) = peek_token(input) {
                        if k.eq_ignore_ascii_case("AS") {
                            let _ = next_token(input);
                        }
                        match next_token(input) {
                            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                            _ => return Err(ErrMode::Backtrack(ContextError::new())),
                        }
                    } else if let Some(Token::Identifier(id)) = peek_token(input) {
                        let id = id.clone();
                        let _ = next_token(input);
                        id
                    } else {
                        return Err(ErrMode::Backtrack(ContextError::new()));
                    };
                    current = TableRef::Pivot {
                        source: Box::new(current),
                        spec: PivotSpec { aggregate_func, aggregate_col, pivot_col, pivot_values },
                        alias,
                    };
                }
                "UNPIVOT" => {
                    let _ = next_token(input);
                    expect_punctuation(input, Token::LParen)?;
                    let value_col = match next_token(input) {
                        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                        _ => return Err(ErrMode::Backtrack(ContextError::new())),
                    };
                    expect_keyword(input, "FOR")?;
                    let pivot_col = match next_token(input) {
                        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                        _ => return Err(ErrMode::Backtrack(ContextError::new())),
                    };
                    expect_keyword(input, "IN")?;
                    expect_punctuation(input, Token::LParen)?;
                    let column_list = parse_comma_list(input, |i| {
                        match next_token(i) {
                            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => Ok(id.clone()),
                            _ => Err(ErrMode::Backtrack(ContextError::new())),
                        }
                    })?;
                    expect_punctuation(input, Token::RParen)?;
                    expect_punctuation(input, Token::RParen)?;
                    let alias = if let Some(Token::Keyword(k)) = peek_token(input) {
                        if k.eq_ignore_ascii_case("AS") {
                            let _ = next_token(input);
                        }
                        match next_token(input) {
                            Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => id.clone(),
                            _ => return Err(ErrMode::Backtrack(ContextError::new())),
                        }
                    } else if let Some(Token::Identifier(id)) = peek_token(input) {
                        let id = id.clone();
                        let _ = next_token(input);
                        id
                    } else {
                        return Err(ErrMode::Backtrack(ContextError::new()));
                    };
                    current = TableRef::Unpivot {
                        source: Box::new(current),
                        spec: UnpivotSpec { value_col, pivot_col, column_list },
                        alias,
                    };
                }
                _ => break,
            }
        } else {
            break;
        }
    }

    Ok(current)
}

pub fn parse_select_item<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<SelectItem<'a>> {
    let expr = parse_expr(input)?;
    let alias = if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("AS") {
            let _ = next_token(input);
            match next_token(input) {
                Some(Token::Identifier(alias)) => Some(alias.clone()),
                Some(Token::String(alias)) => Some(alias.clone()),
                _ => return Err(ErrMode::Backtrack(ContextError::new())),
            }
        } else if !is_stop_keyword(k) {
             let next = next_token(input).unwrap();
             if let Token::Identifier(id) = next {
                 Some(id.clone())
             } else {
                 None
             }
        } else {
            None
        }
    } else if let Some(Token::Identifier(alias)) = peek_token(input) {
         if !is_stop_keyword(alias) {
             let alias = alias.clone();
             let _ = next_token(input);
             Some(alias)
         } else {
             None
         }
    } else if let Some(Token::String(alias)) = peek_token(input) {
         let alias = alias.clone();
         let _ = next_token(input);
         Some(alias)
    } else {
        None
    };
    Ok(SelectItem { expr, alias })
}

pub fn parse_order_by_expr<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<OrderByExpr<'a>> {
    let expr = parse_expr(input)?;
    let mut asc = true;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("DESC") {
            let _ = next_token(input);
            asc = false;
        } else if k.eq_ignore_ascii_case("ASC") {
            let _ = next_token(input);
            asc = true;
        }
    }
    Ok(OrderByExpr { expr, asc })
}

pub fn peek_token<'a>(input: &&'a [Token<'a>]) -> Option<&'a Token<'a>> {
    input.first()
}

pub fn next_token<'a>(input: &mut &'a [Token<'a>]) -> Option<&'a Token<'a>> {
    let tok = input.first()?;
    *input = &input[1..];
    Some(tok)
}

pub fn parse_multipart_name<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Vec<std::borrow::Cow<'a, str>>> {
    let mut parts = Vec::new();
    if let Some(tok) = next_token(input) {
        match tok {
            Token::Identifier(id) | Token::Keyword(id) | Token::Variable(id) => parts.push(id.clone()),
            _ => return Err(ErrMode::Backtrack(ContextError::new())),
        }
    } else {
        return Err(ErrMode::Backtrack(ContextError::new()));
    }
    while matches!(peek_token(input), Some(Token::Dot)) {
        let _ = next_token(input);
        if let Some(tok) = next_token(input) {
            match tok {
                Token::Identifier(id) | Token::Keyword(id) | Token::Variable(id) => parts.push(id.clone()),
                _ => return Err(ErrMode::Backtrack(ContextError::new())),
            }
        } else {
            return Err(ErrMode::Backtrack(ContextError::new()));
        }
    }
    Ok(parts)
}

pub fn parse_expr<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    crate::parser::parser::expressions::parse_expr(input)
}

pub fn parse_comma_list<'a, P, R>(input: &mut &'a [Token<'a>], parser: P) -> ModalResult<Vec<R>>
where P: FnMut(&mut &'a [Token<'a>]) -> ModalResult<R>
{
    crate::parser::parser::expressions::parse_comma_list(input, parser)
}

pub fn is_stop_keyword(k: &str) -> bool {
    crate::parser::parser::expressions::is_stop_keyword(k)
}

pub fn expect_keyword<'a>(input: &mut &'a [Token<'a>], expected: &str) -> ModalResult<()> {
    crate::parser::parser::expressions::expect_keyword(input, expected)
}

pub fn expect_punctuation<'a>(input: &mut &'a [Token<'a>], expected: Token<'a>) -> ModalResult<()> {
    crate::parser::parser::expressions::expect_punctuation(input, expected)
}
