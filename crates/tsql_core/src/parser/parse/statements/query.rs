use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};
use std::borrow::Cow;

pub fn parse_select<'a>(parser: &mut Parser<'a>) -> ParseResult<SelectStmt<'a>> {
    parser.expect_keyword(Keyword::Select)?;
    parse_select_body(parser)
}

pub fn parse_select_body<'a>(parser: &mut Parser<'a>) -> ParseResult<SelectStmt<'a>> {
    let mut current = parse_single_select_body(parser)?;

    loop {
        let kind = match parser.peek() {
            Some(Token::Keyword(k)) => match *k {
                Keyword::Union => {
                    let _ = parser.next();
                    if matches!(parser.peek(), Some(Token::Keyword(Keyword::All))) {
                        let _ = parser.next();
                        SetOpKind::UnionAll
                    } else {
                        SetOpKind::Union
                    }
                }
                Keyword::Intersect => {
                    let _ = parser.next();
                    SetOpKind::Intersect
                }
                Keyword::Except => {
                    let _ = parser.next();
                    SetOpKind::Except
                }
                _ => break,
            },
            _ => break,
        };

        let right = parse_single_select(parser)?;
        
        let mut target = &mut current;
        while let Some(ref mut op) = target.set_op {
            target = &mut op.right;
        }
        target.set_op = Some(Box::new(SetOp { kind, right }));
    }

    Ok(current)
}

fn parse_single_select<'a>(parser: &mut Parser<'a>) -> ParseResult<SelectStmt<'a>> {
    parser.expect_keyword(Keyword::Select)?;
    parse_single_select_body(parser)
}

pub fn parse_single_select_body<'a>(parser: &mut Parser<'a>) -> ParseResult<SelectStmt<'a>> {
    let mut distinct = false;
    if let Some(Token::Keyword(Keyword::Distinct)) = parser.peek() {
        let _ = parser.next();
        distinct = true;
    }

    let mut top = None;
    if let Some(Token::Keyword(Keyword::Top)) = parser.peek() {
        let _ = parser.next();
        top = Some(crate::parser::parse::expressions::parse_primary(parser)?);
    }

    let projection = parse_projection(parser)?;

    let mut into_table = None;
    if let Some(Token::Keyword(Keyword::Into)) = parser.peek() {
        let _ = parser.next();
        into_table = Some(parse_multipart_name(parser)?);
    }

    let mut from = None;
    if let Some(tok) = parser.peek() {
        let is_from = match tok {
            Token::Keyword(Keyword::From) => true,
            Token::Identifier(id) => id.eq_ignore_ascii_case("FROM"),
            _ => false,
        };
        if is_from {
            let _ = parser.next();
            from = Some(crate::parser::parse::expressions::parse_comma_list(parser, parse_table_ref)?);
        }
    }

    let mut applies = Vec::new();
    loop {
        if let Some(Token::Keyword(k)) = parser.peek() {
            let apply_type = match *k {
                Keyword::Cross => {
                    let saved = parser.save();
                    let _ = parser.next();
                    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Apply))) {
                        let _ = parser.next();
                        ApplyType::Cross
                    } else {
                        parser.restore(saved);
                        break;
                    }
                }
                Keyword::Outer => {
                    let saved = parser.save();
                    let _ = parser.next();
                    if matches!(parser.peek(), Some(Token::Keyword(Keyword::Apply))) {
                        let _ = parser.next();
                        ApplyType::Outer
                    } else {
                        parser.restore(saved);
                        break;
                    }
                }
                _ => break,
            };
            parser.expect_lparen()?;
            let subquery = Box::new(parse_select(parser)?);
            parser.expect_rparen()?;
            let alias = if let Some(Token::Keyword(k)) = parser.peek() {
                if *k == Keyword::As {
                    let _ = parser.next();
                }
                match parser.next() {
                    Some(Token::Identifier(id)) => Some(id.clone()),
                    Some(Token::Keyword(kw)) => Some(Cow::Owned(kw.as_ref().to_string())),
                    _ => return parser.backtrack(Expected::Description("alias")),
                }
            } else if let Some(Token::Identifier(id)) = parser.peek() {
                let id = id.clone();
                let _ = parser.next();
                Some(id)
            } else {
                return parser.backtrack(Expected::Description("alias"));
            };
            let Some(alias) = alias else {
                return parser.backtrack(Expected::Description("alias"));
            };
            applies.push(ApplyClause { apply_type, subquery, alias });
        } else {
            break;
        }
    }

    let mut selection = None;
    if let Some(Token::Keyword(Keyword::Where)) = parser.peek() {
        let _ = parser.next();
        selection = Some(crate::parser::parse::expressions::parse_expr(parser)?);
    }

    let mut group_by = Vec::new();
    if let Some(Token::Keyword(Keyword::Group)) = parser.peek() {
        let _ = parser.next();
        if matches!(parser.peek(), Some(Token::Keyword(Keyword::By))) {
            let _ = parser.next();
        }
        group_by = crate::parser::parse::expressions::parse_comma_list(parser, crate::parser::parse::expressions::parse_expr)?;
    }

    let mut having = None;
    if let Some(Token::Keyword(Keyword::Having)) = parser.peek() {
        let _ = parser.next();
        having = Some(crate::parser::parse::expressions::parse_expr(parser)?);
    }

    let mut order_by = Vec::new();
    if let Some(Token::Keyword(Keyword::Order)) = parser.peek() {
        let _ = parser.next();
        if matches!(parser.peek(), Some(Token::Keyword(Keyword::By))) {
            let _ = parser.next();
        }
        order_by = crate::parser::parse::expressions::parse_comma_list(parser, parse_order_by_expr)?;
    }

    let mut offset = None;
    let mut fetch = None;
    if let Some(Token::Keyword(Keyword::Offset)) = parser.peek() {
        let _ = parser.next();
        offset = Some(crate::parser::parse::expressions::parse_expr(parser)?);
        parser.expect_keyword(Keyword::Rows)?;
        if let Some(Token::Keyword(Keyword::Fetch)) = parser.peek() {
            let _ = parser.next();
            parser.expect_keyword(Keyword::Next)?;
            fetch = Some(crate::parser::parse::expressions::parse_expr(parser)?);
            parser.expect_keyword(Keyword::Rows)?;
            parser.expect_keyword(Keyword::Only)?;
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

fn parse_projection<'a>(parser: &mut Parser<'a>) -> ParseResult<Vec<SelectItem<'a>>> {
    let mut items = Vec::new();
    loop {
        items.push(parse_select_item(parser)?);
        if let Some(Token::Comma) = parser.peek() {
            let _ = parser.next();
            continue;
        }
        if let Some(Token::Keyword(k)) = parser.peek() {
            if matches!(k, Keyword::From | Keyword::Into | Keyword::Where | Keyword::Group | Keyword::Order | Keyword::Having | Keyword::Union | Keyword::Intersect | Keyword::Except) {
                break;
            }
        }
        break;
    }
    Ok(items)
}

pub fn parse_table_ref<'a>(parser: &mut Parser<'a>) -> ParseResult<TableRef<'a>> {
    let mut current = match parser.peek() {
        Some(Token::LParen) => {
            let _ = parser.next();
            let subquery = Box::new(parse_select(parser)?);
            parser.expect_rparen()?;
            let alias = if let Some(tok) = parser.peek() {
                match tok {
                    Token::Keyword(Keyword::As) => {
                        let _ = parser.next();
                        match parser.next() {
                            Some(Token::Identifier(alias)) => alias.clone(),
                            _ => return parser.backtrack(Expected::Description("alias")),
                        }
                    }
                    Token::Keyword(k) => Cow::Owned(k.as_ref().to_string()),
                    Token::Identifier(id) => id.clone(),
                    _ => return parser.backtrack(Expected::Description("alias")),
                }
            } else {
                return parser.backtrack(Expected::Description("alias"));
            };
            TableRef::Subquery { subquery, alias }
        }
        Some(Token::Identifier(_)) | Some(Token::Keyword(_)) | Some(Token::Variable(_)) => {
            let name = parse_multipart_name(parser)?;
            if matches!(parser.peek(), Some(Token::LParen)) {
                let _ = parser.next();
                let args = crate::parser::parse::expressions::parse_comma_list(parser, crate::parser::parse::expressions::parse_expr)?;
                parser.expect_rparen()?;
                let alias = if let Some(Token::Keyword(k)) = parser.peek() {
                    if *k == Keyword::As {
                        let _ = parser.next();
                        match parser.next() {
                            Some(Token::Identifier(alias)) => Some(alias.clone()),
                            _ => return parser.backtrack(Expected::Description("alias")),
                        }
                    } else if !crate::parser::parse::expressions::is_stop_keyword(k.as_ref()) {
                        let next = parser.next().unwrap();
                        if let Token::Identifier(id) = next {
                            Some(id.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else if let Some(Token::Identifier(alias)) = parser.peek() {
                    let alias = alias.clone();
                    let _ = parser.next();
                    Some(alias)
                } else {
                    None
                };
                TableRef::TableValuedFunction { name, args, alias }
            } else {
            let alias = if let Some(Token::Keyword(k)) = parser.peek() {
                if *k == Keyword::As {
                    let _ = parser.next();
                    match parser.next() {
                        Some(Token::Identifier(alias)) => Some(alias.clone()),
                        Some(Token::String(alias)) => Some(alias.clone()),
                        _ => return parser.backtrack(Expected::Description("alias")),
                    }
                } else if !crate::parser::parse::expressions::is_stop_keyword(k.as_ref()) {
                    let next = parser.next().unwrap();
                    if let Token::Identifier(id) = next {
                        Some(id.clone())
                    } else if let Token::Keyword(kw) = next {
                        Some(Cow::Owned(kw.as_ref().to_string()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else if let Some(Token::Identifier(alias)) = parser.peek() {
                if !crate::parser::parse::expressions::is_stop_keyword(alias) {
                    let alias = alias.clone();
                    let _ = parser.next();
                    Some(alias)
                } else {
                    None
                }
            } else if let Some(Token::String(alias)) = parser.peek() {
                let alias = alias.clone();
                let _ = parser.next();
                Some(alias)
            } else {
                None
            };
            let mut hints = Vec::new();
            if let Some(Token::Keyword(Keyword::With)) = parser.peek() {
                let saved = parser.save();
                let _ = parser.next();
                if matches!(parser.peek(), Some(Token::LParen)) {
                    let _ = parser.next();
                    hints = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                        if let Some(tok) = p.next() {
                            match tok {
                                Token::Identifier(id) => Ok(id.clone()),
                                Token::Keyword(kw) => Ok(Cow::Owned(kw.as_ref().to_string())),
                                _ => p.backtrack(Expected::Description("identifier")),
                            }
                        } else {
                            p.backtrack(Expected::Description("identifier"))
                        }
                    })?;
                    parser.expect_rparen()?;
                } else {
                    parser.restore(saved);
                }
            }
            TableRef::Table { name, alias, hints }
            }
        }
        _ => return parser.backtrack(Expected::Description("table reference")),
    };

    loop {
        if let Some(Token::Keyword(k)) = parser.peek() {
            match *k {
                Keyword::Inner | Keyword::Left | Keyword::Right | Keyword::Full | Keyword::Cross | Keyword::Join => {
                    let join_type = if *k == Keyword::Join {
                        let _ = parser.next();
                        JoinType::Inner
                    } else {
                        let jt = match *k {
                            Keyword::Inner => JoinType::Inner,
                            Keyword::Left => JoinType::Left,
                            Keyword::Right => JoinType::Right,
                            Keyword::Full => JoinType::Full,
                            Keyword::Cross => {
                                if matches!(parser.peek_at(1), Some(Token::Keyword(Keyword::Apply))) {
                                    break;
                                }
                                JoinType::Cross
                            }
                            _ => unreachable!(),
                        };
                        
                        if matches!(k, Keyword::Left | Keyword::Right | Keyword::Full) {
                            if matches!(parser.peek_at(1), Some(Token::Keyword(Keyword::Apply))) {
                                break;
                            }
                        }

                        let _ = parser.next();
                        if matches!(parser.peek(), Some(Token::Keyword(Keyword::Outer))) {
                            let _ = parser.next();
                        }
                        parser.expect_keyword(Keyword::Join)?;
                        jt
                    };
                    let right = parse_table_ref(parser)?;
                    let on = if join_type != JoinType::Cross {
                        parser.expect_keyword(Keyword::On)?;
                        Some(crate::parser::parse::expressions::parse_expr(parser)?)
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
                Keyword::Pivot => {
                    let _ = parser.next();
                    parser.expect_lparen()?;
                    let aggregate_func = match parser.next() {
                        Some(Token::Identifier(id)) => id.clone(),
                        Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                        _ => return parser.backtrack(Expected::Description("identifier")),
                    };
                    parser.expect_lparen()?;
                    let aggregate_col = match parser.next() {
                        Some(Token::Identifier(id)) => id.clone(),
                        Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                        _ => return parser.backtrack(Expected::Description("identifier")),
                    };
                    parser.expect_rparen()?;
                    parser.expect_keyword(Keyword::For)?;
                    let pivot_col = match parser.next() {
                        Some(Token::Identifier(id)) => id.clone(),
                        Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                        _ => return parser.backtrack(Expected::Description("identifier")),
                    };
                    parser.expect_keyword(Keyword::In)?;
                    parser.expect_lparen()?;
                    let pivot_values = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                        match p.next() {
                            Some(Token::Identifier(id)) => Ok(id.clone()),
                            Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                            _ => p.backtrack(Expected::Description("identifier")),
                        }
                    })?;
                    parser.expect_rparen()?;
                    parser.expect_rparen()?;
                    let alias = if let Some(Token::Keyword(k)) = parser.peek() {
                        if *k == Keyword::As {
                            let _ = parser.next();
                        }
                        match parser.next() {
                            Some(Token::Identifier(id)) => id.clone(),
                            Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                            _ => return parser.backtrack(Expected::Description("alias")),
                        }
                    } else if let Some(Token::Identifier(id)) = parser.peek() {
                        let id = id.clone();
                        let _ = parser.next();
                        id
                    } else {
                        return parser.backtrack(Expected::Description("alias"));
                    };
                    current = TableRef::Pivot {
                        source: Box::new(current),
                        spec: PivotSpec { aggregate_func, aggregate_col, pivot_col, pivot_values },
                        alias,
                    };
                }
                Keyword::Unpivot => {
                    let _ = parser.next();
                    parser.expect_lparen()?;
                    let value_col = match parser.next() {
                        Some(Token::Identifier(id)) => id.clone(),
                        Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                        _ => return parser.backtrack(Expected::Description("identifier")),
                    };
                    parser.expect_keyword(Keyword::For)?;
                    let pivot_col = match parser.next() {
                        Some(Token::Identifier(id)) => id.clone(),
                        Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                        _ => return parser.backtrack(Expected::Description("identifier")),
                    };
                    parser.expect_keyword(Keyword::In)?;
                    parser.expect_lparen()?;
                    let column_list = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                        match p.next() {
                            Some(Token::Identifier(id)) => Ok(id.clone()),
                            Some(Token::Keyword(kw)) => Ok(Cow::Owned(kw.as_ref().to_string())),
                            _ => p.backtrack(Expected::Description("identifier")),
                        }
                    })?;
                    parser.expect_rparen()?;
                    parser.expect_rparen()?;
                    let alias = if let Some(Token::Keyword(k)) = parser.peek() {
                        if *k == Keyword::As {
                            let _ = parser.next();
                        }
                        match parser.next() {
                            Some(Token::Identifier(id)) => id.clone(),
                            Some(Token::Keyword(kw)) => Cow::Owned(kw.as_ref().to_string()),
                            _ => return parser.backtrack(Expected::Description("alias")),
                        }
                    } else if let Some(Token::Identifier(id)) = parser.peek() {
                        let id = id.clone();
                        let _ = parser.next();
                        id
                    } else {
                        return parser.backtrack(Expected::Description("alias"));
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

pub fn parse_select_item<'a>(parser: &mut Parser<'a>) -> ParseResult<SelectItem<'a>> {
    let expr = crate::parser::parse::expressions::parse_expr(parser)?;
    let alias = if let Some(Token::Keyword(k)) = parser.peek() {
        if *k == Keyword::As {
            let _ = parser.next();
            match parser.next() {
                Some(Token::Identifier(alias)) => Some(alias.clone()),
                Some(Token::String(alias)) => Some(alias.clone()),
                _ => return parser.backtrack(Expected::Description("alias")),
            }
        } else if !crate::parser::parse::expressions::is_stop_keyword(k.as_ref()) {
             let next = parser.next().unwrap();
             if let Token::Identifier(id) = next {
                 Some(id.clone())
             } else {
                 None
             }
        } else {
            None
        }
    } else if let Some(Token::Identifier(alias)) = parser.peek() {
         if !crate::parser::parse::expressions::is_stop_keyword(alias) {
             let alias = alias.clone();
             let _ = parser.next();
             Some(alias)
         } else {
             None
         }
    } else if let Some(Token::String(alias)) = parser.peek() {
         let alias = alias.clone();
         let _ = parser.next();
         Some(alias)
    } else {
        None
    };
    Ok(SelectItem { expr, alias })
}

pub fn parse_order_by_expr<'a>(parser: &mut Parser<'a>) -> ParseResult<OrderByExpr<'a>> {
    let expr = crate::parser::parse::expressions::parse_expr(parser)?;
    let mut asc = true;
    if let Some(Token::Keyword(Keyword::Desc)) = parser.peek() {
        let _ = parser.next();
        asc = false;
    } else if let Some(Token::Keyword(Keyword::Asc)) = parser.peek() {
        let _ = parser.next();
        asc = true;
    }
    Ok(OrderByExpr { expr, asc })
}

pub fn parse_multipart_name<'a>(parser: &mut Parser<'a>) -> ParseResult<Vec<Cow<'a, str>>> {
    let mut parts = Vec::new();
    if let Some(tok) = parser.next() {
        match tok {
            Token::Identifier(id) | Token::Variable(id) => parts.push(id.clone()),
            Token::Keyword(k) => parts.push(Cow::Owned(k.as_ref().to_string())),
            _ => return parser.backtrack(Expected::Description("identifier")),
        }
    } else {
        return parser.backtrack(Expected::Description("identifier"));
    }
    while matches!(parser.peek(), Some(Token::Dot)) {
        let _ = parser.next();
        if let Some(tok) = parser.next() {
            match tok {
                Token::Identifier(id) | Token::Variable(id) => parts.push(id.clone()),
                Token::Keyword(k) => parts.push(Cow::Owned(k.as_ref().to_string())),
                _ => return parser.backtrack(Expected::Description("identifier")),
            }
        } else {
            return parser.backtrack(Expected::Description("identifier"));
        }
    }
    Ok(parts)
}
