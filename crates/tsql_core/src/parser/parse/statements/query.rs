use crate::parser::ast::*;
use crate::parser::token::Keyword;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};

pub fn parse_select(parser: &mut Parser) -> ParseResult<SelectStmt> {
    parser.expect_keyword(Keyword::Select)?;
    parse_select_body(parser)
}

pub fn parse_select_body(parser: &mut Parser) -> ParseResult<SelectStmt> {
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

fn parse_single_select(parser: &mut Parser) -> ParseResult<SelectStmt> {
    parser.expect_keyword(Keyword::Select)?;
    parse_single_select_body(parser)
}

pub fn parse_single_select_body(parser: &mut Parser) -> ParseResult<SelectStmt> {
    let mut distinct = false;
    if let Some(Token::Keyword(Keyword::Distinct)) = parser.peek() {
        let _ = parser.next();
        distinct = true;
    }

    let mut top = None;
    if let Some(Token::Keyword(Keyword::Top)) = parser.peek() {
        let _ = parser.next();
        top = Some(TopSpec { value: crate::parser::parse::expressions::parse_primary(parser)? });
    }

    let projection = parse_projection(parser)?;

    let mut into_table = None;
    if let Some(Token::Keyword(Keyword::Into)) = parser.peek() {
        let _ = parser.next();
        into_table = Some(parse_object_name(parse_multipart_name(parser)?));
    }

    let mut from = None;
    let mut joins = Vec::new();
    if let Some(tok) = parser.peek() {
        let is_from = match tok {
            Token::Keyword(Keyword::From) => true,
            Token::Identifier(id) => id.eq_ignore_ascii_case("FROM"),
            _ => false,
        };
        if is_from {
            let _ = parser.next();
            let tables = crate::parser::parse::expressions::parse_comma_list(parser, parse_table_ref)?;
            let mut iter = tables.into_iter();
            from = iter.next();
            for table in iter {
                joins.push(JoinClause {
                    join_type: JoinType::Cross,
                    table,
                    on: None,
                });
            }
        }
    }

    while let Some(join) = parse_join_clause(parser)? {
        joins.push(join);
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
            let table = parse_table_ref(parser)?;
            applies.push(ApplyClause { apply_type, table });
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
        joins,
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

fn parse_projection(parser: &mut Parser) -> ParseResult<Vec<SelectItem>> {
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

pub fn parse_table_ref(parser: &mut Parser) -> ParseResult<TableRef> {
    let (factor, alias, hints) = match parser.peek() {
        Some(Token::LParen) => {
            let _ = parser.next();
            if parser.at_keyword(Keyword::Values) {
                let _ = parser.next();
                let rows = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                    p.expect_lparen()?;
                    let vals = crate::parser::parse::expressions::parse_comma_list(
                        p,
                        crate::parser::parse::expressions::parse_expr,
                    )?;
                    p.expect_rparen()?;
                    Ok(vals)
                })?;
                parser.expect_rparen()?;
                let alias = parse_required_alias(parser)?;
                let mut columns = Vec::new();
                if matches!(parser.peek(), Some(Token::LParen)) {
                    let _ = parser.next();
                    columns = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                        match p.next() {
                            Some(Token::Identifier(id)) => Ok(id.clone()),
                            Some(Token::Keyword(kw)) => Ok(kw.as_ref().to_string()),
                            _ => p.backtrack(Expected::Description("column name")),
                        }
                    })?;
                    parser.expect_rparen()?;
                }
                (
                    TableFactor::Values { rows, columns },
                    Some(alias),
                    Vec::new(),
                )
            } else if parser.at_keyword(Keyword::Select) {
                let subquery = Box::new(parse_select(parser)?);
                parser.expect_rparen()?;
                let alias = parse_required_alias(parser)?;
                (
                    TableFactor::Derived(subquery),
                    Some(alias),
                    Vec::new(),
                )
            } else {
                let base = parse_table_ref(parser)?;
                let mut joins = Vec::new();
                while !matches!(parser.peek(), Some(Token::RParen)) {
                    match parse_join_clause(parser)? {
                        Some(join) => joins.push(join),
                        None => break,
                    }
                }
                parser.expect_rparen()?;
                let alias = parse_optional_alias(parser);
                (
                    TableFactor::JoinedGroup {
                        base: Box::new(base),
                        joins,
                    },
                    alias,
                    Vec::new(),
                )
            }
        }
        Some(Token::Identifier(_)) | Some(Token::Keyword(_)) | Some(Token::Variable(_)) => {
            let name = parse_multipart_name(parser)?;
            if matches!(parser.peek(), Some(Token::LParen)) {
                let _ = parser.next();
                let args = crate::parser::parse::expressions::parse_comma_list(
                    parser,
                    crate::parser::parse::expressions::parse_expr,
                )?;
                parser.expect_rparen()?;
                let alias = parse_optional_alias(parser);
                let factor = TableFactor::TableValuedFunction {
                    name,
                    args,
                    alias: alias.clone(),
                };
                return Ok(TableRef {
                    factor,
                    alias,
                    pivot: None,
                    unpivot: None,
                    hints: Vec::new(),
                });
            }
            let alias = parse_optional_alias(parser);
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
                                Token::Keyword(kw) => Ok(kw.as_ref().to_string()),
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
            (
                TableFactor::Named(parse_object_name(name)),
                alias,
                hints,
            )
        }
        _ => return parser.backtrack(Expected::Description("table reference")),
    };

    let mut table = TableRef {
        factor,
        alias,
        pivot: None,
        unpivot: None,
        hints,
    };

    loop {
        match parser.peek() {
            Some(Token::Keyword(Keyword::Pivot)) => {
                let _ = parser.next();
                parser.expect_lparen()?;
                let aggregate_func = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
                    _ => return parser.backtrack(Expected::Description("identifier")),
                };
                parser.expect_lparen()?;
                let aggregate_col = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
                    _ => return parser.backtrack(Expected::Description("identifier")),
                };
                parser.expect_rparen()?;
                parser.expect_keyword(Keyword::For)?;
                let pivot_col = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
                    _ => return parser.backtrack(Expected::Description("identifier")),
                };
                parser.expect_keyword(Keyword::In)?;
                parser.expect_lparen()?;
                let pivot_values = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                    match p.next() {
                        Some(Token::Identifier(id)) => Ok(id.clone()),
                        Some(Token::Keyword(kw)) => Ok(kw.as_ref().to_string()),
                        _ => p.backtrack(Expected::Description("identifier")),
                    }
                })?;
                parser.expect_rparen()?;
                parser.expect_rparen()?;
                let alias = parse_required_alias(parser)?;
                table.pivot = Some(Box::new(PivotSpec {
                    aggregate_func,
                    aggregate_col,
                    pivot_col,
                    pivot_values,
                }));
                table.alias = Some(alias);
            }
            Some(Token::Keyword(Keyword::Unpivot)) => {
                let _ = parser.next();
                parser.expect_lparen()?;
                let value_col = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
                    _ => return parser.backtrack(Expected::Description("identifier")),
                };
                parser.expect_keyword(Keyword::For)?;
                let pivot_col = match parser.next() {
                    Some(Token::Identifier(id)) => id.clone(),
                    Some(Token::Keyword(kw)) => kw.as_ref().to_string(),
                    _ => return parser.backtrack(Expected::Description("identifier")),
                };
                parser.expect_keyword(Keyword::In)?;
                parser.expect_lparen()?;
                let column_list = crate::parser::parse::expressions::parse_comma_list(parser, |p| {
                    match p.next() {
                        Some(Token::Identifier(id)) => Ok(id.clone()),
                        Some(Token::Keyword(kw)) => Ok(kw.as_ref().to_string()),
                        _ => p.backtrack(Expected::Description("identifier")),
                    }
                })?;
                parser.expect_rparen()?;
                parser.expect_rparen()?;
                let alias = parse_required_alias(parser)?;
                table.unpivot = Some(Box::new(UnpivotSpec {
                    value_col,
                    pivot_col,
                    column_list,
                }));
                table.alias = Some(alias);
            }
            _ => break,
        }
    }

    Ok(table)
}

pub fn parse_join_clause(parser: &mut Parser) -> ParseResult<Option<JoinClause>> {
    let Some(Token::Keyword(k)) = parser.peek() else {
        return Ok(None);
    };

    if matches!(k, Keyword::Cross | Keyword::Left | Keyword::Right | Keyword::Full)
        && matches!(parser.peek_at(1), Some(Token::Keyword(Keyword::Apply)))
    {
        return Ok(None);
    }

    let join_type = match *k {
        Keyword::Join => {
            let _ = parser.next();
            JoinType::Inner
        }
        Keyword::Inner | Keyword::Left | Keyword::Right | Keyword::Full | Keyword::Cross => {
            let jt = match *k {
                Keyword::Inner => JoinType::Inner,
                Keyword::Left => JoinType::Left,
                Keyword::Right => JoinType::Right,
                Keyword::Full => JoinType::Full,
                Keyword::Cross => JoinType::Cross,
                _ => unreachable!(),
            };
            let _ = parser.next();
            if matches!(parser.peek(), Some(Token::Keyword(Keyword::Outer))) {
                let _ = parser.next();
            }
            parser.expect_keyword(Keyword::Join)?;
            jt
        }
        _ => return Ok(None),
    };

    let table = parse_table_ref(parser)?;
    let on = if join_type != JoinType::Cross {
        parser.expect_keyword(Keyword::On)?;
        Some(crate::parser::parse::expressions::parse_expr(parser)?)
    } else {
        None
    };
    Ok(Some(JoinClause { join_type, table, on }))
}

fn parse_optional_alias(parser: &mut Parser) -> Option<String> {
    let saved = parser.save();
    match parser.peek() {
        Some(Token::Keyword(Keyword::As)) => {
            let _ = parser.next();
            match parser.next() {
                Some(Token::Identifier(alias)) => Some(alias.clone()),
                Some(Token::String(alias)) => Some(alias.clone()),
                Some(Token::Keyword(kw)) => Some(kw.as_ref().to_string()),
                _ => {
                    parser.restore(saved);
                    None
                }
            }
        }
        Some(Token::Keyword(k)) if !crate::parser::parse::expressions::is_stop_keyword(k.as_sql()) => {
            match parser.next() {
                Some(Token::Identifier(alias)) => Some(alias.clone()),
                Some(Token::Keyword(kw)) => Some(kw.as_ref().to_string()),
                Some(Token::String(alias)) => Some(alias.clone()),
                Some(_) | None => {
                    parser.restore(saved);
                    None
                }
            }
        }
        Some(Token::Identifier(alias)) if !crate::parser::parse::expressions::is_stop_keyword(alias) => {
            let alias = alias.clone();
            let _ = parser.next();
            Some(alias)
        }
        Some(Token::String(alias)) => {
            let alias = alias.clone();
            let _ = parser.next();
            Some(alias)
        }
        _ => {
            parser.restore(saved);
            None
        }
    }
}

fn parse_required_alias(parser: &mut Parser) -> ParseResult<String> {
    parse_optional_alias(parser).ok_or_else(|| parser.error(Expected::Description("alias")))
}

pub fn parse_select_item(parser: &mut Parser) -> ParseResult<SelectItem> {
    let expr = crate::parser::parse::expressions::parse_expr(parser)?;
    let alias = if let Some(Token::Keyword(k)) = parser.peek() {
        if *k == Keyword::As {
            let _ = parser.next();
            match parser.next() {
                Some(Token::Identifier(alias)) => Some(alias.clone()),
                Some(Token::String(alias)) => Some(alias.clone()),
                _ => return parser.backtrack(Expected::Description("alias")),
            }
        } else if !crate::parser::parse::expressions::is_stop_keyword(k.as_sql()) {
            match parser.next() {
                Some(Token::Identifier(id)) => Some(id.clone()),
                Some(_) | None => None,
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

pub fn parse_order_by_expr(parser: &mut Parser) -> ParseResult<OrderByExpr> {
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

pub fn parse_multipart_name(parser: &mut Parser) -> ParseResult<Vec<String>> {
    let mut parts = Vec::new();
    if let Some(tok) = parser.next() {
        match tok {
            Token::Identifier(id) | Token::Variable(id) => parts.push(id.clone()),
            Token::Keyword(k) => parts.push(k.as_ref().to_string()),
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
                Token::Keyword(k) => parts.push(k.as_ref().to_string()),
                _ => return parser.backtrack(Expected::Description("identifier")),
            }
        } else {
            return parser.backtrack(Expected::Description("identifier"));
        }
    }
    Ok(parts)
}

pub fn parse_object_name(mut parts: Vec<String>) -> ObjectName {
    match parts.len() {
        0 => ObjectName { schema: None, name: "".to_string() },
        1 => ObjectName { schema: None, name: parts.remove(0) },
        _ => {
            let name = parts.pop().unwrap_or_default();
            let schema = Some(parts.pop().unwrap_or_default());
            ObjectName { schema, name }
        }
    }
}
