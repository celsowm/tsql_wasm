use crate::parser::ast::*;
use crate::parser::error::{Expected, ParseResult};
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

use super::common::parse_comma_list;
use super::data_types::parse_data_type;
use super::parse_expr;
use super::window::parse_window_over;

pub fn parse_primary(parser: &mut Parser) -> ParseResult<Expr> {
    match parser.peek() {
        Some(Token::Number {
            value: n,
            is_float,
            raw,
        }) => {
            let n = *n;
            let is_float = *is_float;
            let raw = raw.clone();
            let _ = parser.next();
            if !is_float && n.fract() == 0.0 {
                Ok(Expr::Integer(n as i64))
            } else {
                Ok(Expr::Float(raw))
            }
        }
        Some(Token::String(s)) => {
            let s = s.clone();
            let _ = parser.next();
            Ok(Expr::String(s))
        }
        Some(Token::NString(s)) => {
            let s = s.clone();
            let _ = parser.next();
            Ok(Expr::UnicodeString(s))
        }
        Some(Token::Variable(v)) => {
            let v = v.clone();
            let _ = parser.next();
            Ok(Expr::Variable(v))
        }
        Some(Token::BinaryLiteral(hex)) => {
            let hex = hex.clone();
            let _ = parser.next();
            let hex_str = if hex.starts_with("0x") || hex.starts_with("0X") {
                &hex[2..]
            } else {
                hex.as_ref()
            };
            let padded;
            let normalized = if hex_str.len() % 2 != 0 {
                padded = format!("0{}", hex_str);
                padded.as_str()
            } else {
                hex_str
            };
            let bytes = (0..normalized.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&normalized[i..i + 2], 16).unwrap_or(0))
                .collect();
            Ok(Expr::BinaryLiteral(bytes))
        }
        Some(Token::Tilde) => {
            let _ = parser.next();
            let expr = super::pratt::parse_expr(parser)?;
            Ok(Expr::Unary {
                op: UnaryOp::BitwiseNot,
                expr: Box::new(expr),
            })
        }
        Some(Token::Identifier(id)) => {
            let id = id.clone();
            let _ = parser.next();
            parse_identifier_or_function(parser, id)
        }
        Some(Token::Keyword(k)) if *k == Keyword::Null => {
            let _ = parser.next();
            Ok(Expr::Null)
        }
        Some(Token::Keyword(k)) if *k == Keyword::Coalesce || *k == Keyword::Nullif => {
            let name = k.as_ref().to_string();
            let _ = parser.next();
            parser.expect_lparen()?;
            let args = parse_comma_list(parser, parse_expr)?;
            parser.expect_rparen()?;
            Ok(Expr::FunctionCall {
                name,
                args,
                within_group: Vec::new(),
            })
        }
        Some(Token::Keyword(k)) if *k == Keyword::Case => {
            let _ = parser.next();
            parse_case(parser)
        }
        Some(Token::Keyword(k)) if *k == Keyword::Exists => {
            let _ = parser.next();
            parser.expect_lparen()?;
            let subquery = Box::new(crate::parser::parse::statements::query::parse_select(
                parser,
            )?);
            parser.expect_rparen()?;
            Ok(Expr::Exists {
                subquery,
                negated: false,
            })
        }
        Some(Token::Keyword(k)) if *k == Keyword::Cast => {
            let _ = parser.next();
            parse_cast(parser)
        }
        Some(Token::Keyword(k)) if *k == Keyword::Convert => {
            let _ = parser.next();
            parse_convert(parser)
        }
        Some(Token::Keyword(k)) if *k == Keyword::TryCast => {
            let _ = parser.next();
            parse_try_cast(parser)
        }
        Some(Token::Keyword(k)) if *k == Keyword::TryConvert => {
            let _ = parser.next();
            parse_try_convert(parser)
        }
        Some(Token::Keyword(k)) if *k == Keyword::Not => {
            let _ = parser.next();
            if parser.at_keyword(Keyword::Exists) {
                let _ = parser.next();
                parser.expect_lparen()?;
                let subquery = Box::new(crate::parser::parse::statements::query::parse_select(
                    parser,
                )?);
                parser.expect_rparen()?;
                Ok(Expr::Exists {
                    subquery,
                    negated: true,
                })
            } else {
                let expr = super::pratt::parse_expr(parser)?;
                Ok(Expr::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(expr),
                })
            }
        }
        Some(Token::Keyword(Keyword::Next)) => {
            let _ = parser.next();
            parser.expect_keyword(Keyword::Value)?;
            parser.expect_keyword(Keyword::For)?;
            let sequence_name =
                crate::parser::parse::statements::query::parse_multipart_name(parser)?;
            Ok(Expr::NextValueFor { sequence_name })
        }
        Some(Token::Keyword(k))
            if matches!(
                k,
                Keyword::CurrentDate
                    | Keyword::CurrentTime
                    | Keyword::CurrentTimestamp
                    | Keyword::CurrentUser
                    | Keyword::SessionUser
                    | Keyword::SystemUser
            ) =>
        {
            let name = k.as_sql().to_string();
            let _ = parser.next();
            Ok(Expr::FunctionCall {
                name,
                args: vec![],
                within_group: vec![],
            })
        }
        Some(Token::Keyword(k)) if matches!(parser.peek_at(1), Some(Token::LParen)) => {
            let name = k.as_ref().to_string();
            let _ = parser.next();
            parse_identifier_or_function(parser, name)
        }
        Some(Token::Operator(op)) if *op == "-" => {
            let _ = parser.next();
            let expr = super::pratt::parse_expr(parser)?;
            Ok(Expr::Unary {
                op: UnaryOp::Negate,
                expr: Box::new(expr),
            })
        }
        Some(Token::Keyword(k))
            if matches!(parser.peek_at(1), Some(Token::LParen) | Some(Token::Dot)) =>
        {
            let name = k.as_ref().to_string();
            let _ = parser.next();
            parse_identifier_or_function(parser, name)
        }
        Some(Token::Keyword(k)) => {
            let name = k.as_ref().to_string();
            let _ = parser.next();
            Ok(Expr::Identifier(name))
        }
        Some(Token::LParen) => {
            let _ = parser.next();
            let expr = if parser.at_keyword(Keyword::Select) {
                Expr::Subquery(Box::new(
                    crate::parser::parse::statements::query::parse_select(parser)?,
                ))
            } else {
                parse_expr(parser)?
            };
            parser.expect_rparen()?;
            Ok(expr)
        }
        Some(Token::Star) => {
            let _ = parser.next();
            Ok(Expr::Wildcard)
        }
        _ => parser.backtrack(Expected::Description("expression")),
    }
}

pub fn parse_case(parser: &mut Parser) -> ParseResult<Expr> {
    let mut operand = None;
    if !parser.at_keyword(Keyword::When) {
        operand = Some(Box::new(parse_expr(parser)?));
    }

    let mut when_clauses = Vec::new();
    while parser.at_keyword(Keyword::When) {
        let _ = parser.next();
        let condition = parse_expr(parser)?;
        parser.expect_keyword(Keyword::Then)?;
        let result = parse_expr(parser)?;
        when_clauses.push(WhenClause { condition, result });
    }

    let mut else_result = None;
    if parser.at_keyword(Keyword::Else) {
        let _ = parser.next();
        else_result = Some(Box::new(parse_expr(parser)?));
    }

    parser.expect_keyword(Keyword::End)?;
    Ok(Expr::Case {
        operand,
        when_clauses,
        else_result,
    })
}

pub fn parse_cast(parser: &mut Parser) -> ParseResult<Expr> {
    parser.expect_lparen()?;
    let expr = parse_expr(parser)?;
    parser.expect_keyword(Keyword::As)?;
    let target = parse_data_type(parser)?;
    parser.expect_rparen()?;
    Ok(Expr::Cast {
        expr: Box::new(expr),
        target,
    })
}

pub fn parse_convert(parser: &mut Parser) -> ParseResult<Expr> {
    parser.expect_lparen()?;
    let target = parse_data_type(parser)?;
    parser.expect_comma()?;
    let expr = parse_expr(parser)?;
    let mut style = None;
    if matches!(parser.peek(), Some(Token::Comma)) {
        let _ = parser.next();
        if let Some(Token::Number { value: s, .. }) = parser.next() {
            style = Some(*s as i32);
        } else {
            return parser.backtrack(Expected::Description("number"));
        }
    }
    parser.expect_rparen()?;
    Ok(Expr::Convert {
        target,
        expr: Box::new(expr),
        style,
    })
}

pub fn parse_try_cast(parser: &mut Parser) -> ParseResult<Expr> {
    parser.expect_lparen()?;
    let expr = parse_expr(parser)?;
    parser.expect_keyword(Keyword::As)?;
    let target = parse_data_type(parser)?;
    parser.expect_rparen()?;
    Ok(Expr::TryCast {
        expr: Box::new(expr),
        target,
    })
}

pub fn parse_try_convert(parser: &mut Parser) -> ParseResult<Expr> {
    parser.expect_lparen()?;
    let target = parse_data_type(parser)?;
    parser.expect_comma()?;
    let expr = parse_expr(parser)?;
    let mut style = None;
    if matches!(parser.peek(), Some(Token::Comma)) {
        let _ = parser.next();
        if let Some(Token::Number { value: s, .. }) = parser.next() {
            style = Some(*s as i32);
        } else {
            return parser.backtrack(Expected::Description("number"));
        }
    }
    parser.expect_rparen()?;
    Ok(Expr::TryConvert {
        target,
        expr: Box::new(expr),
        style,
    })
}

fn parse_identifier_or_function(parser: &mut Parser, name: String) -> ParseResult<Expr> {
    let mut parts = vec![name];
    while matches!(parser.peek(), Some(Token::Dot)) {
        let _ = parser.next();
        if let Some(tok) = parser.next() {
            match tok {
                Token::Identifier(next_id) => parts.push(next_id.clone()),
                Token::Keyword(k) => parts.push(k.as_ref().to_string()),
                Token::Star => {
                    return Ok(Expr::QualifiedWildcard(parts));
                }
                _ => return parser.backtrack(Expected::Description("identifier")),
            }
        } else {
            return parser.backtrack(Expected::Description("identifier"));
        }
    }

    if matches!(parser.peek(), Some(Token::LParen)) {
        let _ = parser.next();
        let mut has_distinct = false;
        if parser.at_keyword(Keyword::Distinct) {
            let _ = parser.next();
            has_distinct = true;
        }
        let args = if matches!(parser.peek(), Some(Token::Star)) {
            let _ = parser.next();
            vec![Expr::Wildcard]
        } else if matches!(parser.peek(), Some(Token::RParen)) {
            vec![]
        } else {
            parse_comma_list(parser, parse_expr)?
        };
        parser.expect_rparen()?;
        let mut function_name = if parts.len() == 1 {
            parts.remove(0)
        } else {
            parts
                .iter()
                .map(|p| p.as_ref())
                .collect::<Vec<_>>()
                .join(".")
        };
        if has_distinct {
            function_name = format!("{}_DISTINCT", function_name.to_uppercase());
        }
        if parser.at_keyword(Keyword::Over) {
            return parse_window_over(parser, function_name, args);
        }
        let within_group = if parser.at_keyword(Keyword::Within) {
            let _ = parser.next();
            parser.expect_keyword(Keyword::Group)?;
            parser.expect_lparen()?;
            parser.expect_keyword(Keyword::Order)?;
            parser.expect_keyword(Keyword::By)?;
            let order_by = parse_comma_list(
                parser,
                crate::parser::parse::statements::query::parse_order_by_expr,
            )?;
            parser.expect_rparen()?;
            order_by
        } else {
            vec![]
        };
        Ok(Expr::FunctionCall {
            name: function_name,
            args,
            within_group,
        })
    } else if parts.len() == 1 {
        let upper = parts[0].to_uppercase();
        if matches!(
            upper.as_str(),
            "CURRENT_TIMESTAMP"
                | "CURRENT_DATE"
                | "CURRENT_TIME"
                | "CURRENT_USER"
                | "SESSION_USER"
                | "SYSTEM_USER"
                | "GETDATE"
        ) {
            Ok(Expr::FunctionCall {
                name: parts.remove(0),
                args: vec![],
                within_group: vec![],
            })
        } else {
            Ok(Expr::Identifier(parts.remove(0)))
        }
    } else if parts.len() > 1 {
        Ok(Expr::QualifiedIdentifier(parts))
    } else {
        Ok(Expr::Identifier(parts.remove(0)))
    }
}
