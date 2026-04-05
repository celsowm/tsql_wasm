use crate::parser::ast::*;
use crate::parser::state::Parser;
use crate::parser::error::{ParseResult, Expected};
use crate::parser::token::Keyword;

pub fn parse_expr(parser: &mut Parser) -> ParseResult<Expr> {
    parse_pratt_expr(parser, 0)
}

fn parse_pratt_expr(parser: &mut Parser, min_bp: u8) -> ParseResult<Expr> {
    parser.enter_recursion()?;
    let res = (|| {
        let mut left = parse_primary(parser)?;

        loop {
            if let Some(Token::Keyword(k)) = parser.peek() {
                if *k == Keyword::Is {
                    let (l_bp, ()) = postfix_binding_power(k);
                    if l_bp < min_bp { break; }
                    let _ = parser.next();
                    let mut negated = false;
                    if parser.at_keyword(Keyword::Not) {
                        let _ = parser.next();
                        negated = true;
                    }
                    parser.expect_keyword(Keyword::Null)?;
                    left = if negated { Expr::IsNotNull(Box::new(left)) } else { Expr::IsNull(Box::new(left)) };
                    continue;
                }
                if *k == Keyword::Like {
                    let (l_bp, r_bp) = infix_binding_power_special("LIKE");
                    if l_bp < min_bp { break; }
                    let _ = parser.next();
                    let pattern = parse_pratt_expr(parser, r_bp)?;
                    left = Expr::Like {
                        expr: Box::new(left),
                        pattern: Box::new(pattern),
                        negated: false,
                    };
                    continue;
                }
                if *k == Keyword::Between {
                    let (l_bp, _) = infix_binding_power_special("BETWEEN");
                    if l_bp < min_bp { break; }
                    let _ = parser.next();
                    let low = parse_pratt_expr(parser, 6)?;
                    parser.expect_keyword(Keyword::And)?;
                    let high = parse_pratt_expr(parser, 6)?;
                    left = Expr::Between {
                        expr: Box::new(left),
                        low: Box::new(low),
                        high: Box::new(high),
                        negated: false,
                    };
                    continue;
                }
                if *k == Keyword::In {
                    let (l_bp, _) = infix_binding_power_special("IN");
                    if l_bp < min_bp { break; }
                    let _ = parser.next();
                    parser.expect_lparen()?;
                    if parser.at_keyword(Keyword::Select) {
                        let subquery = Box::new(crate::parser::parse::statements::query::parse_select(parser)?);
                        parser.expect_rparen()?;
                        left = Expr::InSubquery { expr: Box::new(left), subquery, negated: false };
                    } else {
                        let list = parse_comma_list(parser, parse_expr)?;
                        parser.expect_rparen()?;
                        left = Expr::InList { expr: Box::new(left), list, negated: false };
                    }
                    continue;
                }
                if *k == Keyword::Not {
                    if let Some(Token::Keyword(k2)) = parser.peek_at(1) {
                        if *k2 == Keyword::Like {
                             let (l_bp, r_bp) = infix_binding_power_special("LIKE");
                             if l_bp < min_bp { break; }
                             let _ = parser.next();
                             let _ = parser.next();
                             let pattern = parse_pratt_expr(parser, r_bp)?;
                             left = Expr::Like {
                                 expr: Box::new(left),
                                 pattern: Box::new(pattern),
                                 negated: true,
                             };
                             continue;
                        }
                        if *k2 == Keyword::Between {
                             let (l_bp, _) = infix_binding_power_special("BETWEEN");
                             if l_bp < min_bp { break; }
                             let _ = parser.next();
                             let _ = parser.next();
                             let low = parse_pratt_expr(parser, 6)?;
                             parser.expect_keyword(Keyword::And)?;
                             let high = parse_pratt_expr(parser, 6)?;
                             left = Expr::Between {
                                 expr: Box::new(left),
                                 low: Box::new(low),
                                 high: Box::new(high),
                                 negated: true,
                             };
                             continue;
                        }
                        if *k2 == Keyword::In {
                             let (l_bp, _) = infix_binding_power_special("IN");
                             if l_bp < min_bp { break; }
                             let _ = parser.next();
                             let _ = parser.next();
                             parser.expect_lparen()?;
                             if parser.at_keyword(Keyword::Select) {
                                 let subquery = Box::new(crate::parser::parse::statements::query::parse_select(parser)?);
                                 parser.expect_rparen()?;
                                 left = Expr::InSubquery { expr: Box::new(left), subquery, negated: true };
                             } else {
                                 let list = parse_comma_list(parser, parse_expr)?;
                                 parser.expect_rparen()?;
                                 left = Expr::InList { expr: Box::new(left), list, negated: true };
                             }
                             continue;
                        }
                    }
                }
            }

            let op = match parser.peek() {
                Some(Token::Operator(op_str)) => match op_str.as_ref() {
                    "=" => BinaryOp::Eq,
                    "<>" | "!=" => BinaryOp::NotEq,
                    ">" => BinaryOp::Gt,
                    "<" => BinaryOp::Lt,
                    ">=" => BinaryOp::Gte,
                    "<=" => BinaryOp::Lte,
                    "+" => BinaryOp::Add,
                    "-" => BinaryOp::Subtract,
                    "/" => BinaryOp::Divide,
                    "%" => BinaryOp::Modulo,
                    "&" => BinaryOp::BitwiseAnd,
                    "|" => BinaryOp::BitwiseOr,
                    "^" => BinaryOp::BitwiseXor,
                    _ => break,
                },
                Some(Token::Star) => BinaryOp::Multiply,
                Some(Token::Keyword(k)) if *k == Keyword::And => BinaryOp::And,
                Some(Token::Keyword(k)) if *k == Keyword::Or => BinaryOp::Or,
                _ => break,
            };

            let (l_bp, r_bp) = infix_binding_power(&op);
            if l_bp < min_bp {
                break;
            }

            let _ = parser.next();
            let right = parse_pratt_expr(parser, r_bp)?;
            left = Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    })();
    parser.leave_recursion();
    res
}

fn infix_binding_power(op: &BinaryOp) -> (u8, u8) {
    match op {
        BinaryOp::Or => (1, 2),
        BinaryOp::And => (3, 4),
        BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte | BinaryOp::Like => (5, 6),
        BinaryOp::Add | BinaryOp::Subtract => (7, 8),
        BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo | BinaryOp::BitwiseAnd | BinaryOp::BitwiseOr | BinaryOp::BitwiseXor => (9, 10),
    }
}

fn infix_binding_power_special(op: &str) -> (u8, u8) {
    match op {
        "LIKE" | "BETWEEN" | "IN" => (5, 6),
        _ => (0, 0),
    }
}

fn postfix_binding_power(_op: &Keyword) -> (u8, ()) {
    (11, ())
}

pub fn parse_primary(parser: &mut Parser) -> ParseResult<Expr> {
    match parser.peek() {
        Some(Token::Number { value: n, is_float }) => {
             let n = *n;
             let is_float = *is_float;
             let _ = parser.next();
             if !is_float && n.fract() == 0.0 {
                 Ok(Expr::Integer(n as i64))
             } else {
                 let val: f64 = n;
                 Ok(Expr::Float(val.to_bits()))
             }
        }
        Some(Token::String(s)) => {
            let s = s.clone();
            let _ = parser.next();
            Ok(Expr::String(s))
        }
        Some(Token::Variable(v)) => {
            let v = v.clone();
            let _ = parser.next();
            Ok(Expr::Variable(v))
        }
        Some(Token::BinaryLiteral(hex)) => {
            let hex = hex.clone();
            let _ = parser.next();
            let hex_str = if hex.starts_with("0x") || hex.starts_with("0X") { &hex[2..] } else { hex.as_ref() };
            let bytes = (0..hex_str.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex_str[i..std::cmp::min(i + 2, hex_str.len())], 16).unwrap_or(0))
                .collect();
            Ok(Expr::BinaryLiteral(bytes))
        }
        Some(Token::Tilde) => {
            let _ = parser.next();
            let expr = parse_pratt_expr(parser, 12)?;
            Ok(Expr::Unary { op: UnaryOp::BitwiseNot, expr: Box::new(expr) })
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
        Some(Token::Keyword(k)) if *k == Keyword::Case => {
            let _ = parser.next();
            parse_case(parser)
        }
        Some(Token::Keyword(k)) if *k == Keyword::Exists => {
            let _ = parser.next();
            parser.expect_lparen()?;
            let subquery = Box::new(crate::parser::parse::statements::query::parse_select(parser)?);
            parser.expect_rparen()?;
            Ok(Expr::Exists { subquery, negated: false })
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
                 let subquery = Box::new(crate::parser::parse::statements::query::parse_select(parser)?);
                 parser.expect_rparen()?;
                 Ok(Expr::Exists { subquery, negated: true })
             } else {
                 let expr = parse_pratt_expr(parser, 12)?;
                 Ok(Expr::Unary { op: UnaryOp::Not, expr: Box::new(expr) })
             }
        }
        Some(Token::Keyword(k)) if matches!(parser.peek_at(1), Some(Token::LParen)) => {
            let name = k.as_ref().to_string();
            let _ = parser.next();
            parse_identifier_or_function(parser, name)
        }
        Some(Token::Operator(op)) if *op == "-" => {
             let _ = parser.next();
             let expr = parse_pratt_expr(parser, 12)?;
             Ok(Expr::Unary { op: UnaryOp::Negate, expr: Box::new(expr) })
        }
        Some(Token::Keyword(k)) if matches!(parser.peek_at(1), Some(Token::LParen) | Some(Token::Dot)) => {
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
            let mut open_parens = 0usize;
            while matches!(parser.peek(), Some(Token::LParen)) {
                let _ = parser.next();
                open_parens += 1;
            }
            let expr = if parser.at_keyword(Keyword::Select) {
                Expr::Subquery(Box::new(crate::parser::parse::statements::query::parse_select(parser)?))
            } else {
                parse_expr(parser)?
            };
            for _ in 0..open_parens {
                parser.expect_rparen()?;
            }
            Ok(expr)
        }
        Some(Token::Star) => {
            let _ = parser.next();
            Ok(Expr::Wildcard)
        }
        _ => {
            parser.backtrack(Expected::Description("expression"))
        },
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
    Ok(Expr::Case { operand, when_clauses, else_result })
}

pub fn parse_cast(parser: &mut Parser) -> ParseResult<Expr> {
    parser.expect_lparen()?;
    let expr = parse_expr(parser)?;
    parser.expect_keyword(Keyword::As)?;
    let target = parse_data_type(parser)?;
    parser.expect_rparen()?;
    Ok(Expr::Cast { expr: Box::new(expr), target })
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
    Ok(Expr::Convert { target, expr: Box::new(expr), style })
}

pub fn parse_try_cast(parser: &mut Parser) -> ParseResult<Expr> {
    parser.expect_lparen()?;
    let expr = parse_expr(parser)?;
    parser.expect_keyword(Keyword::As)?;
    let target = parse_data_type(parser)?;
    parser.expect_rparen()?;
    Ok(Expr::TryCast { expr: Box::new(expr), target })
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
    Ok(Expr::TryConvert { target, expr: Box::new(expr), style })
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
        let args = if matches!(parser.peek(), Some(Token::Star)) {
            let _ = parser.next();
            vec![Expr::Wildcard]
        } else if matches!(parser.peek(), Some(Token::RParen)) {
            vec![]
        } else {
            parse_comma_list(parser, parse_expr)?
        };
        parser.expect_rparen()?;
        let function_name = if parts.len() == 1 {
            parts.remove(0)
        } else {
            parts.iter().map(|p| p.as_ref()).collect::<Vec<_>>().join(".")
        };
        if parser.at_keyword(Keyword::Over) {
            return parse_window_over(parser, function_name, args);
        }
        Ok(Expr::FunctionCall { name: function_name, args })
    } else if parts.len() == 1 {
        let upper = parts[0].to_uppercase();
        if matches!(upper.as_str(), "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "GETDATE") {
            Ok(Expr::FunctionCall { name: parts.remove(0), args: vec![] })
        } else {
            Ok(Expr::Identifier(parts.remove(0)))
        }
    } else if parts.len() > 1 {
        Ok(Expr::QualifiedIdentifier(parts))
    } else {
        Ok(Expr::Identifier(parts.remove(0)))
    }
}

fn parse_window_over(
    parser: &mut Parser,
    name: String,
    args: Vec<Expr>,
) -> ParseResult<Expr> {
    parser.expect_keyword(Keyword::Over)?;
    parser.expect_lparen()?;

    let mut partition_by = Vec::new();
    if parser.at_keyword(Keyword::Partition) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::By)?;
        partition_by = parse_comma_list(parser, parse_expr)?;
    }

    let mut order_by = Vec::new();
    if parser.at_keyword(Keyword::Order) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::By)?;
        order_by = parse_comma_list(parser, crate::parser::parse::statements::query::parse_order_by_expr)?;
    }

    let mut frame = None;
    if let Some(Token::Keyword(kw)) = parser.peek() {
        match kw {
            Keyword::Rows => {
                let units = WindowFrameUnits::Rows;
                let _ = parser.next();
                let extent = if parser.at_keyword(Keyword::Between) {
                    let _ = parser.next();
                    let start = parse_window_frame_bound(parser)?;
                    parser.expect_keyword(Keyword::And)?;
                    let end = parse_window_frame_bound(parser)?;
                    WindowFrameExtent::Between(start, end)
                } else {
                    let bound = parse_window_frame_bound(parser)?;
                    WindowFrameExtent::Bound(bound)
                };
                frame = Some(WindowFrame::new(units, extent));
            }
            Keyword::Range => {
                let units = WindowFrameUnits::Range;
                let _ = parser.next();
                let extent = if parser.at_keyword(Keyword::Between) {
                    let _ = parser.next();
                    let start = parse_window_frame_bound(parser)?;
                    parser.expect_keyword(Keyword::And)?;
                    let end = parse_window_frame_bound(parser)?;
                    WindowFrameExtent::Between(start, end)
                } else {
                    let bound = parse_window_frame_bound(parser)?;
                    WindowFrameExtent::Bound(bound)
                };
                frame = Some(WindowFrame::new(units, extent));
            }
            Keyword::Groups => {
                let units = WindowFrameUnits::Groups;
                let _ = parser.next();
                let extent = if parser.at_keyword(Keyword::Between) {
                    let _ = parser.next();
                    let start = parse_window_frame_bound(parser)?;
                    parser.expect_keyword(Keyword::And)?;
                    let end = parse_window_frame_bound(parser)?;
                    WindowFrameExtent::Between(start, end)
                } else {
                    let bound = parse_window_frame_bound(parser)?;
                    WindowFrameExtent::Bound(bound)
                };
                frame = Some(WindowFrame::new(units, extent));
            }
            _ => {}
        }
    }

    parser.expect_rparen()?;
    Ok(Expr::WindowFunction { name, args, partition_by, order_by, frame })
}

fn parse_window_frame_bound(parser: &mut Parser) -> ParseResult<WindowFrameBound> {
    if parser.at_keyword(Keyword::Unbounded) {
        let _ = parser.next();
        if parser.at_keyword(Keyword::Preceding) {
            let _ = parser.next();
            return Ok(WindowFrameBound::UnboundedPreceding);
        }
        if parser.at_keyword(Keyword::Following) {
            let _ = parser.next();
            return Ok(WindowFrameBound::UnboundedFollowing);
        }
        return parser.backtrack(Expected::Description("PRECEDING or FOLLOWING"));
    }
    if parser.at_keyword(Keyword::Current) {
        let _ = parser.next();
        parser.expect_keyword(Keyword::Row)?;
        return Ok(WindowFrameBound::CurrentRow);
    }
    if let Some(Token::Number { value: n, .. }) = parser.peek() {
        let n = *n as i64;
        let _ = parser.next();
        if parser.at_keyword(Keyword::Preceding) {
            let _ = parser.next();
            return Ok(WindowFrameBound::Preceding(Some(n)));
        }
        if parser.at_keyword(Keyword::Following) {
            let _ = parser.next();
            return Ok(WindowFrameBound::Following(Some(n)));
        }
    }
    parser.backtrack(Expected::Description("window frame bound"))
}

pub fn parse_comma_list<P, R>(parser: &mut Parser, mut parser_fn: P) -> ParseResult<Vec<R>>
where P: FnMut(&mut Parser) -> ParseResult<R>
{
    let mut results = Vec::new();
    results.push(parser_fn(parser)?);

    loop {
        if matches!(parser.peek(), Some(Token::Comma)) {
            let _ = parser.next();
            results.push(parser_fn(parser)?);
            continue;
        }
        break;
    }
    Ok(results)
}

pub fn is_stop_keyword(k: &str) -> bool {
    Keyword::parse(k).map(|kw| matches!(kw,
        Keyword::Where | Keyword::Group | Keyword::Order | Keyword::Having |
        Keyword::Else | Keyword::End | Keyword::If | Keyword::Declare |
        Keyword::Set | Keyword::Exec | Keyword::Execute | Keyword::Print |
        Keyword::Select | Keyword::Insert | Keyword::Update | Keyword::Delete |
        Keyword::Go | Keyword::From | Keyword::Join | Keyword::On |
        Keyword::Union | Keyword::Intersect | Keyword::Except | Keyword::Cross |
        Keyword::Apply | Keyword::Outer | Keyword::Inner | Keyword::Left |
        Keyword::Right | Keyword::Full | Keyword::Pivot | Keyword::Unpivot |
        Keyword::Output | Keyword::With | Keyword::By | Keyword::Asc | Keyword::Desc
    )).unwrap_or(false)
}

pub fn parse_data_type(parser: &mut Parser) -> ParseResult<DataType> {
    match parser.next() {
        Some(Token::Identifier(id)) => {
            let upper = id.to_uppercase();
            match upper.as_str() {
                "INT" => Ok(DataType::Int),
                "BIGINT" => Ok(DataType::BigInt),
                "SMALLINT" => Ok(DataType::SmallInt),
                "TINYINT" => Ok(DataType::TinyInt),
                "BIT" => Ok(DataType::Bit),
                "FLOAT" => Ok(DataType::Float),
                "REAL" => Ok(DataType::Real),
                "DECIMAL" | "NUMERIC" => {
                    let mut p = 18;
                    let mut s = 0;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: val, .. }) = parser.next() {
                            p = *val as u8;
                        }
                        if matches!(parser.peek(), Some(Token::Comma)) {
                            let _ = parser.next();
                            if let Some(Token::Number { value: val, .. }) = parser.next() {
                                s = *val as u8;
                            }
                        }
                        parser.expect_rparen()?;
                    }
                    if upper == "DECIMAL" { Ok(DataType::Decimal(p, s)) } else { Ok(DataType::Numeric(p, s)) }
                }
                "CHAR" => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::Char(size))
                }
                "VARCHAR" => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::VarChar(size))
                }
                "NCHAR" => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::NChar(size))
                }
                "NVARCHAR" => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::NVarChar(size))
                }
                "BINARY" => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::Binary(size))
                }
                "VARBINARY" => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::VarBinary(size))
                }
                "MONEY" => Ok(DataType::Money),
                "SMALLMONEY" => Ok(DataType::SmallMoney),
                "UNIQUEIDENTIFIER" => Ok(DataType::UniqueIdentifier),
                "SYSNAME" => Ok(DataType::NVarChar(Some(128))),
                "DATE" => Ok(DataType::Date),
                "DATETIME" => Ok(DataType::DateTime),
                "DATETIME2" => Ok(DataType::DateTime2),
                "TIME" => Ok(DataType::Time),
                _ => {
                    let mut parts = vec![id.clone()];
                    while matches!(parser.peek(), Some(Token::Dot)) {
                        let _ = parser.next();
                        match parser.next() {
                            Some(Token::Identifier(next_id)) => parts.push(next_id.clone()),
                            Some(Token::Keyword(k)) => parts.push(k.as_ref().to_string()),
                            _ => return parser.backtrack(Expected::Description("identifier")),
                        }
                    }
                    Ok(DataType::Custom(parts.iter().map(|p| p.as_ref()).collect::<Vec<_>>().join(".")))
                }
            }
        }
        Some(Token::Keyword(kw)) => {
            match kw {
                Keyword::Int => Ok(DataType::Int),
                Keyword::BigInt => Ok(DataType::BigInt),
                Keyword::SmallInt => Ok(DataType::SmallInt),
                Keyword::TinyInt => Ok(DataType::TinyInt),
                Keyword::Bit => Ok(DataType::Bit),
                Keyword::Float => Ok(DataType::Float),
                Keyword::Real => Ok(DataType::Real),
                Keyword::Decimal | Keyword::Numeric => {
                    let is_decimal = matches!(kw, Keyword::Decimal);
                    let mut p = 18;
                    let mut s = 0;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: val, .. }) = parser.next() {
                            p = *val as u8;
                        }
                        if matches!(parser.peek(), Some(Token::Comma)) {
                            let _ = parser.next();
                            if let Some(Token::Number { value: val, .. }) = parser.next() {
                                s = *val as u8;
                            }
                        }
                        parser.expect_rparen()?;
                    }
                    if is_decimal { Ok(DataType::Decimal(p, s)) } else { Ok(DataType::Numeric(p, s)) }
                }
                Keyword::Char => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::Char(size))
                }
                Keyword::Varchar => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::VarChar(size))
                }
                Keyword::NChar => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::NChar(size))
                }
                Keyword::Nvarchar => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::NVarChar(size))
                }
                Keyword::Binary => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::Binary(size))
                }
                Keyword::Varbinary => {
                    let mut size = None;
                    if matches!(parser.peek(), Some(Token::LParen)) {
                        let _ = parser.next();
                        if let Some(Token::Number { value: s, .. }) = parser.next() {
                            size = Some(*s as u32);
                        }
                        parser.expect_rparen()?;
                    }
                    Ok(DataType::VarBinary(size))
                }
                Keyword::Money => Ok(DataType::Money),
                Keyword::SmallMoney => Ok(DataType::SmallMoney),
                Keyword::UniqueIdentifier => Ok(DataType::UniqueIdentifier),
                Keyword::SysName => Ok(DataType::NVarChar(Some(128))),
                Keyword::Date => Ok(DataType::Date),
                Keyword::DateTime => Ok(DataType::DateTime),
                Keyword::DateTime2 => Ok(DataType::DateTime2),
                Keyword::Time => Ok(DataType::Time),
                _ => Ok(DataType::Custom(kw.as_ref().to_string())),
            }
        }
        _ => parser.backtrack(Expected::Description("data type")),
    }
}
