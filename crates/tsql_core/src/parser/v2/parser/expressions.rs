use crate::parser::v2::ast::*;
use winnow::prelude::*;
use winnow::error::{ErrMode, ContextError};
use std::borrow::Cow;

pub fn parse_expr<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    parse_pratt_expr(input, 0)
}

fn parse_pratt_expr<'a>(input: &mut &'a [Token<'a>], min_bp: u8) -> ModalResult<Expr<'a>> {
    let mut left = parse_primary(input)?;

    loop {
        // Handle Postfix operators first (IS NULL, IS NOT NULL)
        if let Some(Token::Keyword(k)) = peek_token(input) {
            if k.eq_ignore_ascii_case("IS") {
                let (l_bp, ()) = postfix_binding_power(k);
                if l_bp < min_bp { break; }
                let _ = next_token(input); // IS
                let mut negated = false;
                if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("NOT")) {
                    let _ = next_token(input);
                    negated = true;
                }
                expect_keyword(input, "NULL")?;
                left = if negated { Expr::IsNotNull(Box::new(left)) } else { Expr::IsNull(Box::new(left)) };
                continue;
            }
            if k.eq_ignore_ascii_case("LIKE") {
                let (l_bp, r_bp) = infix_binding_power_special("LIKE");
                if l_bp < min_bp { break; }
                let _ = next_token(input);
                let pattern = parse_pratt_expr(input, r_bp)?;
                left = Expr::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: false,
                };
                continue;
            }
            if k.eq_ignore_ascii_case("BETWEEN") {
                let (l_bp, _) = infix_binding_power_special("BETWEEN");
                if l_bp < min_bp { break; }
                let _ = next_token(input); // BETWEEN
                let low = parse_pratt_expr(input, 6)?;
                expect_keyword(input, "AND")?;
                let high = parse_pratt_expr(input, 6)?;
                left = Expr::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: false,
                };
                continue;
            }
            if k.eq_ignore_ascii_case("IN") {
                let (l_bp, _) = infix_binding_power_special("IN");
                if l_bp < min_bp { break; }
                let _ = next_token(input); // IN
                expect_punctuation(input, Token::LParen)?;
                if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("SELECT")) {
                    let subquery = Box::new(crate::parser::v2::parser::statements::query::parse_select(input)?);
                    expect_punctuation(input, Token::RParen)?;
                    left = Expr::InSubquery { expr: Box::new(left), subquery, negated: false };
                } else {
                    let list = parse_comma_list(input, parse_expr)?;
                    expect_punctuation(input, Token::RParen)?;
                    left = Expr::InList { expr: Box::new(left), list, negated: false };
                }
                continue;
            }
            if k.eq_ignore_ascii_case("NOT") {
                // Peek next to see if it's LIKE, BETWEEN, or IN
                let mut temp = *input;
                let _ = next_token(&mut temp);
                if let Some(Token::Keyword(k2)) = peek_token(&temp) {
                    if k2.eq_ignore_ascii_case("LIKE") {
                         let (l_bp, r_bp) = infix_binding_power_special("LIKE");
                         if l_bp < min_bp { break; }
                         let _ = next_token(input); // NOT
                         let _ = next_token(input); // LIKE
                         let pattern = parse_pratt_expr(input, r_bp)?;
                         left = Expr::Like {
                             expr: Box::new(left),
                             pattern: Box::new(pattern),
                             negated: true,
                         };
                         continue;
                    }
                    if k2.eq_ignore_ascii_case("BETWEEN") {
                         let (l_bp, _) = infix_binding_power_special("BETWEEN");
                         if l_bp < min_bp { break; }
                         let _ = next_token(input); // NOT
                         let _ = next_token(input); // BETWEEN
                         let low = parse_pratt_expr(input, 6)?;
                         expect_keyword(input, "AND")?;
                         let high = parse_pratt_expr(input, 6)?;
                         left = Expr::Between {
                             expr: Box::new(left),
                             low: Box::new(low),
                             high: Box::new(high),
                             negated: true,
                         };
                         continue;
                    }
                    if k2.eq_ignore_ascii_case("IN") {
                         let (l_bp, _) = infix_binding_power_special("IN");
                         if l_bp < min_bp { break; }
                         let _ = next_token(input); // NOT
                         let _ = next_token(input); // IN
                         expect_punctuation(input, Token::LParen)?;
                         if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("SELECT")) {
                             let subquery = Box::new(crate::parser::v2::parser::statements::query::parse_select(input)?);
                             expect_punctuation(input, Token::RParen)?;
                             left = Expr::InSubquery { expr: Box::new(left), subquery, negated: true };
                         } else {
                             let list = parse_comma_list(input, parse_expr)?;
                             expect_punctuation(input, Token::RParen)?;
                             left = Expr::InList { expr: Box::new(left), list, negated: true };
                         }
                         continue;
                    }
                }
            }
        }

        let op = match peek_token(input) {
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
            Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("AND") => BinaryOp::And,
            Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("OR") => BinaryOp::Or,
            _ => break,
        };

        let (l_bp, r_bp) = infix_binding_power(&op);
        if l_bp < min_bp {
            break;
        }

        let _ = next_token(input);
        let right = parse_pratt_expr(input, r_bp)?;
        left = Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }

    Ok(left)
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

fn postfix_binding_power(_op: &str) -> (u8, ()) {
    (11, ()) // Higher than most binary ops
}

pub fn parse_primary<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    match peek_token(input) {
        Some(Token::Number(n)) => {
             let n = *n;
             let _ = next_token(input);
             if n.fract() == 0.0 {
                 Ok(Expr::Integer(n as i64))
             } else {
                 let val: f64 = n;
                 Ok(Expr::Float(val.to_bits()))
             }
        }
        Some(Token::String(s)) => {
            let s = s.clone();
            let _ = next_token(input);
            Ok(Expr::String(s))
        }
        Some(Token::Variable(v)) => {
            let v = v.clone();
            let _ = next_token(input);
            Ok(Expr::Variable(v))
        }
        Some(Token::BinaryLiteral(hex)) => {
            let hex = hex.clone();
            let _ = next_token(input);
            let hex_str = if hex.starts_with("0x") || hex.starts_with("0X") { &hex[2..] } else { hex.as_ref() };
            let bytes = (0..hex_str.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex_str[i..std::cmp::min(i + 2, hex_str.len())], 16).unwrap_or(0))
                .collect();
            Ok(Expr::BinaryLiteral(bytes))
        }
        Some(Token::Tilde) => {
            let _ = next_token(input);
            let expr = parse_pratt_expr(input, 12)?;
            Ok(Expr::Unary { op: UnaryOp::BitwiseNot, expr: Box::new(expr) })
        }
        Some(Token::Identifier(id)) => {
            let id = id.clone();
            let _ = next_token(input);
            parse_identifier_or_function(input, id)
        }
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("NULL") => {
            let _ = next_token(input);
            Ok(Expr::Null)
        }
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("CASE") => {
            let _ = next_token(input);
            parse_case(input)
        }
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("EXISTS") => {
            let _ = next_token(input);
            expect_punctuation(input, Token::LParen)?;
            let subquery = Box::new(crate::parser::v2::parser::statements::query::parse_select(input)?);
            expect_punctuation(input, Token::RParen)?;
            Ok(Expr::Exists { subquery, negated: false })
        }
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("CAST") => {
            let _ = next_token(input);
            parse_cast(input)
        }
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("CONVERT") => {
            let _ = next_token(input);
            parse_convert(input)
        }
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("TRY_CAST") => {
            let _ = next_token(input);
            parse_try_cast(input)
        }
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("TRY_CONVERT") => {
            let _ = next_token(input);
            parse_try_convert(input)
        }
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("NOT") => {
             let _ = next_token(input);
             if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("EXISTS")) {
                 let _ = next_token(input); // EXISTS
                 expect_punctuation(input, Token::LParen)?;
                 let subquery = Box::new(crate::parser::v2::parser::statements::query::parse_select(input)?);
                 expect_punctuation(input, Token::RParen)?;
                 Ok(Expr::Exists { subquery, negated: true })
             } else {
                 let expr = parse_pratt_expr(input, 12)?; // High BP
                 Ok(Expr::Unary { op: UnaryOp::Not, expr: Box::new(expr) })
             }
        }
        Some(Token::Keyword(k)) if matches!(peek_second_token(input), Some(Token::LParen)) => {
            let name = k.clone();
            let _ = next_token(input);
            parse_identifier_or_function(input, name)
        }
        Some(Token::Operator(op)) if op.as_ref() == "-" => {
             let _ = next_token(input);
             let expr = parse_pratt_expr(input, 12)?;
             Ok(Expr::Unary { op: UnaryOp::Negate, expr: Box::new(expr) })
        }
        Some(Token::LParen) => {
            let _ = next_token(input);
            let expr = parse_expr(input)?;
            expect_punctuation(input, Token::RParen)?;
            Ok(expr)
        }
        Some(Token::Star) => {
            let _ = next_token(input);
            Ok(Expr::Wildcard)
        }
        _ => {
            Err(ErrMode::Backtrack(ContextError::new()))
        },
    }
}

pub fn parse_case<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    let mut operand = None;
    if !matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("WHEN")) {
        operand = Some(Box::new(parse_expr(input)?));
    }

    let mut when_clauses = Vec::new();
    while matches!(peek_token(input), Some(Token::Keyword(k)) if k.eq_ignore_ascii_case("WHEN")) {
        let _ = next_token(input);
        let condition = parse_expr(input)?;
        expect_keyword(input, "THEN")?;
        let result = parse_expr(input)?;
        when_clauses.push(WhenClause { condition, result });
    }

    let mut else_result = None;
    if let Some(Token::Keyword(k)) = peek_token(input) {
        if k.eq_ignore_ascii_case("ELSE") {
            let _ = next_token(input);
            else_result = Some(Box::new(parse_expr(input)?));
        }
    }

    expect_keyword(input, "END")?;
    Ok(Expr::Case { operand, when_clauses, else_result })
}

pub fn parse_cast<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    expect_punctuation(input, Token::LParen)?;
    let expr = parse_expr(input)?;
    expect_keyword(input, "AS")?;
    let target = parse_data_type(input)?;
    expect_punctuation(input, Token::RParen)?;
    Ok(Expr::Cast { expr: Box::new(expr), target })
}

pub fn parse_convert<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    expect_punctuation(input, Token::LParen)?;
    let target = parse_data_type(input)?;
    expect_punctuation(input, Token::Comma)?;
    let expr = parse_expr(input)?;
    let mut style = None;
    if let Some(Token::Comma) = peek_token(input) {
        let _ = next_token(input);
        if let Some(Token::Number(s)) = next_token(input) {
            style = Some(*s as i32);
        } else {
             return Err(ErrMode::Backtrack(ContextError::new()));
        }
    }
    expect_punctuation(input, Token::RParen)?;
    Ok(Expr::Convert { target, expr: Box::new(expr), style })
}

pub fn parse_try_cast<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    expect_punctuation(input, Token::LParen)?;
    let expr = parse_expr(input)?;
    expect_keyword(input, "AS")?;
    let target = parse_data_type(input)?;
    expect_punctuation(input, Token::RParen)?;
    Ok(Expr::TryCast { expr: Box::new(expr), target })
}

pub fn parse_try_convert<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<Expr<'a>> {
    expect_punctuation(input, Token::LParen)?;
    let target = parse_data_type(input)?;
    expect_punctuation(input, Token::Comma)?;
    let expr = parse_expr(input)?;
    let mut style = None;
    if let Some(Token::Comma) = peek_token(input) {
        let _ = next_token(input);
        if let Some(Token::Number(s)) = next_token(input) {
            style = Some(*s as i32);
        } else {
             return Err(ErrMode::Backtrack(ContextError::new()));
        }
    }
    expect_punctuation(input, Token::RParen)?;
    Ok(Expr::TryConvert { target, expr: Box::new(expr), style })
}

fn parse_identifier_or_function<'a>(input: &mut &'a [Token<'a>], name: Cow<'a, str>) -> ModalResult<Expr<'a>> {
    if let Some(Token::LParen) = peek_token(input) {
        let _ = next_token(input);
        let args = if let Some(Token::Star) = peek_token(input) {
            let _ = next_token(input);
            vec![Expr::Wildcard]
        } else if matches!(peek_token(input), Some(Token::RParen)) {
            vec![]
        } else {
            parse_comma_list(input, parse_expr)?
        };
        expect_punctuation(input, Token::RParen)?;
        if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("OVER")) {
            return parse_window_over(input, name, args);
        }
        Ok(Expr::FunctionCall { name, args })
    } else {
        let mut parts = vec![name];
        while let Some(Token::Dot) = peek_token(input) {
            let _ = next_token(input);
            if let Some(tok) = next_token(input) {
                match tok {
                    Token::Identifier(next_id) => parts.push(next_id.clone()),
                    Token::Keyword(next_id) => parts.push(next_id.clone()),
                    _ => return Err(ErrMode::Backtrack(ContextError::new())),
                }
            } else {
                return Err(ErrMode::Backtrack(ContextError::new()));
            }
        }
        if parts.len() > 1 {
            Ok(Expr::QualifiedIdentifier(parts))
        } else {
            Ok(Expr::Identifier(parts.remove(0)))
        }
    }
}

fn parse_window_over<'a>(
    input: &mut &'a [Token<'a>],
    name: Cow<'a, str>,
    args: Vec<Expr<'a>>,
) -> ModalResult<Expr<'a>> {
    expect_keyword(input, "OVER")?;
    expect_punctuation(input, Token::LParen)?;

    let mut partition_by = Vec::new();
    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("PARTITION")) {
        let _ = next_token(input); // PARTITION
        expect_keyword(input, "BY")?;
        partition_by = parse_comma_list(input, parse_expr)?;
    }

    let mut order_by = Vec::new();
    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("ORDER")) {
        let _ = next_token(input); // ORDER
        expect_keyword(input, "BY")?;
        order_by = parse_comma_list(input, crate::parser::v2::parser::statements::query::parse_order_by_expr)?;
    }

    let mut frame = None;
    if let Some(Token::Keyword(kw)) = peek_token(input) {
        let upper = kw.to_uppercase();
        if matches!(upper.as_str(), "ROWS" | "RANGE" | "GROUPS") {
            let units = match upper.as_str() {
                "ROWS" => WindowFrameUnits::Rows,
                "RANGE" => WindowFrameUnits::Range,
                "GROUPS" => WindowFrameUnits::Groups,
                _ => unreachable!(),
            };
            let _ = next_token(input);
            let extent = if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("BETWEEN")) {
                let _ = next_token(input); // BETWEEN
                let start = parse_window_frame_bound(input)?;
                expect_keyword(input, "AND")?;
                let end = parse_window_frame_bound(input)?;
                WindowFrameExtent::Between(start, end)
            } else {
                let bound = parse_window_frame_bound(input)?;
                WindowFrameExtent::Bound(bound)
            };
            frame = Some(WindowFrame::new(units, extent));
        }
    }

    expect_punctuation(input, Token::RParen)?;
    Ok(Expr::WindowFunction { name, args, partition_by, order_by, frame })
}

fn parse_window_frame_bound<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<WindowFrameBound> {
    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("UNBOUNDED")) {
        let _ = next_token(input);
        if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("PRECEDING")) {
            let _ = next_token(input);
            return Ok(WindowFrameBound::UnboundedPreceding);
        }
        if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("FOLLOWING")) {
            let _ = next_token(input);
            return Ok(WindowFrameBound::UnboundedFollowing);
        }
        return Err(ErrMode::Backtrack(ContextError::new()));
    }
    if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("CURRENT")) {
        let _ = next_token(input);
        expect_keyword(input, "ROW")?;
        return Ok(WindowFrameBound::CurrentRow);
    }
    if let Some(Token::Number(n)) = peek_token(input) {
        let n = *n as i64;
        let _ = next_token(input);
        if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("PRECEDING")) {
            let _ = next_token(input);
            return Ok(WindowFrameBound::Preceding(Some(n)));
        }
        if matches!(peek_token(input), Some(Token::Keyword(kw)) if kw.eq_ignore_ascii_case("FOLLOWING")) {
            let _ = next_token(input);
            return Ok(WindowFrameBound::Following(Some(n)));
        }
    }
    Err(ErrMode::Backtrack(ContextError::new()))
}

pub fn peek_second_token<'a>(input: &&'a [Token<'a>]) -> Option<&'a Token<'a>> {
    input.get(1)
}

pub fn parse_comma_list<'a, P, R>(input: &mut &'a [Token<'a>], mut parser: P) -> ModalResult<Vec<R>>
where P: FnMut(&mut &'a [Token<'a>]) -> ModalResult<R>
{
    let mut results = Vec::new();
    results.push(parser(input)?);
    
    loop {
        if let Some(Token::Comma) = peek_token(input) {
            let _ = next_token(input);
            results.push(parser(input)?);
            continue;
        }
        break;
    }
    Ok(results)
}

pub fn is_stop_keyword(k: &str) -> bool {
    let upper = k.to_uppercase();
    matches!(upper.as_str(), "WHERE" | "GROUP" | "ORDER" | "HAVING" | "ELSE" | "END" | "IF" | "DECLARE" | "SET" | "EXEC" | "EXECUTE" | "PRINT" | "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "GO" | "FROM" | "JOIN" | "ON" | "UNION" | "INTERSECT" | "EXCEPT" | "CROSS" | "APPLY" | "OUTER" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "PIVOT" | "UNPIVOT")
}

pub fn parse_data_type<'a>(input: &mut &'a [Token<'a>]) -> ModalResult<DataType<'a>> {
    match next_token(input) {
        Some(Token::Identifier(id)) | Some(Token::Keyword(id)) => {
            let upper = id.to_uppercase();
            match upper.as_str() {
                "INT" => Ok(DataType::Int),
                "BIGINT" => Ok(DataType::BigInt),
                "SMALLINT" => Ok(DataType::SmallInt),
                "TINYINT" => Ok(DataType::TinyInt),
                "BIT" => Ok(DataType::Bit),
                "FLOAT" => Ok(DataType::Float),
                "DECIMAL" | "NUMERIC" => {
                    let mut p = 18;
                    let mut s = 0;
                    if let Some(Token::LParen) = peek_token(input) {
                        let _ = next_token(input);
                        if let Some(Token::Number(val)) = next_token(input) {
                            p = *val as u8;
                        }
                        if let Some(Token::Comma) = peek_token(input) {
                            let _ = next_token(input);
                            if let Some(Token::Number(val)) = next_token(input) {
                                s = *val as u8;
                            }
                        }
                        expect_punctuation(input, Token::RParen)?;
                    }
                    if upper == "DECIMAL" { Ok(DataType::Decimal(p, s)) } else { Ok(DataType::Numeric(p, s)) }
                }
                "VARCHAR" => {
                    let mut size = None;
                    if let Some(Token::LParen) = peek_token(input) {
                        let _ = next_token(input);
                        if let Some(Token::Number(s)) = next_token(input) {
                            size = Some(*s as u32);
                        }
                        expect_punctuation(input, Token::RParen)?;
                    }
                    Ok(DataType::VarChar(size))
                }
                "NVARCHAR" => {
                    let mut size = None;
                    if let Some(Token::LParen) = peek_token(input) {
                        let _ = next_token(input);
                        if let Some(Token::Number(s)) = next_token(input) {
                            size = Some(*s as u32);
                        }
                        expect_punctuation(input, Token::RParen)?;
                    }
                    Ok(DataType::NVarChar(size))
                }
                "SYSNAME" => Ok(DataType::NVarChar(Some(128))),
                _ => Ok(DataType::Custom(id.clone())),
            }
        }
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}

pub fn is_same_token<'a>(a: &Token<'a>, b: &Token<'a>) -> bool {
    match (a, b) {
        (Token::Keyword(k1), Token::Keyword(k2)) => k1.eq_ignore_ascii_case(k2),
        (Token::Identifier(i1), Token::Identifier(i2)) => i1 == i2,
        (Token::Variable(v1), Token::Variable(v2)) => v1 == v2,
        (Token::String(s1), Token::String(s2)) => s1 == s2,
        (Token::Number(n1), Token::Number(n2)) => n1 == n2,
        (Token::Operator(o1), Token::Operator(o2)) => o1 == o2,
        _ => core::mem::discriminant(a) == core::mem::discriminant(b),
    }
}

pub fn peek_token<'a>(input: &&'a [Token<'a>]) -> Option<&'a Token<'a>> {
    input.first()
}

pub fn next_token<'a>(input: &mut &'a [Token<'a>]) -> Option<&'a Token<'a>> {
    let tok = input.first()?;
    *input = &input[1..];
    Some(tok)
}

pub fn expect_keyword<'a>(input: &mut &'a [Token<'a>], kw: &str) -> ModalResult<()> {
    match next_token(input) {
        Some(Token::Keyword(k)) if k.eq_ignore_ascii_case(kw) => Ok(()),
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}

pub fn expect_punctuation<'a>(input: &mut &'a [Token<'a>], expected: Token<'a>) -> ModalResult<()> {
    match next_token(input) {
        Some(tok) if is_same_token(tok, &expected) => Ok(()),
        _ => Err(ErrMode::Backtrack(ContextError::new())),
    }
}
