use crate::parser::ast::*;
use crate::parser::error::ParseResult;
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

use super::common::parse_comma_list;
use super::primary::parse_primary;

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
                    if l_bp < min_bp {
                        break;
                    }
                    let _ = parser.next();
                    let mut negated = false;
                    if parser.at_keyword(Keyword::Not) {
                        let _ = parser.next();
                        negated = true;
                    }
                    parser.expect_keyword(Keyword::Null)?;
                    left = if negated {
                        Expr::IsNotNull(Box::new(left))
                    } else {
                        Expr::IsNull(Box::new(left))
                    };
                    continue;
                }
                if *k == Keyword::Like {
                    let (l_bp, r_bp) = infix_binding_power_special("LIKE");
                    if l_bp < min_bp {
                        break;
                    }
                    let _ = parser.next();
                    let pattern = parse_pratt_expr(parser, r_bp)?;
                    let mut escape = None;
                    if parser.at_keyword(Keyword::Escape) {
                        let _ = parser.next();
                        escape = Some(Box::new(parse_pratt_expr(parser, r_bp)?));
                    }
                    left = Expr::Like {
                        expr: Box::new(left),
                        pattern: Box::new(pattern),
                        escape,
                        negated: false,
                    };
                    continue;
                }
                if *k == Keyword::Between {
                    let (l_bp, _) = infix_binding_power_special("BETWEEN");
                    if l_bp < min_bp {
                        break;
                    }
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
                    if l_bp < min_bp {
                        break;
                    }
                    let _ = parser.next();
                    parser.expect_lparen()?;
                    if parser.at_keyword(Keyword::Select) {
                        let subquery = Box::new(
                            crate::parser::parse::statements::query::parse_select(parser)?,
                        );
                        parser.expect_rparen()?;
                        left = Expr::InSubquery {
                            expr: Box::new(left),
                            subquery,
                            negated: false,
                        };
                    } else {
                        let list = parse_comma_list(parser, super::parse_expr)?;
                        parser.expect_rparen()?;
                        left = Expr::InList {
                            expr: Box::new(left),
                            list,
                            negated: false,
                        };
                    }
                    continue;
                }
                if *k == Keyword::Not {
                    if let Some(Token::Keyword(k2)) = parser.peek_at(1) {
                        if *k2 == Keyword::Like {
                            let (l_bp, r_bp) = infix_binding_power_special("LIKE");
                            if l_bp < min_bp {
                                break;
                            }
                            let _ = parser.next();
                            let _ = parser.next();
                            let pattern = parse_pratt_expr(parser, r_bp)?;
                            let mut escape = None;
                            if parser.at_keyword(Keyword::Escape) {
                                let _ = parser.next();
                                escape = Some(Box::new(parse_pratt_expr(parser, r_bp)?));
                            }
                            left = Expr::Like {
                                expr: Box::new(left),
                                pattern: Box::new(pattern),
                                escape,
                                negated: true,
                            };
                            continue;
                        }
                        if *k2 == Keyword::Between {
                            let (l_bp, _) = infix_binding_power_special("BETWEEN");
                            if l_bp < min_bp {
                                break;
                            }
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
                            if l_bp < min_bp {
                                break;
                            }
                            let _ = parser.next();
                            let _ = parser.next();
                            parser.expect_lparen()?;
                            if parser.at_keyword(Keyword::Select) {
                                let subquery = Box::new(
                                    crate::parser::parse::statements::query::parse_select(parser)?,
                                );
                                parser.expect_rparen()?;
                                left = Expr::InSubquery {
                                    expr: Box::new(left),
                                    subquery,
                                    negated: true,
                                };
                            } else {
                                let list = parse_comma_list(parser, super::parse_expr)?;
                                parser.expect_rparen()?;
                                left = Expr::InList {
                                    expr: Box::new(left),
                                    list,
                                    negated: true,
                                };
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
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Gt
        | BinaryOp::Lt
        | BinaryOp::Gte
        | BinaryOp::Lte
        | BinaryOp::Like => (5, 6),
        BinaryOp::BitwiseAnd | BinaryOp::BitwiseOr | BinaryOp::BitwiseXor => (7, 8),
        BinaryOp::Add | BinaryOp::Subtract => (9, 10),
        BinaryOp::Multiply | BinaryOp::Divide | BinaryOp::Modulo => (11, 12),
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
