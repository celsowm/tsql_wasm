use crate::parser::ast::*;
use crate::parser::error::{Expected, ParseResult};
use crate::parser::state::Parser;
use crate::parser::token::Keyword;

use super::common::parse_comma_list;
use super::parse_expr;

pub(crate) fn parse_window_over(parser: &mut Parser, name: String, args: Vec<Expr>) -> ParseResult<Expr> {
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
        order_by = parse_comma_list(
            parser,
            crate::parser::parse::statements::query::parse_order_by_expr,
        )?;
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
    Ok(Expr::WindowFunction {
        name,
        args,
        partition_by,
        order_by,
        frame,
    })
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
