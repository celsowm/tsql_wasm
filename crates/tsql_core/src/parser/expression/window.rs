use crate::ast::{Expr, WindowFunc, OrderByExpr};
use crate::error::DbError;
use super::ExprToken;

impl super::ExprParser {
    pub(crate) fn is_window_function(&self, name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "ROW_NUMBER"
                | "RANK"
                | "DENSE_RANK"
                | "NTILE"
                | "LAG"
                | "LEAD"
                | "FIRST_VALUE"
                | "LAST_VALUE"
                | "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
        )
    }

    pub(crate) fn parse_window_function_rest(&mut self, name: String, args: Vec<Expr>) -> Result<Expr, DbError> {
        let func_name_upper = name.to_uppercase();
        let func = match func_name_upper.as_str() {
            "ROW_NUMBER" => WindowFunc::RowNumber,
            "RANK" => WindowFunc::Rank,
            "DENSE_RANK" => WindowFunc::DenseRank,
            "NTILE" => WindowFunc::NTile,
            "LAG" => WindowFunc::Lag,
            "LEAD" => WindowFunc::Lead,
            "FIRST_VALUE" => WindowFunc::FirstValue,
            "LAST_VALUE" => WindowFunc::LastValue,
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => WindowFunc::Aggregate(func_name_upper),
            _ => return Err(DbError::Parse(format!("unknown window function: {}", name))),
        };

        self.expect(|t| matches!(t, ExprToken::LParen), "(")?;

        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut frame = None;

        if !self.match_tok(|t| matches!(t, ExprToken::RParen)) {
            loop {
                if self.match_tok(|t| matches!(t, ExprToken::RParen)) {
                    break;
                }
                if self.match_tok(|t| matches!(t, ExprToken::Partition)) {
                    self.expect(|t| matches!(t, ExprToken::By), "BY")?;
                    loop {
                        partition_by.push(self.parse_or()?);
                        if self.match_tok(|t| matches!(t, ExprToken::Comma)) {
                            continue;
                        }
                        break;
                    }
                } else if self.match_tok(|t| matches!(t, ExprToken::Order)) {
                    self.expect(|t| matches!(t, ExprToken::By), "BY")?;
                    loop {
                        let expr = self.parse_or()?;
                        let asc = if self.match_tok(|t| matches!(t, ExprToken::Desc)) {
                            false
                        } else {
                            self.match_tok(|t| matches!(t, ExprToken::Asc));
                            true
                        };
                        order_by.push(OrderByExpr { expr, asc });
                        if self.match_tok(|t| matches!(t, ExprToken::Comma)) {
                            continue;
                        }
                        break;
                    }
                } else if self.match_tok(|t| matches!(t, ExprToken::Rows | ExprToken::Range | ExprToken::Groups)) {
                    self.pos -= 1;
                    frame = Some(self.parse_window_frame()?);
                } else {
                    return Err(DbError::Parse(
                        "expected PARTITION BY, ORDER BY, or ROWS/RANGE/GROUPS in window specification"
                            .into(),
                    ));
                }
            }
        }

        Ok(Expr::WindowFunction {
            func,
            args,
            partition_by,
            order_by,
            frame,
        })
    }

    pub(crate) fn parse_window_frame(&mut self) -> Result<crate::ast::WindowFrame, DbError> {
        let units = match self.next() {
            Some(ExprToken::Rows) => crate::ast::WindowFrameUnits::Rows,
            Some(ExprToken::Range) => crate::ast::WindowFrameUnits::Range,
            Some(ExprToken::Groups) => crate::ast::WindowFrameUnits::Groups,
            _ => return Err(DbError::Parse("expected ROWS, RANGE, or GROUPS".into())),
        };

        if self.match_tok(|t| matches!(t, ExprToken::Between)) {
            let start = self.parse_window_frame_bound()?;
            self.expect(|t| matches!(t, ExprToken::And), "AND")?;
            let end = self.parse_window_frame_bound()?;
            Ok(crate::ast::WindowFrame {
                units,
                extent: crate::ast::WindowFrameExtent::Between(start, end),
            })
        } else {
            let bound = self.parse_window_frame_bound()?;
            Ok(crate::ast::WindowFrame {
                units,
                extent: crate::ast::WindowFrameExtent::Bound(bound),
            })
        }
    }

    pub(crate) fn parse_window_frame_bound(&mut self) -> Result<crate::ast::WindowFrameBound, DbError> {
        if self.match_tok(|t| matches!(t, ExprToken::Current)) {
            self.expect(|t| matches!(t, ExprToken::Row), "ROW")?;
            Ok(crate::ast::WindowFrameBound::CurrentRow)
        } else if self.match_tok(|t| matches!(t, ExprToken::Unbounded)) {
            if self.match_tok(|t| matches!(t, ExprToken::Preceding)) {
                Ok(crate::ast::WindowFrameBound::UnboundedPreceding)
            } else if self.match_tok(|t| matches!(t, ExprToken::Following)) {
                Ok(crate::ast::WindowFrameBound::UnboundedFollowing)
            } else {
                Err(DbError::Parse("expected PRECEDING or FOLLOWING after UNBOUNDED".into()))
            }
        } else {
            let n = match self.next() {
                Some(ExprToken::Integer(v)) => Some(*v),
                _ => {
                    self.pos -= 1;
                    None
                }
            };
            if self.match_tok(|t| matches!(t, ExprToken::Preceding)) {
                Ok(crate::ast::WindowFrameBound::Preceding(n))
            } else if self.match_tok(|t| matches!(t, ExprToken::Following)) {
                Ok(crate::ast::WindowFrameBound::Following(n))
            } else {
                Err(DbError::Parse("expected PRECEDING or FOLLOWING".into()))
            }
        }
    }
}
