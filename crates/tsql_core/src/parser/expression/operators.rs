use crate::ast::{Expr, BinaryOp, UnaryOp};
use crate::error::DbError;
use super::ExprToken;

impl super::ExprParser {
    pub(crate) fn parse_and(&mut self) -> Result<Expr, DbError> {
        self.depth += 1;
        if self.depth > 100 {
            return Err(DbError::Parse("expression nested too deeply".into()));
        }
        let mut expr = self.parse_cmp()?;
        while self.match_tok(|t| matches!(t, ExprToken::And)) {
            let right = self.parse_cmp()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    pub(crate) fn parse_cmp(&mut self) -> Result<Expr, DbError> {
        self.depth += 1;
        if self.depth > 100 {
            return Err(DbError::Parse("expression nested too deeply".into()));
        }
        let expr = self.parse_additive()?;

        if self.match_tok(|t| matches!(t, ExprToken::Is)) {
            let not = self.match_tok(|t| matches!(t, ExprToken::Not));
            self.expect(|t| matches!(t, ExprToken::Null), "NULL")?;
            return Ok(if not {
                Expr::IsNotNull(Box::new(expr))
            } else {
                Expr::IsNull(Box::new(expr))
            });
        }

        if let Some(op) = self.match_cmp_op() {
            let right = self.parse_additive()?;
            return Ok(Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            });
        }

        let negated = self.match_tok(|t| matches!(t, ExprToken::Not));

        if self.match_tok(|t| matches!(t, ExprToken::In)) {
            self.expect(|t| matches!(t, ExprToken::LParen), "(")?;
            let mut list = Vec::new();
            if !self.match_tok(|t| matches!(t, ExprToken::RParen)) {
                loop {
                    list.push(self.parse_or()?);
                    if self.match_tok(|t| matches!(t, ExprToken::Comma)) {
                        continue;
                    }
                    self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
                    break;
                }
            }
            if list.len() == 1 {
                match &list[0] {
                    Expr::Identifier(name) if name.starts_with("__SUBQ_") => {
                        if let Some(stmt) = self.subquery_map.get(name).cloned() {
                            return Ok(Expr::InSubquery {
                                expr: Box::new(expr),
                                subquery: Box::new(stmt),
                                negated,
                            });
                        }
                    }
                    Expr::Subquery(stmt) => {
                        return Ok(Expr::InSubquery {
                            expr: Box::new(expr),
                            subquery: stmt.clone(),
                            negated,
                        });
                    }
                    _ => {}
                }
            }
            return Ok(Expr::InList {
                expr: Box::new(expr),
                list,
                negated,
            });
        }

        if self.match_tok(|t| matches!(t, ExprToken::Between)) {
            let low = self.parse_additive()?;
            self.expect(|t| matches!(t, ExprToken::And), "AND")?;
            let high = self.parse_additive()?;
            return Ok(Expr::Between {
                expr: Box::new(expr),
                low: Box::new(low),
                high: Box::new(high),
                negated,
            });
        }

        if self.match_tok(|t| matches!(t, ExprToken::Like)) {
            let pattern = self.parse_additive()?;
            return Ok(Expr::Like {
                expr: Box::new(expr),
                pattern: Box::new(pattern),
                negated,
            });
        }

        Ok(expr)
    }

    pub(crate) fn parse_additive(&mut self) -> Result<Expr, DbError> {
        self.depth += 1;
        if self.depth > 100 {
            return Err(DbError::Parse("expression nested too deeply".into()));
        }
        let mut expr = self.parse_multiplicative()?;
        loop {
            if self.match_tok(|t| matches!(t, ExprToken::Plus)) {
                let right = self.parse_multiplicative()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op: BinaryOp::Add,
                    right: Box::new(right),
                };
            } else if self.match_tok(|t| matches!(t, ExprToken::Minus)) {
                let right = self.parse_multiplicative()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op: BinaryOp::Subtract,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    pub(crate) fn parse_multiplicative(&mut self) -> Result<Expr, DbError> {
        self.depth += 1;
        if self.depth > 100 {
            return Err(DbError::Parse("expression nested too deeply".into()));
        }
        let mut expr = self.parse_unary()?;
        loop {
            if self.match_tok(|t| matches!(t, ExprToken::Star)) {
                let right = self.parse_unary()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op: BinaryOp::Multiply,
                    right: Box::new(right),
                };
            } else if self.match_tok(|t| matches!(t, ExprToken::Slash)) {
                let right = self.parse_unary()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op: BinaryOp::Divide,
                    right: Box::new(right),
                };
            } else if self.match_tok(|t| matches!(t, ExprToken::Percent)) {
                let right = self.parse_unary()?;
                expr = Expr::Binary {
                    left: Box::new(expr),
                    op: BinaryOp::Modulo,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    pub(crate) fn parse_unary(&mut self) -> Result<Expr, DbError> {
        if self.match_tok(|t| matches!(t, ExprToken::Minus)) {
            let operand = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Negate,
                expr: Box::new(operand),
            });
        }

        if self.match_tok(|t| matches!(t, ExprToken::Plus)) {
            return self.parse_unary();
        }

        if self.match_tok(|t| matches!(t, ExprToken::Not)) {
            if matches!(
                self.peek(),
                Some(ExprToken::In | ExprToken::Between | ExprToken::Like)
            ) {
                self.pos -= 1;
                return self.parse_primary();
            }
            let operand = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(operand),
            });
        }

        self.parse_primary()
    }
}
