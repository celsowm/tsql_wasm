use crate::ast::Expr;
use crate::error::DbError;
use super::ExprToken;

impl super::ExprParser {
    pub(crate) fn parse_cast_call(&mut self, try_cast: bool) -> Result<Expr, DbError> {
        self.depth += 1;
        if self.depth > 100 {
            return Err(DbError::Parse("expression nested too deeply".into()));
        }
        let expr = self.parse_or()?;
        self.expect(|t| matches!(t, ExprToken::As), "AS")?;
        let target = self.parse_expr_data_type()?;
        self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
        if try_cast {
            Ok(Expr::TryCast {
                expr: Box::new(expr),
                target,
            })
        } else {
            Ok(Expr::Cast {
                expr: Box::new(expr),
                target,
            })
        }
    }

    pub(crate) fn parse_convert_call(&mut self, try_convert: bool) -> Result<Expr, DbError> {
        let target = self.parse_expr_data_type()?;
        self.expect(|t| matches!(t, ExprToken::Comma), ",")?;
        let expr = self.parse_or()?;
        let style = if self.match_tok(|t| matches!(t, ExprToken::Comma)) {
            match self.next().cloned() {
                Some(ExprToken::Integer(v)) => Some(v as i32),
                _ => return Err(DbError::Parse("expected integer style code".into())),
            }
        } else {
            None
        };
        self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
        if try_convert {
            Ok(Expr::TryConvert {
                target,
                expr: Box::new(expr),
                style,
            })
        } else {
            Ok(Expr::Convert {
                target,
                expr: Box::new(expr),
                style,
            })
        }
    }

    pub(crate) fn parse_case(&mut self) -> Result<Expr, DbError> {
        let operand = if self.match_tok(|t| matches!(t, ExprToken::When)) {
            None
        } else {
            let expr = self.parse_or()?;
            self.expect(|t| matches!(t, ExprToken::When), "WHEN")?;
            Some(Box::new(expr))
        };

        let mut when_clauses = Vec::new();
        loop {
            let condition = self.parse_or()?;
            self.expect(|t| matches!(t, ExprToken::Then), "THEN")?;
            let result = self.parse_or()?;
            when_clauses.push(crate::ast::WhenClause { condition, result });

            if self.match_tok(|t| matches!(t, ExprToken::Else)) {
                break;
            }
            if self.match_tok(|t| matches!(t, ExprToken::End)) {
                return Ok(Expr::Case {
                    operand,
                    when_clauses,
                    else_result: None,
                });
            }
            self.expect(|t| matches!(t, ExprToken::When), "WHEN or END")?;
        }

        let else_result = Some(Box::new(self.parse_or()?));
        self.expect(|t| matches!(t, ExprToken::End), "END")?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_result,
        })
    }
}
