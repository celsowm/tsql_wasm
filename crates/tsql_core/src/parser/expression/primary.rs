use crate::ast::Expr;
use crate::error::DbError;
use super::ExprToken;

impl super::ExprParser {
    pub(crate) fn parse_primary(&mut self) -> Result<Expr, DbError> {
        self.depth += 1;
        if self.depth > 100 {
            return Err(DbError::Parse("expression nested too deeply".into()));
        }
        if self.match_tok(|t| matches!(t, ExprToken::Exists)) {
            let negated = self.match_tok(|t| matches!(t, ExprToken::Not));
            match self.next().cloned() {
                Some(ExprToken::Identifier(name)) if name.starts_with("__SUBQ_") => {
                    let stmt = self.subquery_map.get(&name).cloned().ok_or_else(|| {
                        DbError::Parse(format!("unknown subquery placeholder '{}'", name))
                    })?;
                    return Ok(Expr::Exists {
                        subquery: Box::new(stmt),
                        negated,
                    });
                }
                _ => return Err(DbError::Parse("expected subquery after EXISTS".into())),
            }
        }

        if self.match_tok(|t| matches!(t, ExprToken::LParen)) {
            let expr = self.parse_or()?;
            self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
            return Ok(expr);
        }

        if self.match_tok(|t| matches!(t, ExprToken::Case)) {
            return self.parse_case();
        }

        match self.next().cloned() {
            Some(ExprToken::Identifier(name)) => {
                if name.starts_with("__SUBQ_") {
                    if let Some(stmt) = self.subquery_map.get(&name).cloned() {
                        return Ok(Expr::Subquery(Box::new(stmt)));
                    }
                }
                if self.match_tok(|t| matches!(t, ExprToken::LParen)) {
                    if name.eq_ignore_ascii_case("CAST") {
                        return self.parse_cast_call(false);
                    }
                    if name.eq_ignore_ascii_case("TRY_CAST") {
                        return self.parse_cast_call(true);
                    }
                    if name.eq_ignore_ascii_case("CONVERT") {
                        return self.parse_convert_call(false);
                    }
                    if name.eq_ignore_ascii_case("TRY_CONVERT") {
                        return self.parse_convert_call(true);
                    }

                    let mut args = Vec::new();
                    if !self.match_tok(|t| matches!(t, ExprToken::RParen)) {
                        loop {
                            if self.match_tok(|t| matches!(t, ExprToken::Star)) {
                                args.push(Expr::Wildcard);
                            } else {
                                args.push(self.parse_or()?);
                            }

                            if self.match_tok(|t| matches!(t, ExprToken::Comma)) {
                                continue;
                            }
                            self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
                            break;
                        }
                    }
                    if self.is_window_function(&name) && self.match_tok(|t| matches!(t, ExprToken::Over)) {
                        return self.parse_window_function_rest(name, args);
                    }
                    Ok(Expr::FunctionCall { name, args })
                } else if name == "@@IDENTITY" || name.starts_with("@@") {
                    Ok(Expr::FunctionCall {
                        name,
                        args: vec![],
                    })
                } else if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
                    Ok(Expr::FunctionCall {
                        name: "CURRENT_TIMESTAMP".to_string(),
                        args: vec![],
                    })
                } else if name.eq_ignore_ascii_case("CURRENT_DATE") {
                    Ok(Expr::FunctionCall {
                        name: "CURRENT_DATE".to_string(),
                        args: vec![],
                    })
                } else if name.contains('.') {
                    Ok(Expr::QualifiedIdentifier(
                        name.split('.')
                            .map(|s: &str| s.trim_matches('[').trim_matches(']').to_string())
                            .collect(),
                    ))
                } else {
                    Ok(Expr::Identifier(name))
                }
            }
            Some(ExprToken::Integer(v)) => Ok(Expr::Integer(v)),
            Some(ExprToken::FloatLiteral(s)) => Ok(Expr::FloatLiteral(s)),
            Some(ExprToken::BinaryLiteral(bytes)) => Ok(Expr::BinaryLiteral(bytes)),
            Some(ExprToken::String(v)) => Ok(Expr::String(v)),
            Some(ExprToken::UnicodeString(v)) => Ok(Expr::UnicodeString(v)),
            Some(ExprToken::Null) => Ok(Expr::Null),
            Some(ExprToken::Star) => Ok(Expr::Wildcard),
            _ => Err(DbError::Parse("invalid expression".into())),
        }
    }
}
