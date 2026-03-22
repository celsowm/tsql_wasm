use std::collections::HashMap;

use crate::ast::{BinaryOp, DataTypeSpec, Expr, OrderByExpr, SelectStmt, UnaryOp, WhenClause, WindowFunc};
use crate::error::DbError;

use super::tokenizer::{tokenize_expr, ExprToken};

pub fn parse_expr(input: &str) -> Result<Expr, DbError> {
    let tokens = tokenize_expr(input)?;
    let mut parser = ExprParser {
        tokens,
        pos: 0,
        subquery_map: HashMap::new(),
    };
    let expr = parser.parse_or()?;
    if parser.pos != parser.tokens.len() {
        return Err(DbError::Parse(
            "unexpected trailing tokens in expression".into(),
        ));
    }
    Ok(expr)
}

pub fn parse_expr_with_subqueries(
    input: &str,
    subquery_map: &HashMap<String, SelectStmt>,
) -> Result<Expr, DbError> {
    let tokens = tokenize_expr(input)?;
    let mut parser = ExprParser {
        tokens,
        pos: 0,
        subquery_map: subquery_map.clone(),
    };
    let expr = parser.parse_or()?;
    if parser.pos != parser.tokens.len() {
        return Err(DbError::Parse(
            "unexpected trailing tokens in expression".into(),
        ));
    }
    Ok(expr)
}

struct ExprParser {
    tokens: Vec<ExprToken>,
    pos: usize,
    subquery_map: HashMap<String, SelectStmt>,
}

impl ExprParser {
    fn parse_or(&mut self) -> Result<Expr, DbError> {
        let mut expr = self.parse_and()?;
        while self.match_tok(|t| matches!(t, ExprToken::Or)) {
            let right = self.parse_and()?;
            expr = Expr::Binary {
                left: Box::new(expr),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<Expr, DbError> {
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

    fn parse_cmp(&mut self) -> Result<Expr, DbError> {
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

    fn parse_additive(&mut self) -> Result<Expr, DbError> {
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

    fn parse_multiplicative(&mut self) -> Result<Expr, DbError> {
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

    fn parse_unary(&mut self) -> Result<Expr, DbError> {
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

    fn parse_primary(&mut self) -> Result<Expr, DbError> {
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
                        return self.parse_cast_call();
                    }
                    if name.eq_ignore_ascii_case("CONVERT") {
                        return self.parse_convert_call();
                    }
                    if self.is_window_function(&name) {
                        return self.parse_window_function(&name);
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
                    Ok(Expr::FunctionCall { name, args })
                } else if name == "@@IDENTITY" {
                    Ok(Expr::FunctionCall {
                        name: "@@IDENTITY".to_string(),
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
                            .map(|s| s.trim_matches('[').trim_matches(']').to_string())
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

    fn parse_case(&mut self) -> Result<Expr, DbError> {
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
            when_clauses.push(WhenClause { condition, result });

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

    fn parse_cast_call(&mut self) -> Result<Expr, DbError> {
        let expr = self.parse_or()?;
        self.expect(|t| matches!(t, ExprToken::As), "AS")?;
        let target = self.parse_expr_data_type()?;
        self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            target,
        })
    }

    fn parse_convert_call(&mut self) -> Result<Expr, DbError> {
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
        Ok(Expr::Convert {
            target,
            expr: Box::new(expr),
            style,
        })
    }

    fn parse_expr_data_type(&mut self) -> Result<DataTypeSpec, DbError> {
        let name = match self.next().cloned() {
            Some(ExprToken::Identifier(name)) => name,
            _ => return Err(DbError::Parse("expected data type".into())),
        };

        if self.match_tok(|t| matches!(t, ExprToken::LParen)) {
            let first = match self.next().cloned() {
                Some(ExprToken::Integer(v)) => v as u16,
                _ => return Err(DbError::Parse("expected integer type size".into())),
            };

            if self.match_tok(|t| matches!(t, ExprToken::Comma)) {
                let second = match self.next().cloned() {
                    Some(ExprToken::Integer(v)) => v as u8,
                    _ => return Err(DbError::Parse("expected integer scale".into())),
                };
                self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
                if name.eq_ignore_ascii_case("DECIMAL") || name.eq_ignore_ascii_case("NUMERIC") {
                    return Ok(DataTypeSpec::Decimal(first as u8, second));
                }
                return Err(DbError::Parse(format!("unsupported data type '{}'", name)));
            }

            self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
            if name.eq_ignore_ascii_case("VARCHAR") {
                return Ok(DataTypeSpec::VarChar(first));
            }
            if name.eq_ignore_ascii_case("NVARCHAR") {
                return Ok(DataTypeSpec::NVarChar(first));
            }
            if name.eq_ignore_ascii_case("CHAR") {
                return Ok(DataTypeSpec::Char(first));
            }
            if name.eq_ignore_ascii_case("NCHAR") {
                return Ok(DataTypeSpec::NChar(first));
            }
            if name.eq_ignore_ascii_case("DECIMAL") || name.eq_ignore_ascii_case("NUMERIC") {
                return Ok(DataTypeSpec::Decimal(first as u8, 0));
            }
            if name.eq_ignore_ascii_case("FLOAT") || name.eq_ignore_ascii_case("REAL") {
                return Ok(DataTypeSpec::Float);
            }
            if name.eq_ignore_ascii_case("BINARY") {
                return Ok(DataTypeSpec::Binary(first));
            }
            if name.eq_ignore_ascii_case("VARBINARY") {
                return Ok(DataTypeSpec::VarBinary(first));
            }
            return Err(DbError::Parse(format!("unsupported data type '{}'", name)));
        }

        if name.eq_ignore_ascii_case("BIT") {
            Ok(DataTypeSpec::Bit)
        } else if name.eq_ignore_ascii_case("TINYINT") {
            Ok(DataTypeSpec::TinyInt)
        } else if name.eq_ignore_ascii_case("SMALLINT") {
            Ok(DataTypeSpec::SmallInt)
        } else if name.eq_ignore_ascii_case("INT") {
            Ok(DataTypeSpec::Int)
        } else if name.eq_ignore_ascii_case("BIGINT") {
            Ok(DataTypeSpec::BigInt)
        } else if name.eq_ignore_ascii_case("FLOAT") || name.eq_ignore_ascii_case("REAL") {
            Ok(DataTypeSpec::Float)
        } else if name.eq_ignore_ascii_case("MONEY") {
            Ok(DataTypeSpec::Money)
        } else if name.eq_ignore_ascii_case("SMALLMONEY") {
            Ok(DataTypeSpec::SmallMoney)
        } else if name.eq_ignore_ascii_case("VARCHAR") {
            Ok(DataTypeSpec::VarChar(8000))
        } else if name.eq_ignore_ascii_case("NVARCHAR") {
            Ok(DataTypeSpec::NVarChar(4000))
        } else if name.eq_ignore_ascii_case("CHAR") {
            Ok(DataTypeSpec::Char(1))
        } else if name.eq_ignore_ascii_case("NCHAR") {
            Ok(DataTypeSpec::NChar(1))
        } else if name.eq_ignore_ascii_case("BINARY") {
            Ok(DataTypeSpec::Binary(1))
        } else if name.eq_ignore_ascii_case("VARBINARY") {
            Ok(DataTypeSpec::VarBinary(8000))
        } else if name.eq_ignore_ascii_case("DATE") {
            Ok(DataTypeSpec::Date)
        } else if name.eq_ignore_ascii_case("TIME") {
            Ok(DataTypeSpec::Time)
        } else if name.eq_ignore_ascii_case("DATETIME") {
            Ok(DataTypeSpec::DateTime)
        } else if name.eq_ignore_ascii_case("DATETIME2") {
            Ok(DataTypeSpec::DateTime2)
        } else if name.eq_ignore_ascii_case("UNIQUEIDENTIFIER") {
            Ok(DataTypeSpec::UniqueIdentifier)
        } else if name.eq_ignore_ascii_case("SQL_VARIANT") {
            Ok(DataTypeSpec::SqlVariant)
        } else if name.eq_ignore_ascii_case("DECIMAL") || name.eq_ignore_ascii_case("NUMERIC") {
            Ok(DataTypeSpec::Decimal(18, 0))
        } else {
            Err(DbError::Parse(format!("unsupported data type '{}'", name)))
        }
    }

    fn match_cmp_op(&mut self) -> Option<BinaryOp> {
        let op = match self.peek()? {
            ExprToken::Eq => BinaryOp::Eq,
            ExprToken::NotEq => BinaryOp::NotEq,
            ExprToken::Gt => BinaryOp::Gt,
            ExprToken::Lt => BinaryOp::Lt,
            ExprToken::Gte => BinaryOp::Gte,
            ExprToken::Lte => BinaryOp::Lte,
            _ => return None,
        };
        self.pos += 1;
        Some(op)
    }

    fn expect<F>(&mut self, pred: F, label: &str) -> Result<(), DbError>
    where
        F: FnOnce(&ExprToken) -> bool,
    {
        if let Some(tok) = self.peek() {
            if pred(tok) {
                self.pos += 1;
                return Ok(());
            }
        }
        Err(DbError::Parse(format!("expected {}", label)))
    }

    fn match_tok<F>(&mut self, pred: F) -> bool
    where
        F: FnOnce(&ExprToken) -> bool,
    {
        if let Some(tok) = self.peek() {
            if pred(tok) {
                self.pos += 1;
                return true;
            }
        }
        false
    }

    fn peek(&self) -> Option<&ExprToken> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<&ExprToken> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn is_window_function(&self, name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" | "LAG" | "LEAD"
        )
    }

    fn parse_window_function(&mut self, name: &str) -> Result<Expr, DbError> {
        let func = match name.to_uppercase().as_str() {
            "ROW_NUMBER" => WindowFunc::RowNumber,
            "RANK" => WindowFunc::Rank,
            "DENSE_RANK" => WindowFunc::DenseRank,
            "NTILE" => WindowFunc::NTile,
            "LAG" => WindowFunc::Lag,
            "LEAD" => WindowFunc::Lead,
            _ => return Err(DbError::Parse(format!("unknown window function: {}", name))),
        };

        let mut args = Vec::new();
        if !self.match_tok(|t| matches!(t, ExprToken::RParen)) {
            loop {
                if matches!(self.peek(), Some(ExprToken::RParen)) {
                    break;
                }
                args.push(self.parse_or()?);
                if self.match_tok(|t| matches!(t, ExprToken::Comma)) {
                    continue;
                }
                self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
                break;
            }
        }

        self.expect(|t| matches!(t, ExprToken::Over), "OVER")?;
        self.expect(|t| matches!(t, ExprToken::LParen), "(")?;
        
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let frame = None;

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
                        let asc = !self.match_tok(|t| matches!(t, ExprToken::Desc));
                        order_by.push(OrderByExpr { expr, asc });
                        if self.match_tok(|t| matches!(t, ExprToken::Comma)) {
                            continue;
                        }
                        break;
                    }
                } else {
                    return Err(DbError::Parse("expected PARTITION BY, ORDER BY, or ROWS/RANGE in window specification".into()));
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
}
