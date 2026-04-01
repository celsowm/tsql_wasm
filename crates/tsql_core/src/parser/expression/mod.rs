use std::collections::HashMap;
use crate::ast::{Expr, SelectStmt, DataTypeSpec, BinaryOp};
use crate::error::DbError;

mod operators;
mod primary;
mod special;
mod window;

use super::tokenizer::tokenize_expr_with_quoted_ident;
pub(crate) use super::tokenizer::ExprToken;

pub fn parse_expr(input: &str) -> Result<Expr, DbError> {
    parse_expr_with_quoted_ident(input, true)
}

pub fn parse_expr_with_quoted_ident(input: &str, quoted_identifier: bool) -> Result<Expr, DbError> {
    let tokens = tokenize_expr_with_quoted_ident(input, quoted_identifier)?;
    let mut parser = ExprParser {
        tokens,
        pos: 0,
        subquery_map: HashMap::new(),
        depth: 0,
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
    parse_expr_with_subqueries_and_quoted_ident(input, subquery_map, true)
}

pub fn parse_expr_with_subqueries_and_quoted_ident(
    input: &str,
    subquery_map: &HashMap<String, SelectStmt>,
    quoted_identifier: bool,
) -> Result<Expr, DbError> {
    let tokens = tokenize_expr_with_quoted_ident(input, quoted_identifier)?;
    let mut parser = ExprParser {
        tokens,
        pos: 0,
        subquery_map: subquery_map.clone(),
        depth: 0,
    };
    let expr = parser.parse_or()?;
    if parser.pos != parser.tokens.len() {
        return Err(DbError::Parse(
            "unexpected trailing tokens in expression".into(),
        ));
    }
    Ok(expr)
}

pub(crate) struct ExprParser {
    pub(crate) tokens: Vec<ExprToken>,
    pub(crate) pos: usize,
    pub(crate) subquery_map: HashMap<String, SelectStmt>,
    pub(crate) depth: usize,
}

impl ExprParser {
    pub(crate) fn parse_or(&mut self) -> Result<Expr, DbError> {
        self.depth += 1;
        if self.depth > 100 {
            return Err(DbError::Parse("expression nested too deeply".into()));
        }
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

    pub(crate) fn parse_expr_data_type(&mut self) -> Result<DataTypeSpec, DbError> {
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
            if name.eq_ignore_ascii_case("SYSNAME") {
                return Ok(DataTypeSpec::NVarChar(128));
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
        } else if name.eq_ignore_ascii_case("SYSNAME") {
            Ok(DataTypeSpec::NVarChar(128))
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

    pub(crate) fn match_cmp_op(&mut self) -> Option<BinaryOp> {
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

    pub(crate) fn expect<F>(&mut self, pred: F, label: &str) -> Result<(), DbError>
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

    pub(crate) fn match_tok<F>(&mut self, pred: F) -> bool
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

    pub(crate) fn peek(&self) -> Option<&ExprToken> {
        self.tokens.get(self.pos)
    }

    pub(crate) fn next(&mut self) -> Option<&ExprToken> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }
}
