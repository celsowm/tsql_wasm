use crate::ast::{BinaryOp, DataTypeSpec, Expr, UnaryOp, WhenClause};
use crate::error::DbError;

pub fn parse_expr(input: &str) -> Result<Expr, DbError> {
    let tokens = tokenize_expr(input)?;
    let mut parser = ExprParser { tokens, pos: 0 };
    let expr = parser.parse_or()?;
    if parser.pos != parser.tokens.len() {
        return Err(DbError::Parse(
            "unexpected trailing tokens in expression".into(),
        ));
    }
    Ok(expr)
}

#[derive(Debug, Clone)]
enum ExprToken {
    Identifier(String),
    Integer(i64),
    FloatLiteral(String),
    String(String),
    UnicodeString(String),
    Null,
    Star,
    LParen,
    RParen,
    Comma,
    Eq,
    NotEq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    Is,
    Not,
    As,
    Plus,
    Minus,
    Slash,
    Percent,
    Case,
    When,
    Then,
    Else,
    End,
    In,
    Like,
    Between,
}

struct ExprParser {
    tokens: Vec<ExprToken>,
    pos: usize,
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

        // IS [NOT] NULL
        if self.match_tok(|t| matches!(t, ExprToken::Is)) {
            let not = self.match_tok(|t| matches!(t, ExprToken::Not));
            self.expect(|t| matches!(t, ExprToken::Null), "NULL")?;
            return Ok(if not {
                Expr::IsNotNull(Box::new(expr))
            } else {
                Expr::IsNull(Box::new(expr))
            });
        }

        // Comparison operators
        if let Some(op) = self.match_cmp_op() {
            let right = self.parse_additive()?;
            return Ok(Expr::Binary {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            });
        }

        // NOT prefix for compound operators
        let negated = self.match_tok(|t| matches!(t, ExprToken::Not));

        // IN (...)
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
            return Ok(Expr::InList {
                expr: Box::new(expr),
                list,
                negated,
            });
        }

        // BETWEEN ... AND ...
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

        // LIKE
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
            // Check if this NOT is followed by IN/BETWEEN/LIKE — if so, put it back
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
        // Parenthesized expression or subquery
        if self.match_tok(|t| matches!(t, ExprToken::LParen)) {
            let expr = self.parse_or()?;
            self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
            return Ok(expr);
        }

        // CASE expression
        if self.match_tok(|t| matches!(t, ExprToken::Case)) {
            return self.parse_case();
        }

        match self.next().cloned() {
            Some(ExprToken::Identifier(name)) => {
                if self.match_tok(|t| matches!(t, ExprToken::LParen)) {
                    if name.eq_ignore_ascii_case("CAST") {
                        return self.parse_cast_call();
                    }
                    if name.eq_ignore_ascii_case("CONVERT") {
                        return self.parse_convert_call();
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
                } else if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
                    Ok(Expr::FunctionCall {
                        name: "CURRENT_TIMESTAMP".to_string(),
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
            Some(ExprToken::String(v)) => Ok(Expr::String(v)),
            Some(ExprToken::UnicodeString(v)) => Ok(Expr::UnicodeString(v)),
            Some(ExprToken::Null) => Ok(Expr::Null),
            Some(ExprToken::Star) => Ok(Expr::Wildcard),
            _ => Err(DbError::Parse("invalid expression".into())),
        }
    }

    fn parse_case(&mut self) -> Result<Expr, DbError> {
        // Check for simple CASE: CASE expr WHEN val THEN result ... END
        // vs searched CASE: CASE WHEN cond THEN result ... END
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
        self.expect(|t| matches!(t, ExprToken::RParen), ")")?;
        Ok(Expr::Convert {
            target,
            expr: Box::new(expr),
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
        } else if name.eq_ignore_ascii_case("VARCHAR") {
            Ok(DataTypeSpec::VarChar(8000))
        } else if name.eq_ignore_ascii_case("NVARCHAR") {
            Ok(DataTypeSpec::NVarChar(4000))
        } else if name.eq_ignore_ascii_case("CHAR") {
            Ok(DataTypeSpec::Char(1))
        } else if name.eq_ignore_ascii_case("NCHAR") {
            Ok(DataTypeSpec::NChar(1))
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
}

fn tokenize_expr(input: &str) -> Result<Vec<ExprToken>, DbError> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut i = 0usize;
    let mut out = Vec::new();

    while i < chars.len() {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }

        match ch {
            '(' => {
                out.push(ExprToken::LParen);
                i += 1;
            }
            ')' => {
                out.push(ExprToken::RParen);
                i += 1;
            }
            ',' => {
                out.push(ExprToken::Comma);
                i += 1;
            }
            '*' => {
                out.push(ExprToken::Star);
                i += 1;
            }
            '+' => {
                out.push(ExprToken::Plus);
                i += 1;
            }
            '-' => {
                out.push(ExprToken::Minus);
                i += 1;
            }
            '/' => {
                out.push(ExprToken::Slash);
                i += 1;
            }
            '%' => {
                out.push(ExprToken::Percent);
                i += 1;
            }
            '=' => {
                out.push(ExprToken::Eq);
                i += 1;
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(ExprToken::Gte);
                    i += 2;
                } else {
                    out.push(ExprToken::Gt);
                    i += 1;
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    out.push(ExprToken::Lte);
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    out.push(ExprToken::NotEq);
                    i += 2;
                } else {
                    out.push(ExprToken::Lt);
                    i += 1;
                }
            }
            '\'' => {
                let start = i + 1;
                i += 1;
                while i < chars.len() && chars[i] != '\'' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err(DbError::Parse("unterminated string literal".into()));
                }
                out.push(ExprToken::String(chars[start..i].iter().collect()));
                i += 1;
            }
            'N' | 'n' => {
                if i + 1 < chars.len() && chars[i + 1] == '\'' {
                    let start = i + 2;
                    i += 2;
                    while i < chars.len() && chars[i] != '\'' {
                        i += 1;
                    }
                    if i >= chars.len() {
                        return Err(DbError::Parse("unterminated unicode string literal".into()));
                    }
                    out.push(ExprToken::UnicodeString(chars[start..i].iter().collect()));
                    i += 1;
                } else {
                    let ident = read_identifier(&chars, &mut i);
                    push_ident_token(&mut out, ident);
                }
            }
            c if c.is_ascii_digit() => {
                let start = i;
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                // Check for decimal part
                if i < chars.len()
                    && chars[i] == '.'
                    && i + 1 < chars.len()
                    && chars[i + 1].is_ascii_digit()
                {
                    i += 1; // skip '.'
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    let num: String = chars[start..i].iter().collect();
                    out.push(ExprToken::FloatLiteral(num));
                } else {
                    let num: String = chars[start..i].iter().collect();
                    out.push(ExprToken::Integer(
                        num.parse::<i64>()
                            .map_err(|_| DbError::Parse("invalid integer literal".into()))?,
                    ));
                }
            }
            c if is_ident_start(c) => {
                let ident = read_identifier(&chars, &mut i);
                push_ident_token(&mut out, ident);
            }
            _ => return Err(DbError::Parse(format!("unexpected character '{}'", ch))),
        }
    }

    Ok(out)
}

fn read_identifier(chars: &[char], i: &mut usize) -> String {
    let start = *i;
    *i += 1;
    while *i < chars.len() && is_ident_char(chars[*i]) {
        *i += 1;
    }
    chars[start..*i].iter().collect()
}

fn push_ident_token(out: &mut Vec<ExprToken>, ident: String) {
    match ident.to_uppercase().as_str() {
        "NULL" => out.push(ExprToken::Null),
        "AND" => out.push(ExprToken::And),
        "OR" => out.push(ExprToken::Or),
        "IS" => out.push(ExprToken::Is),
        "NOT" => out.push(ExprToken::Not),
        "AS" => out.push(ExprToken::As),
        "CASE" => out.push(ExprToken::Case),
        "WHEN" => out.push(ExprToken::When),
        "THEN" => out.push(ExprToken::Then),
        "ELSE" => out.push(ExprToken::Else),
        "END" => out.push(ExprToken::End),
        "IN" => out.push(ExprToken::In),
        "LIKE" => out.push(ExprToken::Like),
        "BETWEEN" => out.push(ExprToken::Between),
        _ => out.push(ExprToken::Identifier(ident)),
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == '@'
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '@' || ch == '.'
}
