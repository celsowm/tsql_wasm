use crate::error::DbError;

#[derive(Debug, Clone)]
pub enum ExprToken {
    Identifier(String),
    Integer(i64),
    FloatLiteral(String),
    BinaryLiteral(Vec<u8>),
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
    Exists,
    Over,
    Partition,
    By,
    Order,
    Desc,
    Rows,
    Range,
    Groups,
    Asc,
    Unbounded,
    Preceding,
    Following,
    Current,
    Row,
    Within,
    Group,
}

pub fn tokenize_expr(input: &str) -> Result<Vec<ExprToken>, DbError> {
    tokenize_expr_with_quoted_ident(input, true)
}

pub fn tokenize_expr_with_quoted_ident(input: &str, quoted_identifier: bool) -> Result<Vec<ExprToken>, DbError> {
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
            '"' => {
                if quoted_identifier {
                    let start = i + 1;
                    i += 1;
                    while i < chars.len() && chars[i] != '"' {
                        i += 1;
                    }
                    if i >= chars.len() {
                        return Err(DbError::Parse("unterminated quoted identifier".into()));
                    }
                    out.push(ExprToken::Identifier(chars[start..i].iter().collect()));
                    i += 1;
                } else {
                    let start = i + 1;
                    i += 1;
                    while i < chars.len() && chars[i] != '"' {
                        i += 1;
                    }
                    if i >= chars.len() {
                        return Err(DbError::Parse("unterminated string literal".into()));
                    }
                    out.push(ExprToken::String(chars[start..i].iter().collect()));
                    i += 1;
                }
            }
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
                // Check for hex literal 0x...
                if chars[start] == '0' && i < chars.len() && (chars[i] == 'x' || chars[i] == 'X') {
                    i += 1;
                    while i < chars.len() && chars[i].is_ascii_hexdigit() {
                        i += 1;
                    }
                    let hex_str: String = chars[start + 2..i].iter().collect();
                    let bytes = hex_to_bytes(&hex_str)?;
                    out.push(ExprToken::BinaryLiteral(bytes));
                } else {
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    if i < chars.len()
                        && chars[i] == '.'
                        && i + 1 < chars.len()
                        && chars[i + 1].is_ascii_digit()
                    {
                        i += 1;
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
        "EXISTS" => out.push(ExprToken::Exists),
        "OVER" => out.push(ExprToken::Over),
        "PARTITION" => out.push(ExprToken::Partition),
        "ORDER" => out.push(ExprToken::Order),
        "BY" => out.push(ExprToken::By),
        "DESC" => out.push(ExprToken::Desc),
        "ASC" => out.push(ExprToken::Asc),
        "ROWS" => out.push(ExprToken::Rows),
        "RANGE" => out.push(ExprToken::Range),
        "GROUPS" => out.push(ExprToken::Groups),
        "UNBOUNDED" => out.push(ExprToken::Unbounded),
        "PRECEDING" => out.push(ExprToken::Preceding),
        "FOLLOWING" => out.push(ExprToken::Following),
        "CURRENT" => out.push(ExprToken::Current),
        "ROW" => out.push(ExprToken::Row),
        "WITHIN" => out.push(ExprToken::Within),
        "GROUP" => out.push(ExprToken::Group),
        _ => out.push(ExprToken::Identifier(ident)),
    }
}

fn is_ident_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == '@'
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '@' || ch == '.'
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, DbError> {
    if hex.len() % 2 != 0 {
        return Err(DbError::Parse("hex literal must have even number of digits".into()));
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let chars: Vec<char> = hex.chars().collect();
    for i in (0..chars.len()).step_by(2) {
        let hi = hex_char_to_val(chars[i])
            .ok_or_else(|| DbError::Parse(format!("invalid hex digit '{}'", chars[i])))?;
        let lo = hex_char_to_val(chars[i + 1])
            .ok_or_else(|| DbError::Parse(format!("invalid hex digit '{}'", chars[i + 1])))?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

fn hex_char_to_val(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c as u8 - b'0'),
        'a'..='f' => Some(c as u8 - b'a' + 10),
        'A'..='F' => Some(c as u8 - b'A' + 10),
        _ => None,
    }
}
