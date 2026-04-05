use serde::{Deserialize, Serialize};
use super::token::Keyword;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Token {
    Keyword(Keyword),
    Identifier(String),
    Variable(String),
    Number { value: f64, is_float: bool },
    String(String),
    Operator(String),
    LParen,
    RParen,
    Comma,
    Semicolon,
    Dot,
    Star,
    Tilde,
    BinaryLiteral(String),
    Go,
}

pub fn is_keyword(id: &str) -> bool {
    Keyword::parse(id).is_some()
}

pub fn unescape_string(s: &str) -> String {
    let mut s_slice = s;
    if s_slice.starts_with('N') {
        s_slice = &s_slice[1..];
    }
    if s_slice.starts_with('\'') && s_slice.ends_with('\'') {
        s_slice = &s_slice[1..s_slice.len()-1];
    }
    s_slice.replace("''", "'")
}