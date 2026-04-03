use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use super::token::Keyword;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Token<'a> {
    Keyword(Keyword),
    Identifier(Cow<'a, str>),
    Variable(Cow<'a, str>),
    Number(f64),
    String(Cow<'a, str>), // Unescaped string
    Operator(Cow<'a, str>),
    LParen,
    RParen,
    Comma,
    Semicolon,
    Dot,
    Star,
    Tilde,
    BinaryLiteral(Cow<'a, str>),
    Go,
}

pub fn is_keyword(id: &str) -> bool {
    Keyword::from_str(id).is_some()
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
