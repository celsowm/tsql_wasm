use crate::parser::token::Keyword;
use std::fmt;

#[derive(Debug, Clone)]
pub enum Expected {
    Keyword(Keyword),
    Description(&'static str),
}

#[derive(Debug, Clone)]
pub struct ParseError {
    pub position: usize,
    pub expected: Vec<Expected>,
    pub found: Option<String>,
}

impl ParseError {
    pub fn new(position: usize) -> Self {
        Self {
            position,
            expected: Vec::new(),
            found: None,
        }
    }

    pub fn expected(mut self, exp: Expected) -> Self {
        self.expected.push(exp);
        self
    }

    pub fn expected_keyword(mut self, kw: Keyword) -> Self {
        self.expected.push(Expected::Keyword(kw));
        self
    }

    pub fn expected_desc(mut self, desc: &'static str) -> Self {
        self.expected.push(Expected::Description(desc));
        self
    }

    pub fn found(mut self, found: String) -> Self {
        self.found = Some(found);
        self
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "parse error at token {}", self.position)?;
        if !self.expected.is_empty() {
            let expected_strs: Vec<String> = self.expected.iter().map(|e| match e {
                Expected::Keyword(kw) => format!("keyword {}", kw),
                Expected::Description(d) => d.to_string(),
            }).collect();
            write!(f, ", expected: {}", expected_strs.join(", "))?;
        }
        if let Some(ref found) = self.found {
            write!(f, ", found: '{}'", found)?;
        }
        Ok(())
    }
}

impl std::error::Error for ParseError {}

pub type ParseResult<T> = Result<T, ParseError>;
