use crate::parser::ast::Token;
use crate::parser::error::{ParseError, ParseResult, Expected};
use crate::parser::token::Keyword;

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
    depth: usize,
}

const MAX_PARSER_DEPTH: usize = 8;

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
            depth: 0,
        }
    }

    pub fn enter_recursion(&mut self) -> ParseResult<()> {
        self.depth += 1;
        if self.depth > MAX_PARSER_DEPTH {
            return Err(ParseError::new(self.position)
                .expected_desc("recursion limit exceeded")
                .found(format!("depth {}", self.depth)));
        }
        Ok(())
    }

    pub fn leave_recursion(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    pub fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    pub fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.position + offset)
    }

    pub fn next(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.position)?;
        self.position += 1;
        Some(tok)
    }

    pub fn is_empty(&self) -> bool {
        self.position >= self.tokens.len()
    }

    pub fn at_keyword(&self, kw: Keyword) -> bool {
        matches!(self.peek(), Some(Token::Keyword(k)) if *k == kw)
    }

    pub fn expect_keyword(&mut self, kw: Keyword) -> ParseResult<()> {
        let pos = self.position;
        match self.next() {
            Some(Token::Keyword(k)) if *k == kw => Ok(()),
            Some(tok) => Err(ParseError::new(pos)
                .expected_keyword(kw)
                .found(token_display(tok))),
            None => Err(ParseError::new(pos)
                .expected_keyword(kw)
                .found("end of input".to_string())),
        }
    }

    pub fn expect_lparen(&mut self) -> ParseResult<()> {
        self.expect_token(&Token::LParen)
    }

    pub fn expect_rparen(&mut self) -> ParseResult<()> {
        self.expect_token(&Token::RParen)
    }

    pub fn expect_comma(&mut self) -> ParseResult<()> {
        self.expect_token(&Token::Comma)
    }

    pub fn expect_token(&mut self, expected: &Token) -> ParseResult<()> {
        let pos = self.position;
        match self.next() {
            Some(tok) if tokens_equal(tok, expected) => Ok(()),
            Some(tok) => Err(ParseError::new(pos)
                .expected_desc("token")
                .found(token_display(tok))),
            None => Err(ParseError::new(pos)
                .expected_desc("token")
                .found("end of input".to_string())),
        }
    }

    pub fn error(&self, expected: Expected) -> ParseError {
        let pos = self.position;
        let found = self.peek().map(token_display);
        let mut err = ParseError::new(pos).expected(expected);
        if let Some(f) = found {
            err = err.found(f);
        }
        err
    }

    pub fn backtrack<T>(&self, expected: Expected) -> ParseResult<T> {
        Err(self.error(expected))
    }

    pub fn save(&self) -> usize {
        self.position
    }

    pub fn restore(&mut self, pos: usize) {
        self.position = pos;
    }
}

fn tokens_equal(a: &Token, b: &Token) -> bool {
    a == b
}

fn token_display(tok: &Token) -> String {
    match tok {
        Token::Keyword(k) => format!("keyword {}", k),
        Token::Identifier(id) => format!("identifier '{}'", id),
        Token::Variable(v) => format!("variable '{}'", v),
        Token::Number { value: n, .. } => format!("number {}", n),
        Token::String(s) => format!("string '{}'", s),
        Token::Operator(op) => format!("operator '{}'", op),
        Token::LParen => "'('".to_string(),
        Token::RParen => "')'".to_string(),
        Token::Comma => "','".to_string(),
        Token::Semicolon => "';'".to_string(),
        Token::Dot => "'.'".to_string(),
        Token::Star => "'*'".to_string(),
        Token::Tilde => "'~'".to_string(),
        Token::BinaryLiteral(hex) => format!("binary literal '{}'", hex),
        Token::Go => "GO".to_string(),
    }
}
