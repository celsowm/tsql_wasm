pub(crate) mod v2;

use crate::ast::Statement;
use crate::error::DbError;

pub mod utils {
    use super::v2;

    pub fn split_csv_top_level(input: &str) -> Vec<String> {
        let mut sql_ref = input;
        let tokens = match v2::lexer::lex(&mut sql_ref, true) {
            Ok(t) => t,
            Err(_) => return vec![input.to_string()],
        };
        
        let mut result = Vec::new();
        let mut depth: i32 = 0;
        
        let mut current_part_tokens = Vec::new();
        for tok in tokens {
            match tok {
                v2::ast::Token::LParen => {
                    depth += 1;
                    current_part_tokens.push(tok);
                }
                v2::ast::Token::RParen => {
                    depth = depth.saturating_sub(1);
                    current_part_tokens.push(tok);
                }
                v2::ast::Token::Comma if depth == 0 => {
                    result.push(format_tokens(&current_part_tokens));
                    current_part_tokens.clear();
                }
                _ => {
                    current_part_tokens.push(tok);
                }
            }
        }
        if !current_part_tokens.is_empty() {
            result.push(format_tokens(&current_part_tokens));
        }
        result
    }

    fn format_tokens(tokens: &[v2::ast::Token]) -> String {
        tokens.iter().map(|t| match t {
            v2::ast::Token::String(s) => format!("'{}'", s.replace("'", "''")),
            v2::ast::Token::Identifier(id) => format!("[{}]", id),
            v2::ast::Token::Variable(v) => v.to_string(),
            v2::ast::Token::Keyword(k) => k.to_string(),
            v2::ast::Token::Operator(op) => op.to_string(),
            v2::ast::Token::Number(n) => n.to_string(),
            v2::ast::Token::LParen => "(".to_string(),
            v2::ast::Token::RParen => ")".to_string(),
            v2::ast::Token::Comma => ",".to_string(),
            v2::ast::Token::Dot => ".".to_string(),
            v2::ast::Token::Semicolon => ";".to_string(),
            v2::ast::Token::Star => "*".to_string(),
            _ => "".to_string(),
        }).collect::<Vec<_>>().join(" ")
    }
}

pub mod statements {
    pub mod procedural {
        use crate::parser::v2;
        use crate::error::DbError;

        pub fn parse_routine_params(input: &str) -> Result<Vec<crate::ast::RoutineParam>, DbError> {
            let mut sql_ref = input;
            let tokens = v2::lexer::lex(&mut sql_ref, true)
                .map_err(|e| DbError::Parse(format!("Lexer error: {:?}", e)))?;
            let mut tok_ref = tokens.as_slice();
            let v2_params = v2::parser::parse_comma_list(&mut tok_ref, v2::parser::parse_routine_param)
                .map_err(|e| DbError::Parse(format!("Parser error: {:?}", e)))?;
            v2_params.into_iter().map(v2::lower::lower_routine_param).collect()
        }
    }
}

pub fn parse_expr(input: &str) -> Result<crate::ast::Expr, DbError> {
    parse_expr_with_quoted_ident(input, true)
}

pub fn parse_expr_with_quoted_ident(
    input: &str,
    quoted_identifier: bool,
) -> Result<crate::ast::Expr, DbError> {
    let mut sql_ref = input;
    let tokens = v2::lexer::lex(&mut sql_ref, quoted_identifier)
        .map_err(|e| DbError::Parse(format!("Lexer error: {:?}", e)))?;
    let mut tok_ref = tokens.as_slice();
    let v2_expr = v2::parser::expressions::parse_expr(&mut tok_ref)
        .map_err(|e| DbError::Parse(format!("Parser error: {:?}", e)))?;
    v2::lower::lower_expr(v2_expr)
}

pub fn parse_expr_subquery_aware(input: &str) -> Result<crate::ast::Expr, DbError> {
    parse_expr_subquery_aware_with_quoted_ident(input, true)
}

pub fn parse_expr_subquery_aware_with_quoted_ident(
    input: &str,
    quoted_identifier: bool,
) -> Result<crate::ast::Expr, DbError> {
    parse_expr_with_quoted_ident(input, quoted_identifier)
}

pub fn parse_batch(sql: &str) -> Result<Vec<Statement>, DbError> {
    parse_batch_with_quoted_ident(sql, true)
}

pub fn parse_batch_with_quoted_ident(
    sql: &str,
    quoted_identifier: bool,
) -> Result<Vec<Statement>, DbError> {
    let mut sql_ref = sql;
    let tokens = v2::lexer::lex(&mut sql_ref, quoted_identifier)
        .map_err(|e| DbError::Parse(format!("Lexer error: {:?}", e)))?;
    println!("SQL: {} -> Tokens: {:?}", sql, tokens);
    let mut tok_ref = tokens.as_slice();
    let v2_stmts = v2::parser::parse_batch(&mut tok_ref)
        .map_err(|e| DbError::Parse(format!("Parser error: {:?}", e)))?;
    v2::lower::lower_batch(v2_stmts)
}

pub fn parse_sql(sql: &str) -> Result<Statement, DbError> {
    parse_sql_with_quoted_ident(sql, true)
}

pub fn parse_sql_with_quoted_ident(
    sql: &str,
    quoted_identifier: bool,
) -> Result<Statement, DbError> {
    let stmts = parse_batch_with_quoted_ident(sql, quoted_identifier)?;
    if stmts.is_empty() {
        return Err(DbError::Parse("Expected at least one statement".into()));
    }
    // We take the first one, or should we error if more than one?
    // Old parser seems to have error if stmts.len() != 1 in some places.
    if stmts.len() != 1 {
        return Err(DbError::Parse("Expected exactly one statement".into()));
    }
    Ok(stmts.into_iter().next().unwrap())
}
