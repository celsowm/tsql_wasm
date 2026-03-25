use crate::ast::*;
use crate::error::DbError;

pub(crate) fn parse_print(sql: &str) -> Result<Statement, DbError> {
    let after = sql["PRINT".len()..].trim();
    let expr = crate::parser::expression::parse_expr(after)?;
    Ok(Statement::Print(expr))
}
