mod expression;
mod statements;
mod utils;

use crate::ast::Statement;
use crate::error::DbError;

pub use expression::parse_expr;

pub fn parse_sql(sql: &str) -> Result<Statement, DbError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("CREATE TABLE ") {
        statements::parse_create_table(trimmed)
    } else if upper.starts_with("CREATE SCHEMA ") {
        statements::parse_create_schema(trimmed)
    } else if upper.starts_with("DROP TABLE ") {
        statements::parse_drop_table(trimmed)
    } else if upper.starts_with("DROP SCHEMA ") {
        statements::parse_drop_schema(trimmed)
    } else if upper.starts_with("INSERT INTO ") {
        statements::parse_insert(trimmed)
    } else if upper.starts_with("SELECT ") {
        statements::parse_select(trimmed)
    } else if upper.starts_with("UPDATE ") {
        statements::parse_update(trimmed)
    } else if upper.starts_with("DELETE FROM ") {
        statements::parse_delete(trimmed)
    } else {
        Err(DbError::Parse("unsupported statement".into()))
    }
}
