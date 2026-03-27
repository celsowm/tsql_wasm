use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::{find_keyword_top_level, parse_object_name};
use super::output::parse_output_clause;
use super::update::parse_update_from_clause;

pub(crate) fn parse_delete(sql: &str) -> Result<Statement, DbError> {
    let after_delete = if sql.to_uppercase().starts_with("DELETE FROM") {
        &sql["DELETE FROM".len()..]
    } else {
        &sql["DELETE".len()..]
    }
    .trim();

    // Check if there's OUTPUT clause
    let output_idx = find_keyword_top_level(after_delete, "OUTPUT");
    let (output, output_into, after_delete_stripped) = if let Some(oi) = output_idx {
        let after_output = &after_delete[oi + "OUTPUT".len()..];
        let end_idx = find_keyword_top_level(after_output, "FROM")
            .or_else(|| find_keyword_top_level(after_output, "WHERE"))
            .unwrap_or(after_output.len());
        let output_raw = &after_output[..end_idx];
        let (parsed_output, into_target) = parse_output_clause(output_raw.trim())?;
        // Reconstruct: table part + FROM/WHERE part (skipping OUTPUT columns)
        let before_output = &after_delete[..oi];
        let after_output_cols = &after_output[end_idx..];
        let reconstructed = format!("{}{}", before_output, after_output_cols);
        (Some(parsed_output), into_target, reconstructed)
    } else {
        (None, None, after_delete.to_string())
    };

    let after_delete_stripped = after_delete_stripped.as_str();

    // Check if there's a FROM clause after table name (DELETE FROM t OUTPUT ... FROM t INNER JOIN ...)
    let first_from_end = after_delete_stripped.find(|c: char| c.is_whitespace()).unwrap_or(after_delete_stripped.len());
    let after_first_table = after_delete_stripped[first_from_end..].trim();

    if after_first_table.to_uppercase().starts_with("FROM ") {
        // DELETE FROM <table> OUTPUT ... FROM <source> [WHERE ...]
        let table = parse_object_name(after_delete_stripped[..first_from_end].trim());
        let after_second_from = after_first_table["FROM".len()..].trim();
        let where_idx = find_keyword_top_level(after_second_from, "WHERE");

        let (from_source, where_part) = if let Some(idx) = where_idx {
            (&after_second_from[..idx], &after_second_from[idx + "WHERE".len()..])
        } else {
            (after_second_from, "")
        };

        let from_clause = parse_update_from_clause(from_source.trim())?;
        let selection = if !where_part.trim().is_empty() {
            Some(crate::parser::expression::parse_expr(where_part.trim())?)
        } else {
            None
        };

        return Ok(Statement::Delete(DeleteStmt {
            table,
            selection,
            from: Some(from_clause),
            output,
            output_into,
        }));
    }

    // Standard DELETE FROM <table> [WHERE ...]
    let where_idx = find_keyword_top_level(after_delete_stripped, "WHERE");

    let table = if let Some(idx) = where_idx {
        parse_object_name(after_delete_stripped[..idx].trim())
    } else {
        parse_object_name(after_delete_stripped)
    };

    let selection = where_idx
        .map(|idx| crate::parser::expression::parse_expr(after_delete_stripped[idx + "WHERE".len()..].trim()))
        .transpose()?;

    Ok(Statement::Delete(DeleteStmt {
        table,
        selection,
        from: None,
        output,
        output_into,
    }))
}
