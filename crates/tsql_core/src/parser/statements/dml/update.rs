use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::{find_keyword_top_level, parse_object_name, split_csv_top_level};
use super::output::parse_output_clause;

pub(crate) fn parse_update(sql: &str) -> Result<Statement, DbError> {
    let after_update = sql["UPDATE".len()..].trim();
    let set_idx = find_keyword_top_level(after_update, "SET")
        .ok_or_else(|| DbError::Parse("UPDATE missing SET".into()))?;

    let table = parse_object_name(after_update[..set_idx].trim());
    let tail = after_update[set_idx + "SET".len()..].trim();

    // Check for OUTPUT clause
    let output_idx = find_keyword_top_level(tail, "OUTPUT");

    let (output, output_into, tail_stripped) = if let Some(oi) = output_idx {
        let after_output = &tail[oi + "OUTPUT".len()..];
        let end_idx = find_keyword_top_level(after_output, "FROM")
            .or_else(|| find_keyword_top_level(after_output, "WHERE"))
            .unwrap_or(after_output.len());
        let output_raw = &after_output[..end_idx];
        let (parsed_output, into_target) = parse_output_clause(output_raw.trim())?;
        // Reconstruct: assignments part + FROM/WHERE part (skipping OUTPUT columns)
        let before_output = &tail[..oi];
        let after_output_cols = &after_output[end_idx..];
        let reconstructed = format!("{}{}", before_output, after_output_cols);
        (Some(parsed_output), into_target, reconstructed)
    } else {
        (None, None, tail.to_string())
    };

    let tail_stripped = tail_stripped.as_str();

    // Check for FROM clause (UPDATE ... SET ... OUTPUT ... FROM ... WHERE ...)
    let from_idx = find_keyword_top_level(tail_stripped, "FROM");
    let where_idx = find_keyword_top_level(tail_stripped, "WHERE");

    let (assignments_raw, selection, from_clause) = if let Some(fi) = from_idx {
        let assignments_part = &tail_stripped[..fi];
        let after_from = &tail_stripped[fi + "FROM".len()..];
        let where_in_from = find_keyword_top_level(after_from, "WHERE");

        let (from_source, where_part) = if let Some(wi) = where_in_from {
            (&after_from[..wi], &after_from[wi + "WHERE".len()..])
        } else {
            (after_from, "")
        };

        let fc = parse_update_from_clause(from_source.trim())?;
        let selection = if !where_part.trim().is_empty() {
            Some(crate::parser::expression::parse_expr(where_part.trim())?)
        } else {
            None
        };
        (assignments_part.trim(), selection, Some(fc))
    } else if let Some(idx) = where_idx {
        let assignments_raw = &tail_stripped[..idx];
        let selection = crate::parser::expression::parse_expr(tail_stripped[idx + "WHERE".len()..].trim())?;
        (assignments_raw.trim(), Some(selection), None)
    } else {
        (tail_stripped, None, None)
    };

    let assignments = split_csv_top_level(assignments_raw)
        .into_iter()
        .map(|part| parse_assignment(part.trim()))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Statement::Update(UpdateStmt {
        table,
        assignments,
        selection,
        from: from_clause,
        output,
        output_into,
    }))
}

pub(crate) fn parse_assignment(input: &str) -> Result<Assignment, DbError> {
    let eq_idx = input
        .find('=')
        .ok_or_else(|| DbError::Parse("SET assignment missing '='".into()))?;
    let expr_raw = input[eq_idx + 1..].trim();
    let (processed, subquery_map) = crate::parser::statements::subquery_utils::extract_subqueries(expr_raw);
    let mut expr =
        crate::parser::expression::parse_expr_with_subqueries(&processed, &subquery_map)?;
    crate::parser::statements::subquery_utils::apply_subquery_map(&mut expr, &subquery_map);

    // Strip table alias prefix from column name (e.g., "t.col" -> "col")
    let column_raw = input[..eq_idx].trim();
    let column = if let Some(dot_pos) = column_raw.rfind('.') {
        column_raw[dot_pos + 1..].trim().to_string()
    } else {
        column_raw.to_string()
    };

    Ok(Assignment {
        column,
        expr,
    })
}

pub(crate) fn parse_update_from_clause(input: &str) -> Result<FromClause, DbError> {
    let mut tables = Vec::new();
    let mut all_joins = Vec::new();
    let mut all_applies = Vec::new();

    for part in crate::parser::utils::split_csv_top_level(input) {
        let trimmed = part.trim();
        let (base, joins, applies) = crate::parser::statements::select::parse_from_source_internal(trimmed)?;
        tables.push(base);
        all_joins.extend(joins);
        all_applies.extend(applies);
    }

    Ok(FromClause {
        tables,
        joins: all_joins,
        applies: all_applies,
    })
}
