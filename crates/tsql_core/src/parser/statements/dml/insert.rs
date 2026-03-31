use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::{find_keyword_top_level, parse_object_name};
use super::output::parse_output_clause;

pub(crate) fn parse_insert(sql: &str) -> Result<Statement, DbError> {
    let mut after_into = sql["INSERT".len()..].trim();
    if after_into.to_uppercase().starts_with("INTO ") {
        after_into = after_into[5..].trim();
    }
    let upper = after_into.to_uppercase();

    if upper.ends_with("DEFAULT VALUES") {
        let table_name = after_into[..after_into.len() - "DEFAULT VALUES".len()].trim();
        return Ok(Statement::Insert(InsertStmt {
            table: parse_object_name(table_name),
            columns: None,
            source: InsertSource::DefaultValues,
            output: None,
            output_into: None,
        }));
    }

    // Check for OUTPUT clause
    let output_idx = find_keyword_top_level(after_into, "OUTPUT");

    // Determine OUTPUT content
    let (output, output_into, after_into_no_output) = if let Some(oi) = output_idx {
        let after_output = &after_into[oi + "OUTPUT".len()..];
        let end_idx = find_keyword_top_level(after_output, "VALUES")
            .or_else(|| find_keyword_top_level(after_output, "SELECT"))
            .or_else(|| find_keyword_top_level(after_output, "EXEC"))
            .unwrap_or(after_output.len());
        let output_raw = &after_output[..end_idx];
        let (parsed_output, into_target) = parse_output_clause(output_raw.trim())?;
        let before_output = &after_into[..oi];
        let after_output_cols = &after_output[end_idx..];
        let reconstructed = format!("{}{}", before_output, after_output_cols);
        (Some(parsed_output), into_target, reconstructed)
    } else {
        (None, None, after_into.to_string())
    };

    let after_into_stripped = &after_into_no_output;

    // Re-check with stripped string
    let select_idx_stripped = find_keyword_top_level(after_into_stripped, "SELECT");
    let values_idx_stripped = find_keyword_top_level(after_into_stripped, "VALUES");
    let exec_idx_stripped = find_keyword_top_level(after_into_stripped, "EXEC");

    if let Some(sel_idx) = select_idx_stripped {
        // Check if SELECT is before VALUES and EXEC
        let before_values = values_idx_stripped.map_or(true, |v| sel_idx < v);
        let before_exec = exec_idx_stripped.map_or(true, |e| sel_idx < e);
        if before_values && before_exec {
            let head = after_into_stripped[..sel_idx].trim();
            let (table_name, columns) = parse_table_and_columns(head)?;
            let select_sql = &after_into_stripped[sel_idx..];
            let select_stmt = crate::parser::statements::select::parse_select(select_sql)?;
            if let Statement::Select(sel) = select_stmt {
                return Ok(Statement::Insert(InsertStmt {
                    table: parse_object_name(&table_name),
                    columns,
                    source: InsertSource::Select(Box::new(sel)),
                    output,
                    output_into,
                }));
            }
        }
    }

    if let Some(exec_idx) = exec_idx_stripped {
        if values_idx_stripped.map_or(true, |v| exec_idx < v) {
            let head = after_into_stripped[..exec_idx].trim();
            let (table_name, columns) = parse_table_and_columns(head)?;
            let exec_sql = &after_into_stripped[exec_idx..];
            let exec_stmt = crate::parser::parse_sql(exec_sql)?;
            return Ok(Statement::Insert(InsertStmt {
                table: parse_object_name(&table_name),
                columns,
                source: InsertSource::Exec(Box::new(exec_stmt)),
                output,
                output_into,
            }));
        }
    }

    // INSERT ... VALUES
    let values_idx = values_idx_stripped
        .ok_or_else(|| DbError::Parse("INSERT missing VALUES, SELECT or EXEC".into()))?;

    let head = after_into_stripped[..values_idx].trim();
    let values_part = after_into_stripped[values_idx + "VALUES".len()..].trim();

    let (table_name, columns) = parse_table_and_columns(head)?;
    let table = parse_object_name(&table_name);
    let values = parse_values_groups(values_part)?;

    Ok(Statement::Insert(InsertStmt {
        table,
        columns,
        source: InsertSource::Values(values),
        output,
        output_into,
    }))
}

fn parse_table_and_columns(head: &str) -> Result<(String, Option<Vec<String>>), DbError> {
    if let Some(open) = head.find('(') {
        let close = head
            .rfind(')')
            .ok_or_else(|| DbError::Parse("missing ')' in INSERT columns".into()))?;
        let table_name = head[..open].trim();
        let cols = head[open + 1..close]
            .split(',')
            .map(|c| c.trim().to_string())
            .collect::<Vec<_>>();
        Ok((table_name.to_string(), Some(cols)))
    } else {
        Ok((head.to_string(), None))
    }
}

pub(crate) fn parse_values_groups(input: &str) -> Result<Vec<Vec<Expr>>, DbError> {
    let mut out = Vec::new();
    let chars = input.chars().collect::<Vec<_>>();
    let mut i = 0usize;

    while i < chars.len() {
        while i < chars.len() && (chars[i].is_whitespace() || chars[i] == ',') {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        if chars[i] != '(' {
            return Err(DbError::Parse("expected '(' starting VALUES tuple".into()));
        }

        let start = i + 1;
        let mut depth = 1usize;
        let mut in_string = false;
        i += 1;
        while i < chars.len() && depth > 0 {
            match chars[i] {
                '\'' => in_string = !in_string,
                '(' if !in_string => depth += 1,
                ')' if !in_string => depth -= 1,
                _ => {}
            }
            i += 1;
        }

        if depth != 0 {
            return Err(DbError::Parse("unclosed VALUES tuple".into()));
        }

        let inner = &input[start..i - 1];
        let exprs = crate::parser::utils::split_csv_top_level(inner)
            .into_iter()
            .map(|s| crate::parser::expression::parse_expr(s.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        out.push(exprs);
    }

    Ok(out)
}
