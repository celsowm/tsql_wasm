use crate::ast::*;
use crate::error::DbError;

use crate::parser::expression::parse_expr;
use crate::parser::statements::subquery_utils::{apply_subquery_map, extract_subqueries};
use crate::parser::utils::{find_keyword_top_level, parse_object_name, parse_table_ref, split_csv_top_level};

fn parse_output_clause(input: &str) -> Result<Vec<OutputColumn>, DbError> {
    let mut columns = Vec::new();
    for part in split_csv_top_level(input) {
        let trimmed = part.trim();
        let upper = trimmed.to_uppercase();
        let (source, rest) = if upper.starts_with("INSERTED.") {
            (OutputSource::Inserted, &trimmed["INSERTED.".len()..])
        } else if upper.starts_with("DELETED.") {
            (OutputSource::Deleted, &trimmed["DELETED.".len()..])
        } else {
            return Err(DbError::Parse(
                "OUTPUT columns must reference INSERTED. or DELETED.".into(),
            ));
        };

        // Check for alias (AS alias)
        let rest_upper = rest.trim().to_uppercase();
        let (col_name, alias) = if let Some(as_idx) = rest_upper.find(" AS ") {
            let col = rest[..as_idx].trim().to_string();
            let al = rest[as_idx + " AS ".len()..].trim().to_string();
            (col, Some(al))
        } else {
            (rest.trim().to_string(), None)
        };

        columns.push(OutputColumn {
            source,
            column: col_name,
            alias,
        });
    }
    Ok(columns)
}

pub(crate) fn parse_insert(sql: &str) -> Result<Statement, DbError> {
    let after_into = sql["INSERT INTO".len()..].trim();
    let upper = after_into.to_uppercase();

    if upper.ends_with("DEFAULT VALUES") {
        let table_name = after_into[..after_into.len() - "DEFAULT VALUES".len()].trim();
        return Ok(Statement::Insert(InsertStmt {
            table: parse_object_name(table_name),
            columns: None,
            values: vec![],
            default_values: true,
            select_source: None,
            output: None,
        }));
    }

    // Check for OUTPUT clause
    let output_idx = find_keyword_top_level(after_into, "OUTPUT");
    let _select_idx = find_keyword_top_level(after_into, "SELECT");
    let _values_idx = find_keyword_top_level(after_into, "VALUES");

    // Determine OUTPUT content
    let (output, after_into_stripped) = if let Some(oi) = output_idx {
        // OUTPUT INSERTED.col1, DELETED.col1
        // The OUTPUT is between table/columns and VALUES/SELECT
        let after_output = &after_into[oi + "OUTPUT".len()..];
        let end_idx = find_keyword_top_level(after_output, "VALUES")
            .or_else(|| find_keyword_top_level(after_output, "SELECT"))
            .unwrap_or(after_output.len());
        let output_raw = &after_output[..end_idx];
        let parsed_output = parse_output_clause(output_raw.trim())?;
        (Some(parsed_output), &after_into[..oi])
    } else {
        (None, after_into)
    };

    // Re-check with stripped string
    let select_idx_stripped = find_keyword_top_level(after_into_stripped, "SELECT");
    let values_idx_stripped = find_keyword_top_level(after_into_stripped, "VALUES");

    if let Some(sel_idx) = select_idx_stripped {
        // INSERT ... SELECT
        if values_idx_stripped.is_none() || sel_idx < values_idx_stripped.unwrap() {
            let head = after_into_stripped[..sel_idx].trim();
            let (table_name, columns) = if let Some(open) = head.find('(') {
                let close = head
                    .rfind(')')
                    .ok_or_else(|| DbError::Parse("missing ')' in INSERT columns".into()))?;
                let table_name = head[..open].trim();
                let cols = head[open + 1..close]
                    .split(',')
                    .map(|c| c.trim().to_string())
                    .collect::<Vec<_>>();
                (table_name.to_string(), Some(cols))
            } else {
                (head.to_string(), None)
            };

            let select_sql = &after_into_stripped[sel_idx..];
            let select_stmt = super::select::parse_select(select_sql)?;
            if let Statement::Select(sel) = select_stmt {
                return Ok(Statement::Insert(InsertStmt {
                    table: parse_object_name(&table_name),
                    columns,
                    values: vec![],
                    default_values: false,
                    select_source: Some(Box::new(sel)),
                    output,
                }));
            } else {
                return Err(DbError::Parse("expected SELECT in INSERT ... SELECT".into()));
            }
        }
    }

    // INSERT ... VALUES
    let values_idx = values_idx_stripped
        .ok_or_else(|| DbError::Parse("INSERT missing VALUES or SELECT".into()))?;

    let head = after_into_stripped[..values_idx].trim();
    let values_part = after_into_stripped[values_idx + "VALUES".len()..].trim();

    let (table_name, columns) = if let Some(open) = head.find('(') {
        let close = head
            .rfind(')')
            .ok_or_else(|| DbError::Parse("missing ')' in INSERT columns".into()))?;
        let table_name = head[..open].trim();
        let cols = head[open + 1..close]
            .split(',')
            .map(|c| c.trim().to_string())
            .collect::<Vec<_>>();
        (table_name.to_string(), Some(cols))
    } else {
        (head.to_string(), None)
    };

    let table = parse_object_name(&table_name);
    let values = parse_values_groups(values_part)?;

    Ok(Statement::Insert(InsertStmt {
        table,
        columns,
        values,
        default_values: false,
        select_source: None,
        output,
    }))
}

pub(crate) fn parse_update(sql: &str) -> Result<Statement, DbError> {
    let after_update = sql["UPDATE".len()..].trim();
    let set_idx = find_keyword_top_level(after_update, "SET")
        .ok_or_else(|| DbError::Parse("UPDATE missing SET".into()))?;

    let table = parse_object_name(after_update[..set_idx].trim());
    let tail = after_update[set_idx + "SET".len()..].trim();

    // Check for OUTPUT clause
    let output_idx = find_keyword_top_level(tail, "OUTPUT");

    let (output, tail_stripped) = if let Some(oi) = output_idx {
        let after_output = &tail[oi + "OUTPUT".len()..];
        let end_idx = find_keyword_top_level(after_output, "FROM")
            .or_else(|| find_keyword_top_level(after_output, "WHERE"))
            .unwrap_or(after_output.len());
        let output_raw = &after_output[..end_idx];
        let parsed_output = parse_output_clause(output_raw.trim())?;
        (Some(parsed_output), &tail[..oi])
    } else {
        (None, tail)
    };

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
            Some(parse_expr(where_part.trim())?)
        } else {
            None
        };
        (assignments_part.trim(), selection, Some(fc))
    } else if let Some(idx) = where_idx {
        let assignments_raw = &tail_stripped[..idx];
        let selection = parse_expr(tail_stripped[idx + "WHERE".len()..].trim())?;
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
    }))
}

pub(crate) fn parse_delete(sql: &str) -> Result<Statement, DbError> {
    let after_delete = sql["DELETE FROM".len()..].trim();
    let _upper = after_delete.to_uppercase();

    // Check if there's OUTPUT clause
    let output_idx = find_keyword_top_level(after_delete, "OUTPUT");
    let (output, after_delete_stripped) = if let Some(oi) = output_idx {
        let after_output = &after_delete[oi + "OUTPUT".len()..];
        let end_idx = find_keyword_top_level(after_output, "FROM")
            .or_else(|| find_keyword_top_level(after_output, "WHERE"))
            .unwrap_or(after_output.len());
        let output_raw = &after_output[..end_idx];
        let parsed_output = parse_output_clause(output_raw.trim())?;
        (Some(parsed_output), &after_delete[..oi])
    } else {
        (None, after_delete)
    };

    let _upper_stripped = after_delete_stripped.to_uppercase();

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
            Some(parse_expr(where_part.trim())?)
        } else {
            None
        };

        return Ok(Statement::Delete(DeleteStmt {
            table,
            selection,
            from: Some(from_clause),
            output,
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
        .map(|idx| parse_expr(after_delete_stripped[idx + "WHERE".len()..].trim()))
        .transpose()?;

    Ok(Statement::Delete(DeleteStmt {
        table,
        selection,
        from: None,
        output,
    }))
}

fn parse_assignment(input: &str) -> Result<Assignment, DbError> {
    let eq_idx = input
        .find('=')
        .ok_or_else(|| DbError::Parse("SET assignment missing '='".into()))?;
    let expr_raw = input[eq_idx + 1..].trim();
    let (processed, subquery_map) = extract_subqueries(expr_raw);
    let mut expr =
        crate::parser::expression::parse_expr_with_subqueries(&processed, &subquery_map)?;
    apply_subquery_map(&mut expr, &subquery_map);
    Ok(Assignment {
        column: input[..eq_idx].trim().to_string(),
        expr,
    })
}

fn parse_values_groups(input: &str) -> Result<Vec<Vec<Expr>>, DbError> {
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
        let exprs = split_csv_top_level(inner)
            .into_iter()
            .map(|s| parse_expr(s.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        out.push(exprs);
    }

    Ok(out)
}

fn parse_update_from_clause(input: &str) -> Result<FromClause, DbError> {
    // First check for explicit JOINs
    let join_patterns = [
        ("FULL OUTER JOIN", JoinType::Full),
        ("FULL JOIN", JoinType::Full),
        ("LEFT JOIN", JoinType::Left),
        ("RIGHT JOIN", JoinType::Right),
        ("INNER JOIN", JoinType::Inner),
        ("JOIN", JoinType::Inner),
    ];

    let _upper = input.to_uppercase();
    let mut first_join_pos: Option<(usize, JoinType, usize)> = None;
    for (pat, ty) in &join_patterns {
        if let Some(idx) = find_keyword_top_level(input, pat) {
            if first_join_pos.is_none() || idx < first_join_pos.as_ref().unwrap().0 {
                first_join_pos = Some((idx, *ty, pat.len()));
            }
        }
    }

    if let Some((idx, join_type, pat_len)) = first_join_pos {
        // There are explicit JOINs
        let base_source = input[..idx].trim();
        let after_first_join = input[idx + pat_len..].trim();

        // The base might have multiple tables separated by comma
        let mut tables = Vec::new();
        for part in split_csv_top_level(base_source) {
            tables.push(parse_table_ref(part.trim())?);
        }

        let mut joins = Vec::new();
        // Parse the first join
        let on_idx = find_keyword_top_level(after_first_join, "ON")
            .ok_or_else(|| DbError::Parse("JOIN missing ON".into()))?;
        let join_table = parse_table_ref(after_first_join[..on_idx].trim())?;
        let after_on = &after_first_join[on_idx + "ON".len()..];

        // Find next join or end
        let mut next_join: Option<(usize, JoinType, usize)> = None;
        for (pat, ty) in &join_patterns {
            if let Some(ni) = find_keyword_top_level(after_on, pat) {
                if next_join.is_none() || ni < next_join.as_ref().unwrap().0 {
                    next_join = Some((ni, *ty, pat.len()));
                }
            }
        }

        let on_expr_str = if let Some((ni, _, _)) = &next_join {
            &after_on[..*ni]
        } else {
            after_on
        };
        let on_expr = parse_expr(on_expr_str.trim())?;
        joins.push(JoinClause {
            join_type,
            table: join_table,
            on: Some(on_expr),
        });

        // TODO: parse additional joins if present
        let _ = next_join;

        Ok(FromClause { tables, joins })
    } else {
        // No explicit JOINs - comma-separated tables (implicit CROSS JOIN)
        let mut tables = Vec::new();
        for part in split_csv_top_level(input) {
            tables.push(parse_table_ref(part.trim())?);
        }
        Ok(FromClause {
            tables,
            joins: vec![],
        })
    }
}

pub(crate) fn parse_merge(sql: &str) -> Result<Statement, DbError> {
    let upper = sql.to_uppercase();
    let after_merge = if upper.starts_with("MERGE INTO ") {
        &sql["MERGE INTO ".len()..]
    } else {
        &sql["MERGE ".len()..]
    }
    .trim();

    // Find USING
    let using_idx = find_keyword_top_level(after_merge, "USING")
        .ok_or_else(|| DbError::Parse("MERGE missing USING".into()))?;

    let target_raw = after_merge[..using_idx].trim();
    let after_using = after_merge[using_idx + "USING".len()..].trim();

    // Parse target (with optional alias)
    let target = parse_table_ref(target_raw)?;

    // Find ON
    let on_idx = find_keyword_top_level(after_using, "ON")
        .ok_or_else(|| DbError::Parse("MERGE missing ON".into()))?;

    let source_raw = after_using[..on_idx].trim();
    let after_on = after_using[on_idx + "ON".len()..].trim();

    // Parse source (table or subquery)
    let source = if source_raw.starts_with('(') {
        // Subquery source: (SELECT ...) AS alias
        let inner_end = source_raw
            .rfind(')')
            .ok_or_else(|| DbError::Parse("unclosed subquery in MERGE USING".into()))?;
        let inner = &source_raw[1..inner_end];
        let after_subquery = source_raw[inner_end + 1..].trim();
        let alias = if after_subquery.to_uppercase().starts_with("AS ") {
            Some(after_subquery["AS ".len()..].trim().to_string())
        } else if !after_subquery.is_empty() {
            Some(after_subquery.to_string())
        } else {
            None
        };
        let select_stmt = super::select::parse_select(inner)?;
        if let Statement::Select(sel) = select_stmt {
            MergeSource::Subquery(sel, alias)
        } else {
            return Err(DbError::Parse("expected SELECT in MERGE USING subquery".into()));
        }
    } else {
        MergeSource::Table(parse_table_ref(source_raw)?)
    };

    // Find first WHEN clause
    let when_idx = find_keyword_top_level(after_on, "WHEN")
        .ok_or_else(|| DbError::Parse("MERGE missing WHEN".into()))?;

    let on_condition_str = &after_on[..when_idx];
    let on_condition = parse_expr(on_condition_str.trim())?;

    let when_section = &after_on[when_idx..];

    // Parse WHEN clauses
    let mut when_clauses = Vec::new();
    let mut remaining = when_section;
    let mut output = None;

    loop {
        let Some(when_idx_local) = find_keyword_top_level(remaining, "WHEN") else {
            break;
        };
        let after_when = &remaining[when_idx_local + "WHEN".len()..].trim();
        let upper_when = after_when.to_uppercase();

        let (when_kind, _after_when_kind) = if upper_when.starts_with("MATCHED THEN") {
            (MergeWhen::Matched, after_when["MATCHED THEN".len()..].trim())
        } else if upper_when.starts_with("NOT MATCHED BY SOURCE THEN") {
            (
                MergeWhen::NotMatchedBySource,
                after_when["NOT MATCHED BY SOURCE THEN".len()..].trim(),
            )
        } else if upper_when.starts_with("NOT MATCHED THEN") {
            (
                MergeWhen::NotMatched,
                after_when["NOT MATCHED THEN".len()..].trim(),
            )
        } else {
            return Err(DbError::Parse("invalid WHEN clause in MERGE".into()));
        };

        // Find the end of this action (next WHEN or OUTPUT or end)
        // Search in the original remaining string after the current WHEN
        let after_current_when = &remaining[when_idx_local + "WHEN".len()..];
        let next_when_in_action = find_keyword_top_level(after_current_when, "WHEN");
        let output_in_action = find_keyword_top_level(after_current_when, "OUTPUT");

        let action_end_pos = next_when_in_action
            .or(output_in_action)
            .unwrap_or(after_current_when.len());

        let action_str = after_current_when[..action_end_pos].trim();
        let action_upper = action_str.to_uppercase();

        let action = if action_upper.starts_with("UPDATE SET") {
            let set_part = &action_str["UPDATE SET".len()..].trim();
            let assignments = split_csv_top_level(set_part)
                .into_iter()
                .map(|part| parse_assignment(part.trim()))
                .collect::<Result<Vec<_>, _>>()?;
            MergeAction::Update { assignments }
        } else if action_upper.starts_with("INSERT") {
            let after_insert = &action_str["INSERT".len()..].trim();
            let open_idx = after_insert.find('(').ok_or_else(|| {
                DbError::Parse("INSERT in MERGE missing column list".into())
            })?;
            let close_idx = after_insert.rfind(')').ok_or_else(|| {
                DbError::Parse("INSERT in MERGE missing closing ')'".into())
            })?;
            let columns: Vec<String> = after_insert[open_idx + 1..close_idx]
                .split(',')
                .map(|c| c.trim().to_string())
                .collect();
            let values_part = &after_insert[close_idx + 1..].trim();
            if !values_part.to_uppercase().starts_with("VALUES") {
                return Err(DbError::Parse(
                    "INSERT in MERGE missing VALUES".into(),
                ));
            }
            let values_str = &values_part["VALUES".len()..].trim();
            let values = parse_values_groups(values_str)?
                .into_iter()
                .flatten()
                .collect();
            MergeAction::Insert { columns, values }
        } else if action_upper.starts_with("DELETE") {
            MergeAction::Delete
        } else {
            return Err(DbError::Parse("invalid action in MERGE WHEN clause".into()));
        };

        when_clauses.push(MergeWhenClause {
            when: when_kind,
            condition: None,
            action,
        });

        // Advance remaining past this entire WHEN clause
        // remaining[when_idx_local..] contains "WHEN" + rest
        // after_current_when = remaining[when_idx_local + "WHEN".len()..]
        // action_end_pos is relative to after_current_when
        let consumed = when_idx_local + "WHEN".len() + action_end_pos;
        if consumed >= remaining.len() {
            break;
        }
        remaining = &remaining[consumed..];
    }

    // Check for OUTPUT
    if let Some(oi) = find_keyword_top_level(remaining, "OUTPUT") {
        let output_str = &remaining[oi + "OUTPUT".len()..].trim();
        output = Some(parse_output_clause(output_str)?);
    }

    Ok(Statement::Merge(MergeStmt {
        target,
        source,
        on_condition,
        when_clauses,
        output,
    }))
}
