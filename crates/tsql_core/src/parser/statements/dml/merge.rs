use crate::ast::*;
use crate::error::DbError;
use crate::parser::utils::{find_keyword_top_level, parse_table_ref};
use super::output::parse_output_clause;
use super::update::parse_assignment;
use super::insert::parse_values_groups;

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
        let select_stmt = crate::parser::statements::select::parse_select(inner)?;
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
    let on_condition = crate::parser::expression::parse_expr(on_condition_str.trim())?;

    let when_section = &after_on[when_idx..];

    // Parse WHEN clauses
    let mut when_clauses = Vec::new();
    let mut remaining = when_section;
    let mut output = None;
    let mut output_into = None;

    loop {
        let Some(when_idx_local) = find_keyword_top_level(remaining, "WHEN") else {
            break;
        };

        let after_current_when = &remaining[when_idx_local + "WHEN".len()..];

        let (when_kind, condition, action_start) = {
            let mut current_pos = 0;
            while current_pos < after_current_when.len()
                && after_current_when.as_bytes()[current_pos].is_ascii_whitespace()
            {
                current_pos += 1;
            }

            let upper_rest = after_current_when[current_pos..].to_uppercase();
            let (kind, kind_len) = if upper_rest.starts_with("NOT MATCHED BY SOURCE") {
                (
                    MergeWhen::NotMatchedBySource,
                    "NOT MATCHED BY SOURCE".len(),
                )
            } else if upper_rest.starts_with("NOT MATCHED") {
                (MergeWhen::NotMatched, "NOT MATCHED".len())
            } else if upper_rest.starts_with("MATCHED") {
                (MergeWhen::Matched, "MATCHED".len())
            } else {
                return Err(DbError::Parse("invalid WHEN clause in MERGE".into()));
            };

            current_pos += kind_len;
            while current_pos < after_current_when.len()
                && after_current_when.as_bytes()[current_pos].is_ascii_whitespace()
            {
                current_pos += 1;
            }

            let upper_rest2 = after_current_when[current_pos..].to_uppercase();
            let (cond, action_pos) = if upper_rest2.starts_with("AND ") {
                let then_idx = find_keyword_top_level(&after_current_when[current_pos..], "THEN")
                    .ok_or_else(|| DbError::Parse("MERGE WHEN clause missing THEN".into()))?;
                let cond_str = after_current_when[current_pos + 4..current_pos + then_idx].trim();
                (Some(crate::parser::expression::parse_expr(cond_str)?), current_pos + then_idx + "THEN".len())
            } else if upper_rest2.starts_with("THEN") {
                (None, current_pos + "THEN".len())
            } else {
                return Err(DbError::Parse("MERGE WHEN clause missing THEN".into()));
            };

            (kind, cond, action_pos)
        };

        // Find the end of this action (next WHEN or OUTPUT or end)
        // Search in the original remaining string after the current WHEN
        let next_when_in_action = find_keyword_top_level(after_current_when, "WHEN");
        let output_in_action = find_keyword_top_level(after_current_when, "OUTPUT");

        let action_end_pos = next_when_in_action
            .or(output_in_action)
            .unwrap_or(after_current_when.len());

        let action_str = after_current_when[action_start..action_end_pos].trim();
        let action_upper = action_str.to_uppercase();

        let action = if action_upper.starts_with("UPDATE SET") {
            let set_part = &action_str["UPDATE SET".len()..].trim();
            let assignments = crate::parser::utils::split_csv_top_level(set_part)
                .into_iter()
                .map(|part| parse_assignment(part.trim()))
                .collect::<Result<Vec<_>, _>>()?;
            MergeAction::Update { assignments }
        } else if action_upper.starts_with("INSERT") {
            let after_insert = &action_str["INSERT".len()..].trim();
            let open_idx = after_insert.find('(').ok_or_else(|| {
                DbError::Parse("INSERT in MERGE missing column list".into())
            })?;
            // Find matching closing paren (not rfind, which gets the last one)
            let col_start = open_idx + 1;
            let mut paren_depth = 1usize;
            let mut close_idx = None;
            for (i, ch) in after_insert[col_start..].char_indices() {
                match ch {
                    '(' => paren_depth += 1,
                    ')' => {
                        paren_depth -= 1;
                        if paren_depth == 0 {
                            close_idx = Some(col_start + i);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let close_idx = close_idx.ok_or_else(|| {
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
            condition,
            action,
        });

        // Advance remaining past this entire WHEN clause
        let consumed = when_idx_local + "WHEN".len() + action_end_pos;
        if consumed >= remaining.len() {
            break;
        }
        remaining = &remaining[consumed..];
    }

    // Check for OUTPUT
    if let Some(oi) = find_keyword_top_level(remaining, "OUTPUT") {
        let output_str = &remaining[oi + "OUTPUT".len()..].trim();
        let (parsed_output, into_target) = parse_output_clause(output_str)?;
        output = Some(parsed_output);
        output_into = into_target;
    }

    Ok(Statement::Merge(MergeStmt {
        target,
        source,
        on_condition,
        when_clauses,
        output,
        output_into,
    }))
}
