use crate::ast::{OutputColumn, ObjectName, OutputSource};
use crate::error::DbError;
use crate::parser::utils::{find_keyword_top_level, parse_object_name, split_csv_top_level};

pub(crate) fn parse_output_clause(input: &str) -> Result<(Vec<OutputColumn>, Option<ObjectName>), DbError> {
    let mut input_to_use = input;
    let mut into_target = None;

    if let Some(into_idx) = find_keyword_top_level(input, "INTO") {
        let before_into = input[..into_idx].trim();
        let after_into = input[into_idx + "INTO".len()..].trim();
        let first_space = after_into.find(|c: char| c.is_whitespace()).unwrap_or(after_into.len());
        let table_name = &after_into[..first_space];
        into_target = Some(parse_object_name(table_name));
        input_to_use = before_into;
    }

    let mut columns = Vec::new();
    for part in split_csv_top_level(input_to_use) {
        let trimmed = part.trim();
        let upper = trimmed.to_uppercase();
        let (source, rest) = if upper.starts_with("INSERTED.") {
            (OutputSource::Inserted, &trimmed["INSERTED.".len()..])
        } else if upper.starts_with("DELETED.") {
            (OutputSource::Deleted, &trimmed["DELETED.".len()..])
        } else if upper == "INSERTED" {
            return Err(DbError::Parse(
                "OUTPUT columns must reference INSERTED.column or INSERTED.*".into(),
            ));
        } else if upper == "DELETED" {
            return Err(DbError::Parse(
                "OUTPUT columns must reference DELETED.column or DELETED.*".into(),
            ));
        } else {
            return Err(DbError::Parse(
                "OUTPUT columns must reference INSERTED. or DELETED.".into(),
            ));
        };

        let rest_trimmed = rest.trim();

        // Handle wildcard: INSERTED.* or DELETED.*
        if rest_trimmed == "*" {
            columns.push(OutputColumn {
                source,
                column: "*".to_string(),
                alias: None,
                is_wildcard: true,
            });
            continue;
        }

        // Check for alias (AS alias)
        let rest_upper = rest_trimmed.to_uppercase();
        let (col_name, alias) = if let Some(as_idx) = rest_upper.find(" AS ") {
            let col = rest_trimmed[..as_idx].trim().to_string();
            let al = rest_trimmed[as_idx + " AS ".len()..].trim().to_string();
            (col, Some(al))
        } else {
            (rest_trimmed.to_string(), None)
        };

        columns.push(OutputColumn {
            source,
            column: col_name,
            alias,
            is_wildcard: false,
        });
    }
    Ok((columns, into_target))
}
