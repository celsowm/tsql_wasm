use crate::error::DbError;
use crate::types::Value;

use super::context::ExecutionContext;
use super::model::ContextTable;

pub(crate) fn resolve_identifier(
    row: &[ContextTable],
    name: &str,
    ctx: &ExecutionContext,
) -> Result<Value, DbError> {
    if name.starts_with("@@") {
        if let Some(val) = super::metadata::system_vars::resolve_system_variable(name, ctx)? {
            return Ok(val);
        }
    }
    if name.starts_with('@') {
        match ctx.session.variables.get(name) {
            Some((_, val)) => return Ok(val.clone()),
            None => {
                return Err(DbError::invalid_identifier(name));
            }
        }
    }

    let mut matches: Vec<(usize, Value)> = Vec::new();
    for (binding_idx, binding) in row.iter().enumerate() {
        if let Some(col_idx) = binding
            .table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
        {
            let value = binding
                .row
                .as_ref()
                .map(|r| r.values[col_idx].clone())
                .unwrap_or(Value::Null);
            matches.push((binding_idx, value));
        }
    }

    if matches.is_empty() {
        for apply_row in ctx.row.apply_stack.iter().rev() {
            for binding in apply_row.iter() {
                if let Some(col_idx) = binding
                    .table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(name))
                {
                    let value = binding
                        .row
                        .as_ref()
                        .map(|r| r.values[col_idx].clone())
                        .unwrap_or(Value::Null);
                    matches.push((0, value));
                }
            }
            if !matches.is_empty() {
                break;
            }
        }
    }

    if matches.is_empty() {
        for outer_row in ctx.row.outer_stack.iter().rev() {
            for binding in outer_row.iter() {
                if let Some(col_idx) = binding
                    .table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(name))
                {
                    let value = binding
                        .row
                        .as_ref()
                        .map(|r| r.values[col_idx].clone())
                        .unwrap_or(Value::Null);
                    matches.push((0, value));
                }
            }
            if !matches.is_empty() {
                break;
            }
        }
    }

    match matches.len() {
        0 => Err(DbError::column_not_found(name)),
        1 => Ok(matches[0].1.clone()),
        _ => Err(DbError::invalid_identifier(format!("ambiguous column '{}'", name))),
    }
}

pub(crate) fn resolve_qualified_identifier(
    row: &[ContextTable],
    parts: &[String],
    ctx: &ExecutionContext,
) -> Result<Value, DbError> {
    if parts.len() != 2 {
        return Err(DbError::invalid_identifier("only two-part identifiers are supported in this build"));
    }

    let table_name = &parts[0];
    let column_name = &parts[1];

    let search_row = |row: &[ContextTable]| -> Option<Value> {
        for binding in row {
            if binding.alias.eq_ignore_ascii_case(table_name)
                || binding.table.name.eq_ignore_ascii_case(table_name)
            {
                let idx = binding
                    .table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(column_name))?;
                return Some(
                    binding
                        .row
                        .as_ref()
                        .map(|r| r.values[idx].clone())
                        .unwrap_or(Value::Null),
                );
            }
        }
        None
    };

    if let Some(val) = search_row(row) {
        return Ok(val);
    }

    for apply_row in ctx.row.apply_stack.iter().rev() {
        if let Some(val) = search_row(apply_row) {
            return Ok(val);
        }
    }

    for outer_row in ctx.row.outer_stack.iter().rev() {
        if let Some(val) = search_row(outer_row) {
            return Ok(val);
        }
    }

    Err(DbError::object_not_found(table_name))
}
