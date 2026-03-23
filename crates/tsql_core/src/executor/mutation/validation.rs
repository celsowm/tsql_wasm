use crate::ast::Assignment;
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};
use crate::types::{DataType, Value};

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::evaluator::{eval_expr_to_type_in_context, eval_predicate};
use super::super::model::single_row_context;
use super::super::value_ops::compare_values;

pub(crate) fn enforce_string_length(
    data_type: &DataType,
    value: &Value,
    col_name: &str,
) -> Result<(), DbError> {
    let max_len = match data_type {
        DataType::Char { len } | DataType::NChar { len } => Some(*len as usize),
        DataType::VarChar { max_len } | DataType::NVarChar { max_len } => Some(*max_len as usize),
        DataType::Binary { len } | DataType::VarBinary { max_len: len } => Some(*len as usize),
        _ => None,
    };

    if let Some(max) = max_len {
        let actual_len = match value {
            Value::Char(s) | Value::VarChar(s) | Value::NChar(s) | Value::NVarChar(s) => {
                Some(s.len())
            }
            Value::Binary(v) | Value::VarBinary(v) => Some(v.len()),
            _ => None,
        };
        if let Some(actual_len) = actual_len {
            if actual_len > max {
                return Err(DbError::Execution(format!(
                    "String or binary data would be truncated for column '{}'",
                    col_name
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn enforce_unique_on_insert(
    table: &TableDef,
    storage: &dyn Storage,
    table_id: u32,
    new_row: &StoredRow,
) -> Result<(), DbError> {
    let rows = storage.get_rows(table_id)?;

    for (col_idx, col) in table.columns.iter().enumerate().filter(|(_, c)| c.unique) {
        let new_val = &new_row.values[col_idx];
        if new_val.is_null() {
            continue;
        }
        for existing in rows.iter().filter(|r| !r.deleted) {
            let existing_val = &existing.values[col_idx];
            if !existing_val.is_null()
                && compare_values(new_val, existing_val) == std::cmp::Ordering::Equal
            {
                return Err(DbError::Execution(format!(
                    "Violation of UNIQUE KEY constraint on column '{}'. Cannot insert duplicate key.",
                    col.name
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn enforce_unique_on_update(
    table: &TableDef,
    storage: &dyn Storage,
    table_id: u32,
    updated_row: &StoredRow,
    updated_idx: usize,
) -> Result<(), DbError> {
    let rows = storage.get_rows(table_id)?;

    for (col_idx, col) in table.columns.iter().enumerate().filter(|(_, c)| c.unique) {
        let new_val = &updated_row.values[col_idx];
        if new_val.is_null() {
            continue;
        }
        for (i, existing) in rows.iter().enumerate() {
            if i == updated_idx || existing.deleted {
                continue;
            }
            let existing_val = &existing.values[col_idx];
            if !existing_val.is_null()
                && compare_values(new_val, existing_val) == std::cmp::Ordering::Equal
            {
                return Err(DbError::Execution(format!(
                    "Violation of UNIQUE KEY constraint on column '{}'. Cannot insert duplicate key.",
                    col.name
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn apply_assignments(
    table: &TableDef,
    row: &mut StoredRow,
    assignments: &[Assignment],
    joined: &super::super::model::JoinedRow,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<(), DbError> {
    for assignment in assignments {
        let idx = table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&assignment.column))
            .ok_or_else(|| {
                DbError::Semantic(format!("column '{}' not found", assignment.column))
            })?;
        let target = &table.columns[idx].data_type;
        if table.columns[idx].computed_expr.is_some() {
            return Err(DbError::Execution(format!(
                "cannot update computed column '{}'",
                table.columns[idx].name
            )));
        }
        let value = eval_expr_to_type_in_context(
            &assignment.expr,
            target,
            joined,
            ctx,
            catalog,
            storage,
            clock,
        )?;
        enforce_string_length(target, &value, &table.columns[idx].name)?;
        row.values[idx] = value;
    }

    for (idx, col) in table.columns.iter().enumerate() {
        if let Some(computed) = &col.computed_expr {
            let snapshot = row.clone();
            let joined = single_row_context(table, snapshot);
            let value =
                super::super::evaluator::eval_expr(computed, &joined, ctx, catalog, storage, clock)?;
            row.values[idx] = value;
        }
    }
    Ok(())
}

pub(crate) fn validate_row_against_table(
    table: &TableDef,
    values: &[Value],
) -> Result<(), DbError> {
    for (col, value) in table.columns.iter().zip(values.iter()) {
        if !col.nullable && value.is_null() {
            return Err(DbError::Execution(format!(
                "column '{}' does not allow NULL",
                col.name
            )));
        }
    }
    Ok(())
}

pub(crate) fn enforce_checks_on_row(
    table: &TableDef,
    row: &StoredRow,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<(), DbError> {
    let joined = single_row_context(table, row.clone());
    for col in &table.columns {
        if let Some(check_expr) = &col.check {
            let check_val =
                super::super::evaluator::eval_expr(check_expr, &joined, ctx, catalog, storage, clock)?;
            if !check_val.is_null() && !super::super::value_ops::truthy(&check_val) {
                let cname = col
                    .check_constraint_name
                    .as_deref()
                    .unwrap_or("unnamed_check");
                return Err(DbError::Execution(format!(
                    "CHECK constraint '{}' violated",
                    cname
                )));
            }
        }
    }

    for chk in &table.check_constraints {
        let check_val =
            super::super::evaluator::eval_expr(&chk.expr, &joined, ctx, catalog, storage, clock)?;
        if !check_val.is_null() && !super::super::value_ops::truthy(&check_val) {
            return Err(DbError::Execution(format!(
                "CHECK constraint '{}' violated",
                chk.name
            )));
        }
    }

    Ok(())
}
