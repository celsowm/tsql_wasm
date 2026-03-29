use crate::ast::Assignment;
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};
use crate::types::{DataType, Value};

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr_to_type_in_context;
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

pub(crate) fn enforce_foreign_keys_on_delete(
    table: &TableDef,
    catalog: &mut dyn Catalog,
    storage: &mut dyn Storage,
    deleted_row: &StoredRow,
) -> Result<(), DbError> {
    for other_table in catalog.get_tables() {
        for fk in &other_table.foreign_keys {
            if fk.referenced_table.schema_or_dbo().eq_ignore_ascii_case(table.schema_or_dbo())
                && fk.referenced_table.name.eq_ignore_ascii_case(&table.name)
            {
                let other_rows = storage.get_rows(other_table.id)?;
                let mut rows_to_update: Vec<(usize, StoredRow)> = Vec::new();
                
                for (idx, other_row) in other_rows.iter().enumerate() {
                    if other_row.deleted {
                        continue;
                    }
                    let mut matches = true;
                    let mut all_null = true;
                    for (i, ref_col_name) in fk.referenced_columns.iter().enumerate() {
                        let ref_col_idx = table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(ref_col_name))
                            .ok_or_else(|| DbError::Semantic(format!("referenced column '{}' not found", ref_col_name)))?;

                        let col_name = &fk.columns[i];
                        let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                            .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))?;

                        if !other_row.values[col_idx].is_null() {
                            all_null = false;
                        }

                        if compare_values(&other_row.values[col_idx], &deleted_row.values[ref_col_idx]) != std::cmp::Ordering::Equal {
                            matches = false;
                            break;
                        }
                    }

                    if matches && !all_null {
                        match fk.on_delete {
                            crate::ast::ReferentialAction::Cascade => {
                                let mut new_row = other_row.clone();
                                new_row.deleted = true;
                                rows_to_update.push((idx, new_row));
                            }
                            crate::ast::ReferentialAction::SetNull => {
                                let mut new_row = other_row.clone();
                                for (_i, col_name) in fk.columns.iter().enumerate() {
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))?;
                                    new_row.values[col_idx] = Value::Null;
                                }
                                rows_to_update.push((idx, new_row));
                            }
                            crate::ast::ReferentialAction::SetDefault => {
                                let mut new_row = other_row.clone();
                                for (_i, col_name) in fk.columns.iter().enumerate() {
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))?;
                                    if let Some(_default) = &other_table.columns[col_idx].default {
                                        new_row.values[col_idx] = Value::Null;
                                    }
                                }
                                rows_to_update.push((idx, new_row));
                            }
                            crate::ast::ReferentialAction::NoAction => {
                                return Err(DbError::Execution(format!(
                                    "The DELETE statement conflicted with the REFERENCE constraint \"{}\". The conflict occurred in database \"master\", table \"{}.{}\", column '{}'.",
                                    fk.name, other_table.schema_or_dbo(), other_table.name, fk.columns[0]
                                )));
                            }
                        }
                    }
                }
                
                for (idx, row) in rows_to_update {
                    storage.update_row(other_table.id, idx, row)?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn enforce_foreign_keys_on_update(
    table: &TableDef,
    catalog: &mut dyn Catalog,
    storage: &mut dyn Storage,
    old_row: &StoredRow,
    new_row: &StoredRow,
) -> Result<(), DbError> {
    for other_table in catalog.get_tables() {
        for fk in &other_table.foreign_keys {
            if fk.referenced_table.schema_or_dbo().eq_ignore_ascii_case(table.schema_or_dbo())
                && fk.referenced_table.name.eq_ignore_ascii_case(&table.name)
            {
                let other_rows = storage.get_rows(other_table.id)?;
                let mut rows_to_update: Vec<(usize, StoredRow)> = Vec::new();
                
                for (idx, other_row) in other_rows.iter().enumerate() {
                    if other_row.deleted {
                        continue;
                    }
                    let mut matches_old = true;
                    let mut matches_new = true;
                    let mut all_null_old = true;
                    
                    for (i, ref_col_name) in fk.referenced_columns.iter().enumerate() {
                        let ref_col_idx = table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(ref_col_name))
                            .ok_or_else(|| DbError::Semantic(format!("referenced column '{}' not found", ref_col_name)))?;

                        let col_name = &fk.columns[i];
                        let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                            .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))?;

                        if !other_row.values[col_idx].is_null() {
                            all_null_old = false;
                        }

                        if compare_values(&other_row.values[col_idx], &old_row.values[ref_col_idx]) != std::cmp::Ordering::Equal {
                            matches_old = false;
                        }
                        if compare_values(&other_row.values[col_idx], &new_row.values[ref_col_idx]) != std::cmp::Ordering::Equal {
                            matches_new = false;
                        }
                    }

                    if matches_old && !matches_new && !all_null_old {
                        match fk.on_update {
                            crate::ast::ReferentialAction::Cascade => {
                                let mut updated_row = other_row.clone();
                                for (i, ref_col_name) in fk.referenced_columns.iter().enumerate() {
                                    let ref_col_idx = table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(ref_col_name))
                                        .ok_or_else(|| DbError::Semantic(format!("referenced column '{}' not found", ref_col_name)))?;
                                    let col_name = &fk.columns[i];
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))?;
                                    updated_row.values[col_idx] = new_row.values[ref_col_idx].clone();
                                }
                                rows_to_update.push((idx, updated_row));
                            }
                            crate::ast::ReferentialAction::SetNull => {
                                let mut updated_row = other_row.clone();
                                for (_i, col_name) in fk.columns.iter().enumerate() {
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))?;
                                    updated_row.values[col_idx] = Value::Null;
                                }
                                rows_to_update.push((idx, updated_row));
                            }
                            crate::ast::ReferentialAction::SetDefault => {
                                let mut updated_row = other_row.clone();
                                for (_i, col_name) in fk.columns.iter().enumerate() {
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))?;
                                    if let Some(_default) = &other_table.columns[col_idx].default {
                                        updated_row.values[col_idx] = Value::Null;
                                    }
                                }
                                rows_to_update.push((idx, updated_row));
                            }
                            crate::ast::ReferentialAction::NoAction => {
                                return Err(DbError::Execution(format!(
                                    "The UPDATE statement conflicted with the REFERENCE constraint \"{}\". The conflict occurred in database \"master\", table \"{}.{}\", column '{}'.",
                                    fk.name, other_table.schema_or_dbo(), other_table.name, fk.columns[0]
                                )));
                            }
                        }
                    }
                }
                
                for (idx, row) in rows_to_update {
                    storage.update_row(other_table.id, idx, row)?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn enforce_foreign_keys_on_insert(
    table: &TableDef,
    catalog: &mut dyn Catalog,
    storage: &mut dyn Storage,
    row: &StoredRow,
) -> Result<(), DbError> {
    for fk in &table.foreign_keys {
        let mut row_values = Vec::new();
        let mut all_null = true;
        for col_name in &fk.columns {
            let col_idx = table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", col_name)))?;
            let val = &row.values[col_idx];
            if !val.is_null() {
                all_null = false;
            }
            row_values.push(val.clone());
        }

        if all_null {
            continue;
        }

        let ref_schema = fk.referenced_table.schema_or_dbo();
        let ref_name = &fk.referenced_table.name;
        let ref_table = catalog.find_table(ref_schema, ref_name)
            .ok_or_else(|| DbError::Execution(format!("referenced table '{}.{}' not found", ref_schema, ref_name)))?;

        let ref_rows = storage.get_rows(ref_table.id)?;
        let mut found = false;

        for ref_row in ref_rows.iter().filter(|r| !r.deleted) {
            let mut matches = true;
            for (i, ref_col_name) in fk.referenced_columns.iter().enumerate() {
                let ref_col_idx = ref_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(ref_col_name))
                    .ok_or_else(|| DbError::Semantic(format!("referenced column '{}' not found", ref_col_name)))?;

                if compare_values(&row_values[i], &ref_row.values[ref_col_idx]) != std::cmp::Ordering::Equal {
                    matches = false;
                    break;
                }
            }
            if matches {
                found = true;
                break;
            }
        }

        if !found {
            return Err(DbError::Execution(format!(
                "The INSERT statement conflicted with the FOREIGN KEY constraint \"{}\". The conflict occurred in database \"master\", table \"{}.{}\", column '{}'.",
                fk.name, ref_schema, ref_name, fk.referenced_columns[0]
            )));
        }
    }
    Ok(())
}

pub(crate) fn enforce_unique_on_insert(
    table: &TableDef,
    storage: &mut dyn Storage,
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
    storage: &mut dyn Storage,
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
    catalog: &mut dyn Catalog,
    storage: &mut dyn Storage,
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
    catalog: &mut dyn Catalog,
    storage: &mut dyn Storage,
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
