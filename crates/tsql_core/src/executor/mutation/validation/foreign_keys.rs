use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};
use crate::types::Value;

use super::super::super::value_ops::compare_values;

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
                let mut rows_to_update: Vec<(usize, StoredRow)> = Vec::new();

                for (idx, other_row) in storage.scan_rows(other_table.id)?.enumerate() {
                    let other_row = other_row?;
                    if other_row.deleted {
                        continue;
                    }
                    let mut matches = true;
                    let mut all_null = true;
                    for (i, ref_col_name) in fk.referenced_columns.iter().enumerate() {
                        let ref_col_idx = table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(ref_col_name))
                            .ok_or_else(|| DbError::column_not_found(ref_col_name))?;

                        let col_name = &fk.columns[i];
                        let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                            .ok_or_else(|| DbError::column_not_found(col_name))?;

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
                                        .ok_or_else(|| DbError::column_not_found(col_name))?;
                                    new_row.values[col_idx] = Value::Null;
                                }
                                rows_to_update.push((idx, new_row));
                            }
                            crate::ast::ReferentialAction::SetDefault => {
                                let mut new_row = other_row.clone();
                                for (_i, col_name) in fk.columns.iter().enumerate() {
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::column_not_found(col_name))?;
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
                let mut rows_to_update: Vec<(usize, StoredRow)> = Vec::new();

                for (idx, other_row) in storage.scan_rows(other_table.id)?.enumerate() {
                    let other_row = other_row?;
                    if other_row.deleted {
                        continue;
                    }
                    let mut matches_old = true;
                    let mut matches_new = true;
                    let mut all_null_old = true;

                    for (i, ref_col_name) in fk.referenced_columns.iter().enumerate() {
                        let ref_col_idx = table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(ref_col_name))
                            .ok_or_else(|| DbError::column_not_found(ref_col_name))?;

                        let col_name = &fk.columns[i];
                        let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                            .ok_or_else(|| DbError::column_not_found(col_name))?;

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
                                        .ok_or_else(|| DbError::column_not_found(ref_col_name))?;
                                    let col_name = &fk.columns[i];
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::column_not_found(col_name))?;
                                    updated_row.values[col_idx] = new_row.values[ref_col_idx].clone();
                                }
                                rows_to_update.push((idx, updated_row));
                            }
                            crate::ast::ReferentialAction::SetNull => {
                                let mut updated_row = other_row.clone();
                                for (_i, col_name) in fk.columns.iter().enumerate() {
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::column_not_found(col_name))?;
                                    updated_row.values[col_idx] = Value::Null;
                                }
                                rows_to_update.push((idx, updated_row));
                            }
                            crate::ast::ReferentialAction::SetDefault => {
                                let mut updated_row = other_row.clone();
                                for (_i, col_name) in fk.columns.iter().enumerate() {
                                    let col_idx = other_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(col_name))
                                        .ok_or_else(|| DbError::column_not_found(col_name))?;
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
                .ok_or_else(|| DbError::column_not_found(col_name))?;
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

        let mut found = false;

        for ref_row in storage.scan_rows(ref_table.id)? {
            let ref_row = ref_row?;
            if ref_row.deleted {
                continue;
            }
            let mut matches = true;
            for (i, ref_col_name) in fk.referenced_columns.iter().enumerate() {
                let ref_col_idx = ref_table.columns.iter().position(|c| c.name.eq_ignore_ascii_case(ref_col_name))
                    .ok_or_else(|| DbError::column_not_found(ref_col_name))?;

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
