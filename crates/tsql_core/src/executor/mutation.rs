use crate::ast::{Assignment, DeleteStmt, Expr, InsertStmt, UpdateStmt};
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::storage::{InMemoryStorage, StoredRow};
use crate::types::{DataType, Value};

use super::clock::Clock;
use super::evaluator::{eval_expr_to_type_constant, eval_expr_to_type_in_context, eval_predicate};
use super::model::single_row_context;
use super::value_ops::compare_values;

pub(crate) struct MutationExecutor<'a> {
    pub(crate) catalog: &'a mut Catalog,
    pub(crate) storage: &'a mut InMemoryStorage,
    pub(crate) clock: &'a dyn Clock,
}

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_insert(&mut self, stmt: InsertStmt) -> Result<(), DbError> {
        let schema = stmt.table.schema_or_dbo().to_string();
        let table_name = stmt.table.name.clone();
        let schema_id = self
            .catalog
            .get_schema_id(&schema)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;

        let table_pos = self
            .catalog
            .tables
            .iter()
            .position(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(&table_name))
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?;

        let table_id = self.catalog.tables[table_pos].id;

        if stmt.default_values {
            let row = self.build_insert_row(table_pos, &[], vec![])?;
            self.storage
                .tables
                .get_mut(&table_id)
                .ok_or_else(|| DbError::Storage("table storage not found".to_string()))?
                .push(row);
            return Ok(());
        }

        let insert_columns = if let Some(cols) = stmt.columns.clone() {
            cols
        } else {
            self.catalog.tables[table_pos]
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        };

        for value_row in stmt.values {
            let row = self.build_insert_row(table_pos, &insert_columns, value_row)?;

            let table = &self.catalog.tables[table_pos];
            enforce_unique_on_insert(table, self.storage, table_id, &row)?;

            self.storage
                .tables
                .get_mut(&table_id)
                .ok_or_else(|| DbError::Storage("table storage not found".to_string()))?
                .push(row);
        }

        Ok(())
    }

    pub(crate) fn execute_update(&mut self, stmt: UpdateStmt) -> Result<(), DbError> {
        let schema = stmt.table.schema_or_dbo().to_string();
        let table_name = stmt.table.name.clone();
        let schema_id = self
            .catalog
            .get_schema_id(&schema)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;

        let table_pos = self
            .catalog
            .tables
            .iter()
            .position(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(&table_name))
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?;

        let table_snapshot = self.catalog.tables[table_pos].clone();
        let table_id = table_snapshot.id;
        let rows = self
            .storage
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| DbError::Storage("table storage not found".to_string()))?;

        for row in rows.iter_mut().filter(|r| !r.deleted) {
            let joined = single_row_context(&table_snapshot, row.clone());
            let matches = if let Some(selection) = &stmt.selection {
                eval_predicate(selection, &joined, self.clock)?
            } else {
                true
            };

            if !matches {
                continue;
            }

            apply_assignments(&table_snapshot, row, &stmt.assignments, self.clock)?;
            validate_row_against_table(&table_snapshot, &row.values)?;
        }

        // Re-check unique constraints after update
        let rows = self
            .storage
            .tables
            .get(&table_id)
            .ok_or_else(|| DbError::Storage("table storage not found".to_string()))?;
        for (col_idx, col) in table_snapshot
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.unique)
        {
            let mut seen: Vec<Value> = Vec::new();
            for row in rows.iter().filter(|r| !r.deleted) {
                let val = &row.values[col_idx];
                if val.is_null() {
                    continue;
                }
                if seen
                    .iter()
                    .any(|s| compare_values(s, val) == std::cmp::Ordering::Equal)
                {
                    return Err(DbError::Execution(format!(
                        "Violation of UNIQUE KEY constraint on column '{}'. Cannot insert duplicate key.",
                        col.name
                    )));
                }
                seen.push(val.clone());
            }
        }

        Ok(())
    }

    pub(crate) fn execute_delete(&mut self, stmt: DeleteStmt) -> Result<(), DbError> {
        let schema = stmt.table.schema_or_dbo().to_string();
        let table_name = stmt.table.name.clone();
        let schema_id = self
            .catalog
            .get_schema_id(&schema)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;

        let table = self
            .catalog
            .tables
            .iter()
            .find(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(&table_name))
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?
            .clone();

        let rows = self
            .storage
            .tables
            .get_mut(&table.id)
            .ok_or_else(|| DbError::Storage("table storage not found".to_string()))?;

        for row in rows.iter_mut().filter(|r| !r.deleted) {
            let joined = single_row_context(&table, row.clone());
            let matches = if let Some(selection) = &stmt.selection {
                eval_predicate(selection, &joined, self.clock)?
            } else {
                true
            };
            if matches {
                row.deleted = true;
            }
        }

        Ok(())
    }

    fn build_insert_row(
        &mut self,
        table_pos: usize,
        insert_columns: &[String],
        values: Vec<Expr>,
    ) -> Result<StoredRow, DbError> {
        if insert_columns.len() != values.len() {
            return Err(DbError::Execution(
                "insert column count does not match values count".to_string(),
            ));
        }

        let table = self
            .catalog
            .tables
            .get_mut(table_pos)
            .ok_or_else(|| DbError::Execution("table not found".to_string()))?;

        let mut final_values = vec![Value::Null; table.columns.len()];

        for (input_col, expr) in insert_columns.iter().zip(values.iter()) {
            let col_idx = table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(input_col))
                .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", input_col)))?;

            let col = &table.columns[col_idx];
            let value = eval_expr_to_type_constant(expr, &col.data_type, self.clock)?;
            enforce_string_length(&col.data_type, &value, &col.name)?;
            final_values[col_idx] = value;
        }

        apply_missing_values(table, &mut final_values, self.clock)?;

        Ok(StoredRow {
            values: final_values,
            deleted: false,
        })
    }
}

fn enforce_string_length(
    data_type: &DataType,
    value: &Value,
    col_name: &str,
) -> Result<(), DbError> {
    let max_len = match data_type {
        DataType::Char { len } | DataType::NChar { len } => Some(*len as usize),
        DataType::VarChar { max_len } | DataType::NVarChar { max_len } => Some(*max_len as usize),
        _ => None,
    };

    if let Some(max) = max_len {
        let s = match value {
            Value::Char(s) | Value::VarChar(s) | Value::NChar(s) | Value::NVarChar(s) => {
                Some(s.len())
            }
            _ => None,
        };
        if let Some(actual_len) = s {
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

fn enforce_unique_on_insert(
    table: &TableDef,
    storage: &InMemoryStorage,
    table_id: u32,
    new_row: &StoredRow,
) -> Result<(), DbError> {
    let rows = storage
        .tables
        .get(&table_id)
        .ok_or_else(|| DbError::Storage("table storage not found".to_string()))?;

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

fn apply_missing_values(
    table: &mut TableDef,
    final_values: &mut [Value],
    clock: &dyn Clock,
) -> Result<(), DbError> {
    for (idx, col) in table.columns.iter_mut().enumerate() {
        if matches!(final_values[idx], Value::Null) {
            if let Some(identity) = &mut col.identity {
                final_values[idx] = match &col.data_type {
                    DataType::TinyInt => Value::TinyInt(identity.next_value() as u8),
                    DataType::SmallInt => Value::SmallInt(identity.next_value() as i16),
                    DataType::Int => Value::Int(identity.next_value() as i32),
                    DataType::BigInt => Value::BigInt(identity.next_value()),
                    _ => {
                        return Err(DbError::Execution(format!(
                            "identity not supported for column type {:?}",
                            col.data_type
                        )))
                    }
                };
                continue;
            }

            if let Some(default_expr) = &col.default {
                final_values[idx] =
                    eval_expr_to_type_constant(default_expr, &col.data_type, clock)?;
                continue;
            }

            if !col.nullable {
                return Err(DbError::Execution(format!(
                    "column '{}' does not allow NULL",
                    col.name
                )));
            }
        }
    }
    Ok(())
}

fn apply_assignments(
    table: &TableDef,
    row: &mut StoredRow,
    assignments: &[Assignment],
    clock: &dyn Clock,
) -> Result<(), DbError> {
    let snapshot = row.clone();
    let joined = single_row_context(table, snapshot);
    for assignment in assignments {
        let idx = table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&assignment.column))
            .ok_or_else(|| {
                DbError::Semantic(format!("column '{}' not found", assignment.column))
            })?;
        let target = &table.columns[idx].data_type;
        let value = eval_expr_to_type_in_context(&assignment.expr, target, &joined, clock)?;
        enforce_string_length(target, &value, &table.columns[idx].name)?;
        row.values[idx] = value;
    }
    Ok(())
}

fn validate_row_against_table(table: &TableDef, values: &[Value]) -> Result<(), DbError> {
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
