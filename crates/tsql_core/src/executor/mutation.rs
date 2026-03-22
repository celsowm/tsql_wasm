use crate::ast::{Assignment, DeleteStmt, Expr, InsertStmt, UpdateStmt};
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};
use crate::types::{DataType, Value};

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::{eval_expr_to_type_constant, eval_expr_to_type_in_context, eval_predicate};
use super::model::single_row_context;
use super::value_ops::compare_values;

pub(crate) struct MutationExecutor<'a> {
    pub(crate) catalog: &'a mut dyn Catalog,
    pub(crate) storage: &'a mut dyn Storage,
    pub(crate) clock: &'a dyn Clock,
}

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_insert_with_context(
        &mut self,
        mut stmt: InsertStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<(), DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let schema = stmt.table.schema_or_dbo().to_string();
        let table_name = stmt.table.name.clone();

        let table = self
            .catalog
            .find_table(&schema, &table_name)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?
            .clone();

        let table_id = table.id;

        if stmt.default_values {
            let row = self.build_insert_row(&table, &[], vec![], ctx)?;
            self.storage.insert_row(table_id, row)?;
            return Ok(());
        }

        let insert_columns = if let Some(cols) = stmt.columns.clone() {
            cols
        } else {
            table
                .columns
                .iter()
                .filter(|c| c.computed_expr.is_none())
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        };

        for value_row in stmt.values {
            let row = self.build_insert_row(&table, &insert_columns, value_row, ctx)?;
            enforce_unique_on_insert(&table, self.storage, table_id, &row)?;
            enforce_checks_on_row(&table, &row, ctx, self.catalog, self.storage, self.clock)?;
            self.storage.insert_row(table_id, row)?;
        }

        Ok(())
    }

    pub(crate) fn execute_update_with_context(
        &mut self,
        mut stmt: UpdateStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<(), DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let schema = stmt.table.schema_or_dbo().to_string();
        let table_name = stmt.table.name.clone();

        let table = self
            .catalog
            .find_table(&schema, &table_name)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?
            .clone();

        let table_id = table.id;
        let mut rows = self.storage.get_rows(table_id)?;

        let mut updated_indices = Vec::new();
        for i in 0..rows.len() {
            if rows[i].deleted {
                continue;
            }
            let joined = single_row_context(&table, rows[i].clone());
            let matches = if let Some(selection) = &stmt.selection {
                eval_predicate(
                    selection,
                    &joined,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?
            } else {
                true
            };

            if matches {
                apply_assignments(
                    &table,
                    &mut rows[i],
                    &stmt.assignments,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                validate_row_against_table(&table, &rows[i].values)?;
                enforce_checks_on_row(
                    &table,
                    &rows[i],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                updated_indices.push(i);
            }
        }

        // Re-check unique constraints for updated rows
        for &idx in &updated_indices {
            enforce_unique_on_update(&table, self.storage, table_id, &rows[idx], idx)?;
        }

        self.storage.update_rows(table_id, rows)?;
        Ok(())
    }

    pub(crate) fn execute_delete_with_context(
        &mut self,
        mut stmt: DeleteStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<(), DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let schema = stmt.table.schema_or_dbo().to_string();
        let table_name = stmt.table.name.clone();

        let table = self
            .catalog
            .find_table(&schema, &table_name)
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?
            .clone();

        let table_id = table.id;
        let mut rows = self.storage.get_rows(table_id)?;

        for row in rows.iter_mut().filter(|r| !r.deleted) {
            let joined = single_row_context(&table, row.clone());
            let matches = if let Some(selection) = &stmt.selection {
                eval_predicate(
                    selection,
                    &joined,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?
            } else {
                true
            };
            if matches {
                row.deleted = true;
            }
        }

        self.storage.update_rows(table_id, rows)?;
        Ok(())
    }

    fn build_insert_row(
        &mut self,
        table: &TableDef,
        insert_columns: &[String],
        values: Vec<Expr>,
        ctx: &mut ExecutionContext,
    ) -> Result<StoredRow, DbError> {
        if insert_columns.len() != values.len() {
            return Err(DbError::Execution(
                "insert column count does not match values count".to_string(),
            ));
        }

        let mut final_values = vec![Value::Null; table.columns.len()];

        for (input_col, expr) in insert_columns.iter().zip(values.iter()) {
            let col_idx = table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(input_col))
                .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", input_col)))?;

            let col = &table.columns[col_idx];
            if col.computed_expr.is_some() {
                return Err(DbError::Execution(format!(
                    "cannot insert explicit value for computed column '{}'",
                    col.name
                )));
            }
            let value = eval_expr_to_type_constant(
                expr,
                &col.data_type,
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
            enforce_string_length(&col.data_type, &value, &col.name)?;
            final_values[col_idx] = value;
        }

        self.apply_missing_values(table, &mut final_values, ctx)?;

        Ok(StoredRow {
            values: final_values,
            deleted: false,
        })
    }

    fn apply_missing_values(
        &mut self,
        table: &TableDef,
        final_values: &mut [Value],
        ctx: &mut ExecutionContext,
    ) -> Result<(), DbError> {
        for (idx, col) in table.columns.iter().enumerate() {
            if col.computed_expr.is_some() {
                continue;
            }
            if matches!(final_values[idx], Value::Null) {
                if col.identity.is_some() {
                    let next_val = self.catalog.next_identity_value(table.id, &col.name)?;
                    ctx.set_last_identity(next_val);
                    final_values[idx] = match &col.data_type {
                        DataType::TinyInt => Value::TinyInt(next_val as u8),
                        DataType::SmallInt => Value::SmallInt(next_val as i16),
                        DataType::Int => Value::Int(next_val as i32),
                        DataType::BigInt => Value::BigInt(next_val),
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
                    final_values[idx] = eval_expr_to_type_constant(
                        default_expr,
                        &col.data_type,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?;
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

        // Computed columns are materialized after base/default/identity values are known.
        for (idx, col) in table.columns.iter().enumerate() {
            if let Some(computed) = &col.computed_expr {
                let snapshot = StoredRow {
                    values: final_values.to_vec(),
                    deleted: false,
                };
                let joined = single_row_context(table, snapshot);
                let value = super::evaluator::eval_expr(
                    computed,
                    &joined,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                final_values[idx] = value;
            }
        }
        Ok(())
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

fn enforce_unique_on_update(
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

fn apply_assignments(
    table: &TableDef,
    row: &mut StoredRow,
    assignments: &[Assignment],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
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
        if table.columns[idx].computed_expr.is_some() {
            return Err(DbError::Execution(format!(
                "cannot update computed column '{}'",
                table.columns[idx].name
            )));
        }
        let value = eval_expr_to_type_in_context(
            &assignment.expr,
            target,
            &joined,
            ctx,
            catalog,
            storage,
            clock,
        )?;
        enforce_string_length(target, &value, &table.columns[idx].name)?;
        row.values[idx] = value;
    }

    // Recompute computed columns after base assignments.
    for (idx, col) in table.columns.iter().enumerate() {
        if let Some(computed) = &col.computed_expr {
            let snapshot = row.clone();
            let joined = single_row_context(table, snapshot);
            let value =
                super::evaluator::eval_expr(computed, &joined, ctx, catalog, storage, clock)?;
            row.values[idx] = value;
        }
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

fn enforce_checks_on_row(
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
                super::evaluator::eval_expr(check_expr, &joined, ctx, catalog, storage, clock)?;
            if !check_val.is_null() && !super::value_ops::truthy(&check_val) {
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
            super::evaluator::eval_expr(&chk.expr, &joined, ctx, catalog, storage, clock)?;
        if !check_val.is_null() && !super::value_ops::truthy(&check_val) {
            return Err(DbError::Execution(format!(
                "CHECK constraint '{}' violated",
                chk.name
            )));
        }
    }

    Ok(())
}
