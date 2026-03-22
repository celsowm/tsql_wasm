use crate::ast::{Assignment, DeleteStmt, Expr, FromClause, InsertStmt, JoinType, OutputColumn, OutputSource, UpdateStmt};
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};
use crate::types::{DataType, Value};

use super::clock::Clock;
use super::context::ExecutionContext;
use super::evaluator::{eval_expr_to_type_constant, eval_expr_to_type_in_context, eval_predicate};
use super::model::single_row_context;
use super::result::QueryResult;
use super::value_ops::compare_values;

fn build_output_columns(
    output: &[OutputColumn],
    _table: &TableDef,
) -> Result<Vec<String>, DbError> {
    let mut columns = Vec::new();
    for col in output {
        let alias = col.alias.clone().unwrap_or_else(|| col.column.clone());
        columns.push(alias);
    }
    Ok(columns)
}

fn extract_output_value(
    output_col: &OutputColumn,
    table: &TableDef,
    row: &StoredRow,
) -> Result<Value, DbError> {
    let col_idx = table
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(&output_col.column))
        .ok_or_else(|| DbError::Semantic(format!("column '{}' not found", output_col.column)))?;
    Ok(row.values[col_idx].clone())
}

pub fn build_output_result(
    output: &[OutputColumn],
    table: &TableDef,
    inserted_rows: &[&StoredRow],
    deleted_rows: &[&StoredRow],
) -> Result<Option<QueryResult>, DbError> {
    if output.is_empty() {
        return Ok(None);
    }

    let columns = build_output_columns(output, table)?;
    let mut rows = Vec::new();

    for (inserted, deleted) in inserted_rows.iter().zip(deleted_rows.iter()) {
        let mut row = Vec::new();
        for col in output {
            let val = match col.source {
                OutputSource::Inserted => extract_output_value(col, table, inserted)?,
                OutputSource::Deleted => extract_output_value(col, table, deleted)?,
            };
            row.push(val);
        }
        rows.push(row);
    }

    // For INSERT, there are no deleted rows - just use inserted for both
    if deleted_rows.is_empty() && !inserted_rows.is_empty() {
        rows.clear();
        for inserted in inserted_rows {
            let mut row = Vec::new();
            for col in output {
                let val = match col.source {
                    OutputSource::Inserted => extract_output_value(col, table, inserted)?,
                    OutputSource::Deleted => Value::Null,
                };
                row.push(val);
            }
            rows.push(row);
        }
    }

    // For DELETE, there are no inserted rows - just use deleted for both
    if inserted_rows.is_empty() && !deleted_rows.is_empty() {
        rows.clear();
        for deleted in deleted_rows {
            let mut row = Vec::new();
            for col in output {
                let val = match col.source {
                    OutputSource::Inserted => Value::Null,
                    OutputSource::Deleted => extract_output_value(col, table, deleted)?,
                };
                row.push(val);
            }
            rows.push(row);
        }
    }

    Ok(Some(QueryResult { columns, rows }))
}

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

        // INSERT ... SELECT
        if let Some(select_stmt) = stmt.select_source {
            let query_result = super::query::QueryExecutor {
                catalog: self.catalog as &dyn Catalog,
                storage: self.storage as &dyn Storage,
                clock: self.clock,
            }
            .execute_select(*select_stmt, ctx)?;

            // Validate column count matches
            if insert_columns.len() != query_result.columns.len() {
                return Err(DbError::Execution(format!(
                    "insert column count ({}) does not match select column count ({})",
                    insert_columns.len(),
                    query_result.columns.len()
                )));
            }

            for row_values in query_result.rows {
                let mut final_values = vec![crate::types::Value::Null; table.columns.len()];

                for (input_col, val) in insert_columns.iter().zip(row_values.iter()) {
                    let col_idx = table
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(input_col))
                        .ok_or_else(|| {
                            DbError::Semantic(format!("column '{}' not found", input_col))
                        })?;

                    let col = &table.columns[col_idx];
                    if col.computed_expr.is_some() {
                        return Err(DbError::Execution(format!(
                            "cannot insert explicit value for computed column '{}'",
                            col.name
                        )));
                    }
                    enforce_string_length(&col.data_type, val, &col.name)?;
                    final_values[col_idx] = val.clone();
                }

                // Apply missing values (identity, defaults, nullable)
                let mut temp_row = crate::storage::StoredRow {
                    values: final_values.clone(),
                    deleted: false,
                };
                self.apply_missing_values(&table, &mut temp_row.values, ctx)?;
                enforce_unique_on_insert(&table, self.storage, table_id, &temp_row)?;
                enforce_checks_on_row(
                    &table,
                    &temp_row,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                self.storage.insert_row(table_id, temp_row)?;
            }
            return Ok(());
        }

        // INSERT ... VALUES
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

        if let Some(from_clause) = &stmt.from {
            // UPDATE ... FROM: join target table with from tables
            return self.execute_update_with_from(
                &table, table_id, &mut rows, &stmt, from_clause, ctx,
            );
        }

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
                    &joined,
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

    fn execute_update_with_from(
        &mut self,
        table: &TableDef,
        table_id: u32,
        rows: &mut Vec<crate::storage::StoredRow>,
        stmt: &UpdateStmt,
        from_clause: &FromClause,
        ctx: &mut ExecutionContext,
    ) -> Result<(), DbError> {
        // Build combined context: for each target row, join with from tables
        let mut updated_indices = Vec::new();

        for i in 0..rows.len() {
            if rows[i].deleted {
                continue;
            }

            // Start with the target table row context
            let mut combined_ctx: super::model::JoinedRow =
                single_row_context(table, rows[i].clone());

            // Add from tables' first row context (simplified: uses first non-deleted row)
            let mut all_match = true;
            for from_table_ref in &from_clause.tables {
                let resolved_name = ctx
                    .resolve_table_name(&from_table_ref.name.name)
                    .unwrap_or_else(|| from_table_ref.name.name.clone());
                let from_schema = from_table_ref.name.schema_or_dbo();
                let from_table = self
                    .catalog
                    .find_table(from_schema, &resolved_name)
                    .ok_or_else(|| {
                        DbError::Semantic(format!(
                            "table '{}.{}' not found",
                            from_schema, resolved_name
                        ))
                    })?
                    .clone();
                let from_rows = self.storage.get_rows(from_table.id)?;
                let non_deleted: Vec<_> = from_rows.iter().filter(|r| !r.deleted).collect();
                if non_deleted.is_empty() {
                    all_match = false;
                    break;
                }
                let alias = from_table_ref
                    .alias
                    .clone()
                    .unwrap_or_else(|| resolved_name.clone());
                combined_ctx.push(super::model::ContextTable {
                    table: from_table.clone(),
                    alias: alias.clone(),
                    row: Some(non_deleted[0].clone()),
                });
            }

            if !all_match {
                continue;
            }

            // Apply explicit joins
            for join_clause in &from_clause.joins {
                let resolved_name = ctx
                    .resolve_table_name(&join_clause.table.name.name)
                    .unwrap_or_else(|| join_clause.table.name.name.clone());
                let join_schema = join_clause.table.name.schema_or_dbo();
                let join_table = self
                    .catalog
                    .find_table(join_schema, &resolved_name)
                    .ok_or_else(|| {
                        DbError::Semantic(format!(
                            "table '{}.{}' not found",
                            join_schema, resolved_name
                        ))
                    })?
                    .clone();
                let join_rows = self.storage.get_rows(join_table.id)?;

                let mut found_match = false;
                let alias = join_clause
                    .table
                    .alias
                    .clone()
                    .unwrap_or_else(|| resolved_name.clone());
                for join_row in join_rows.iter().filter(|r| !r.deleted) {
                    let mut test_ctx = combined_ctx.clone();
                    test_ctx.push(super::model::ContextTable {
                        table: join_table.clone(),
                        alias: alias.clone(),
                        row: Some(join_row.clone()),
                    });
                    if let Some(ref on_expr) = join_clause.on {
                        if eval_predicate(
                            on_expr,
                            &test_ctx,
                            ctx,
                            self.catalog,
                            self.storage,
                            self.clock,
                        )? {
                            combined_ctx = test_ctx;
                            found_match = true;
                            break;
                        }
                    }
                }
                match join_clause.join_type {
                    JoinType::Inner => {
                        if !found_match {
                            all_match = false;
                            break;
                        }
                    }
                    JoinType::Left => {} // Keep combined_ctx even without match
                    _ => {} // Simplified: treat right/full as left for now
                }
            }

            if !all_match {
                continue;
            }

            // Apply WHERE filter if present
            let matches = if let Some(selection) = &stmt.selection {
                eval_predicate(
                    selection,
                    &combined_ctx,
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
                    table,
                    &mut rows[i],
                    &stmt.assignments,
                    &combined_ctx,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                validate_row_against_table(table, &rows[i].values)?;
                enforce_checks_on_row(
                    table,
                    &rows[i],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                updated_indices.push(i);
            }
        }

        for &idx in &updated_indices {
            enforce_unique_on_update(table, self.storage, table_id, &rows[idx], idx)?;
        }

        self.storage.update_rows(table_id, rows.to_vec())?;
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

        if let Some(from_clause) = &stmt.from {
            // DELETE ... FROM: join target table with from tables
            return self.execute_delete_with_from(
                &table, table_id, &mut rows, &stmt, from_clause, ctx,
            );
        }

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

    fn execute_delete_with_from(
        &mut self,
        table: &TableDef,
        table_id: u32,
        rows: &mut Vec<StoredRow>,
        stmt: &DeleteStmt,
        from_clause: &FromClause,
        ctx: &mut ExecutionContext,
    ) -> Result<(), DbError> {
        let mut delete_indices = Vec::new();

        for i in 0..rows.len() {
            if rows[i].deleted {
                continue;
            }

            let mut combined_ctx: super::model::JoinedRow =
                single_row_context(table, rows[i].clone());

            let mut all_match = true;
            for from_table_ref in &from_clause.tables {
                let resolved_name = ctx
                    .resolve_table_name(&from_table_ref.name.name)
                    .unwrap_or_else(|| from_table_ref.name.name.clone());
                let from_schema = from_table_ref.name.schema_or_dbo();
                let from_table = self
                    .catalog
                    .find_table(from_schema, &resolved_name)
                    .ok_or_else(|| {
                        DbError::Semantic(format!(
                            "table '{}.{}' not found",
                            from_schema, resolved_name
                        ))
                    })?
                    .clone();
                let from_rows = self.storage.get_rows(from_table.id)?;
                let non_deleted: Vec<_> = from_rows.iter().filter(|r| !r.deleted).collect();
                if non_deleted.is_empty() {
                    all_match = false;
                    break;
                }
                let alias = from_table_ref
                    .alias
                    .clone()
                    .unwrap_or_else(|| resolved_name.clone());
                combined_ctx.push(super::model::ContextTable {
                    table: from_table.clone(),
                    alias: alias.clone(),
                    row: Some(non_deleted[0].clone()),
                });
            }

            if !all_match {
                continue;
            }

            for join_clause in &from_clause.joins {
                let resolved_name = ctx
                    .resolve_table_name(&join_clause.table.name.name)
                    .unwrap_or_else(|| join_clause.table.name.name.clone());
                let join_schema = join_clause.table.name.schema_or_dbo();
                let join_table = self
                    .catalog
                    .find_table(join_schema, &resolved_name)
                    .ok_or_else(|| {
                        DbError::Semantic(format!(
                            "table '{}.{}' not found",
                            join_schema, resolved_name
                        ))
                    })?
                    .clone();
                let join_rows = self.storage.get_rows(join_table.id)?;

                let mut found_match = false;
                let alias = join_clause
                    .table
                    .alias
                    .clone()
                    .unwrap_or_else(|| resolved_name.clone());
                for join_row in join_rows.iter().filter(|r| !r.deleted) {
                    let mut test_ctx = combined_ctx.clone();
                    test_ctx.push(super::model::ContextTable {
                        table: join_table.clone(),
                        alias: alias.clone(),
                        row: Some(join_row.clone()),
                    });
                    if let Some(ref on_expr) = join_clause.on {
                        if eval_predicate(
                            on_expr,
                            &test_ctx,
                            ctx,
                            self.catalog,
                            self.storage,
                            self.clock,
                        )? {
                            combined_ctx = test_ctx;
                            found_match = true;
                            break;
                        }
                    }
                }
                match join_clause.join_type {
                    JoinType::Inner => {
                        if !found_match {
                            all_match = false;
                            break;
                        }
                    }
                    JoinType::Left => {}
                    _ => {}
                }
            }

            if !all_match {
                continue;
            }

            let matches = if let Some(selection) = &stmt.selection {
                eval_predicate(
                    selection,
                    &combined_ctx,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?
            } else {
                true
            };

            if matches {
                delete_indices.push(i);
            }
        }

        for idx in delete_indices {
            rows[idx].deleted = true;
        }

        self.storage.update_rows(table_id, rows.to_vec())?;
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
    joined: &super::model::JoinedRow,
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
