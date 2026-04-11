use crate::ast::{InsertSource, InsertStmt};
use crate::catalog::{Catalog, TableDef};
use crate::error::{DbError, StmtOutcome};
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr_to_type_constant;
use super::super::model::single_row_context;
use super::super::query::plan::RelationalQuery;
use super::super::result::QueryResult;
use super::super::string_norm::normalize_identifier;

use super::MutationExecutor;
use super::output::build_output_result;
use super::validation::{
    apply_ansi_padding, enforce_checks_on_row, enforce_foreign_keys_on_insert,
    enforce_string_length, enforce_unique_on_insert,
};

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_insert_with_context(
        &mut self,
        mut stmt: InsertStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
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
            .ok_or_else(|| DbError::table_not_found(&schema, &table_name))?;
        let table = table.clone();

        let table_id = table.id;

        // Check for INSTEAD OF INSERT trigger
        let instead_of_triggers = if ctx.frame.skip_instead_of {
            vec![]
        } else {
            self.find_triggers(&table, crate::ast::TriggerEvent::Insert)
                .into_iter()
                .filter(|t| t.is_instead_of)
                .collect::<Vec<_>>()
        };

        if !instead_of_triggers.is_empty() {
            let rowcount_limit = if ctx.options.rowcount == 0 {
                None
            } else {
                Some(ctx.options.rowcount as usize)
            };
            let mut inserted_count = 0usize;
            let inserted_rows = match &stmt.source {
                InsertSource::DefaultValues => {
                    let row = self.build_insert_row(&table, &[], vec![], ctx)?;
                    vec![row]
                }
                InsertSource::Select(select_stmt) => {
                    let query_result = super::super::query::QueryExecutor {
                        catalog: self.catalog as &dyn Catalog,
                        storage: self.storage,
                        clock: self.clock,
                    }
                    .execute_select(RelationalQuery::from(*select_stmt.clone()), ctx)?;

                    let insert_columns = self.get_insert_columns(&table, &stmt.columns);
                    let mut rows = Vec::new();
                    for row_values in query_result.rows {
                        if let Some(limit) = rowcount_limit {
                            if inserted_count >= limit {
                                break;
                            }
                        }
                        let row = self.build_row_from_values(&table, &insert_columns, row_values, ctx)?;
                        rows.push(row);
                        inserted_count += 1;
                    }
                    rows
                }
                InsertSource::Values(values) => {
                    let insert_columns = self.get_insert_columns(&table, &stmt.columns);
                    let mut rows = Vec::new();
                    for value_row in values {
                        if let Some(limit) = rowcount_limit {
                            if inserted_count >= limit {
                                break;
                            }
                        }
                        let row = self.build_insert_row(&table, &insert_columns, value_row.clone(), ctx)?;
                        rows.push(row);
                        inserted_count += 1;
                    }
                    rows
                }
                InsertSource::Exec(exec_stmt) => {
                    let outcome = super::super::script::ScriptExecutor {
                        catalog: self.catalog,
                        storage: self.storage,
                        clock: self.clock,
                    }
                    .execute(*exec_stmt.clone(), ctx)?;
                    let query_result = match outcome {
                        StmtOutcome::Ok(Some(r)) => r,
                        StmtOutcome::Ok(None) => {
                            return Err(DbError::Execution("INSERT EXEC source returned no result".into()))
                        }
                        other => return other.into_result(),
                    };

                    let insert_columns = self.get_insert_columns(&table, &stmt.columns);
                    let mut rows = Vec::new();
                    for row_values in query_result.rows {
                        if let Some(limit) = rowcount_limit {
                            if inserted_count >= limit {
                                break;
                            }
                        }
                        let row = self.build_row_from_values(&table, &insert_columns, row_values, ctx)?;
                        rows.push(row);
                        inserted_count += 1;
                    }
                    rows
                }
            };

            self.execute_triggers(&table, crate::ast::TriggerEvent::Insert, true, &inserted_rows, &[], ctx)?;

            if let Some(output) = stmt.output {
                let inserted: Vec<&crate::storage::StoredRow> = inserted_rows.iter().collect();
                let result = build_output_result(&output, &table, &inserted, &[])?;
                if let Some(target) = stmt.output_into {
                    if let Some(result) = result.as_ref() {
                        self.insert_output_into(&target, result, ctx)?;
                    } else {
                        return Err(DbError::Execution("OUTPUT INTO produced no result".into()));
                    }
                    return Ok(None);
                }
                return Ok(result);
            }
            return Ok(None);
        }

        let has_after_triggers = !self.find_triggers(&table, crate::ast::TriggerEvent::Insert)
            .into_iter()
            .filter(|t| !t.is_instead_of)
            .collect::<Vec<_>>()
            .is_empty();

        let collect_rows = stmt.output.is_some() || has_after_triggers;
        let mut inserted_rows_for_output = Vec::new();
        let rowcount_limit = if ctx.options.rowcount == 0 {
            None
        } else {
            Some(ctx.options.rowcount as usize)
        };
        let mut inserted_count = 0usize;

        match stmt.source {
            InsertSource::DefaultValues => {
                let row = self.build_insert_row(&table, &[], vec![], ctx)?;
                self.storage.insert_row(table_id, row.clone())?;
                self.push_dirty_insert(ctx, &table.name, &row);
                if collect_rows {
                    inserted_rows_for_output.push(row);
                }
            }
            InsertSource::Select(select_stmt) => {
                let query_result = super::super::query::QueryExecutor {
                    catalog: self.catalog as &dyn Catalog,
                    storage: self.storage,
                    clock: self.clock,
                }
                .execute_select(RelationalQuery::from(*select_stmt), ctx)?;

                let insert_columns = self.get_insert_columns(&table, &stmt.columns);

                if insert_columns.len() != query_result.columns.len() {
                    return Err(DbError::Execution(format!(
                        "insert column count ({}) does not match select column count ({})",
                        insert_columns.len(),
                        query_result.columns.len()
                    )));
                }

                for row_values in query_result.rows {
                    if let Some(limit) = rowcount_limit {
                        if inserted_count >= limit {
                            break;
                        }
                    }
                    let temp_row = self.build_row_from_values(&table, &insert_columns, row_values, ctx)?;
                    enforce_unique_on_insert(&table, self.storage, table_id, &temp_row)?;
                    enforce_foreign_keys_on_insert(&table, self.catalog, self.storage, &temp_row)?;
                    enforce_checks_on_row(&table, &temp_row, ctx, self.catalog, self.storage, self.clock)?;
                    self.storage.insert_row(table_id, temp_row.clone())?;
                    self.push_dirty_insert(ctx, &table.name, &temp_row);
                    inserted_count += 1;
                    if collect_rows {
                        inserted_rows_for_output.push(temp_row);
                    }
                }
            }
            InsertSource::Values(values) => {
                let insert_columns = self.get_insert_columns(&table, &stmt.columns);

                for value_row in values {
                    if let Some(limit) = rowcount_limit {
                        if inserted_count >= limit {
                            break;
                        }
                    }
                    let row = self.build_insert_row(&table, &insert_columns, value_row, ctx)?;
                    enforce_unique_on_insert(&table, self.storage, table_id, &row)?;
                    enforce_foreign_keys_on_insert(&table, self.catalog, self.storage, &row)?;
                    enforce_checks_on_row(&table, &row, ctx, self.catalog, self.storage, self.clock)?;
                    self.storage.insert_row(table_id, row.clone())?;
                    self.push_dirty_insert(ctx, &table.name, &row);
                    inserted_count += 1;
                    if collect_rows {
                        inserted_rows_for_output.push(row);
                    }
                }
            }
            InsertSource::Exec(exec_stmt) => {
                let outcome = super::super::script::ScriptExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                }
                .execute(*exec_stmt, ctx)?;
                let query_result = match outcome {
                    StmtOutcome::Ok(Some(r)) => r,
                    StmtOutcome::Ok(None) => {
                        return Err(DbError::Execution("INSERT EXEC source returned no result".into()))
                    }
                    other => return other.into_result(),
                };

                let insert_columns = self.get_insert_columns(&table, &stmt.columns);
                if insert_columns.len() != query_result.columns.len() {
                    return Err(DbError::Execution(format!(
                        "insert column count ({}) does not match exec column count ({})",
                        insert_columns.len(),
                        query_result.columns.len()
                    )));
                }

                for row_values in query_result.rows {
                    if let Some(limit) = rowcount_limit {
                        if inserted_count >= limit {
                            break;
                        }
                    }
                    let temp_row = self.build_row_from_values(&table, &insert_columns, row_values, ctx)?;
                    enforce_unique_on_insert(&table, self.storage, table_id, &temp_row)?;
                    enforce_foreign_keys_on_insert(&table, self.catalog, self.storage, &temp_row)?;
                    enforce_checks_on_row(&table, &temp_row, ctx, self.catalog, self.storage, self.clock)?;
                    self.storage.insert_row(table_id, temp_row.clone())?;
                    self.push_dirty_insert(ctx, &table.name, &temp_row);
                    inserted_count += 1;
                    if collect_rows {
                        inserted_rows_for_output.push(temp_row);
                    }
                }
            }
        }

        self.execute_triggers(&table, crate::ast::TriggerEvent::Insert, false, &inserted_rows_for_output, &[], ctx)?;

        if let Some(output) = stmt.output {
            let inserted: Vec<&crate::storage::StoredRow> = inserted_rows_for_output.iter().collect();
            let result = build_output_result(&output, &table, &inserted, &[])?;
            if let Some(target) = stmt.output_into {
                if let Some(result) = result.as_ref() {
                    self.insert_output_into(&target, result, ctx)?;
                } else {
                    return Err(DbError::Execution("OUTPUT INTO produced no result".into()));
                }
                return Ok(None);
            }
            return Ok(result);
        }

        Ok(None)
    }

    fn get_insert_columns(&self, table: &TableDef, stmt_columns: &Option<Vec<String>>) -> Vec<String> {
        if let Some(cols) = stmt_columns.clone() {
            cols
        } else {
            table
                .columns
                .iter()
                .filter(|c| c.computed_expr.is_none())
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        }
    }

    fn build_row_from_values(
        &mut self,
        table: &TableDef,
        insert_columns: &[String],
        row_values: Vec<Value>,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<StoredRow, DbError> {
        let mut final_values = vec![crate::types::Value::Null; table.columns.len()];
        for (input_col, val) in insert_columns.iter().zip(row_values.iter()) {
            let col_idx = table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(input_col))
                .ok_or_else(|| DbError::column_not_found(input_col))?;

            let col = &table.columns[col_idx];
            if col.computed_expr.is_some() {
                return Err(DbError::Execution(format!(
                    "cannot insert explicit value for computed column '{}'",
                    col.name
                )));
            }
            let mut value = val.clone();
            apply_ansi_padding(&mut value, &col.data_type, col.ansi_padding_on);
            enforce_string_length(&col.data_type, &value, &col.name)?;
            final_values[col_idx] = value;
        }
        let mut temp_row = crate::storage::StoredRow {
            values: final_values,
            deleted: false,
        };
        self.apply_missing_values(table, &mut temp_row.values, ctx)?;
        for (col, value) in table.columns.iter().zip(temp_row.values.iter_mut()) {
            apply_ansi_padding(value, &col.data_type, col.ansi_padding_on);
            enforce_string_length(&col.data_type, value, &col.name)?;
        }
        Ok(temp_row)
    }

    pub(crate) fn build_insert_row(
        &mut self,
        table: &TableDef,
        insert_columns: &[String],
        values: Vec<crate::ast::Expr>,
        ctx: &mut ExecutionContext<'_>,
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
                .ok_or_else(|| DbError::column_not_found(input_col))?;

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
            let mut value = value;
            apply_ansi_padding(&mut value, &col.data_type, col.ansi_padding_on);
            enforce_string_length(&col.data_type, &value, &col.name)?;
            final_values[col_idx] = value;
        }

        self.apply_missing_values(table, &mut final_values, ctx)?;
        for (col, value) in table.columns.iter().zip(final_values.iter_mut()) {
            apply_ansi_padding(value, &col.data_type, col.ansi_padding_on);
            enforce_string_length(&col.data_type, value, &col.name)?;
        }

        Ok(StoredRow {
            values: final_values,
            deleted: false,
        })
    }

    pub(crate) fn apply_missing_values(
        &mut self,
        table: &TableDef,
        final_values: &mut [Value],
        ctx: &mut ExecutionContext<'_>,
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
            } else if col.identity.is_some() {
                let table_upper = normalize_identifier(&table.name);
                if !ctx.session.identity_insert.contains(&table_upper) {
                    return Err(DbError::Execution(format!(
                        "Cannot insert explicit value for identity column '{}' in table '{}' when IDENTITY_INSERT is set to OFF.",
                        col.name, table.name
                    )));
                }
            }
        }

        for (idx, col) in table.columns.iter().enumerate() {
            if let Some(computed) = &col.computed_expr {
                let snapshot = StoredRow {
                    values: final_values.to_vec(),
                    deleted: false,
                };
                let joined = single_row_context(table, snapshot);
                let value = super::super::evaluator::eval_expr(
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

        for (col, value) in table.columns.iter().zip(final_values.iter_mut()) {
            apply_ansi_padding(value, &col.data_type, col.ansi_padding_on);
        }
        Ok(())
    }
}
