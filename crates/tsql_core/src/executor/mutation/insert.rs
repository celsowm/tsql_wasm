use crate::ast::InsertStmt;
use crate::catalog::{Catalog, TableDef};
use crate::error::DbError;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr_to_type_constant;
use super::super::model::single_row_context;
use super::super::result::QueryResult;

use super::MutationExecutor;
use super::output::build_output_result;
use super::validation::{
    enforce_checks_on_row, enforce_foreign_keys_on_insert, enforce_string_length,
    enforce_unique_on_insert,
};

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_insert_with_context(
        &mut self,
        mut stmt: InsertStmt,
        ctx: &mut ExecutionContext,
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
            .ok_or_else(|| {
                DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
            })?
            .clone();

        let table_id = table.id;

        // Check for INSTEAD OF INSERT trigger
        let instead_of_triggers = self.find_triggers(&table, crate::ast::TriggerEvent::Insert)
            .into_iter()
            .filter(|t| t.is_instead_of)
            .collect::<Vec<_>>();

        if !instead_of_triggers.is_empty() {
            let mut inserted_rows = Vec::new();
            if stmt.default_values {
                let row = self.build_insert_row(&table, &[], vec![], ctx)?;
                inserted_rows.push(row);
            } else if let Some(select_stmt) = stmt.select_source {
                let query_result = super::super::query::QueryExecutor {
                    catalog: self.catalog as &dyn Catalog,
                    storage: self.storage,
                    clock: self.clock,
                }
                .execute_select(*select_stmt, ctx)?;

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
                        final_values[col_idx] = val.clone();
                    }
                    let mut temp_row = crate::storage::StoredRow {
                        values: final_values,
                        deleted: false,
                    };
                    self.apply_missing_values(&table, &mut temp_row.values, ctx)?;
                    inserted_rows.push(temp_row);
                }
            } else {
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
                    inserted_rows.push(row);
                }
            }

            self.execute_triggers(&table, crate::ast::TriggerEvent::Insert, true, &inserted_rows, &[], ctx)?;

            if let Some(output) = stmt.output {
                let inserted: Vec<&crate::storage::StoredRow> = inserted_rows.iter().collect();
                return build_output_result(&output, &table, &inserted, &[]);
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

        if stmt.default_values {
            let row = self.build_insert_row(&table, &[], vec![], ctx)?;
            self.storage.insert_row(table_id, row.clone())?;
            if collect_rows {
                inserted_rows_for_output.push(row);
            }
        } else if let Some(select_stmt) = stmt.select_source {
            let query_result = super::super::query::QueryExecutor {
                catalog: self.catalog as &dyn Catalog,
                storage: self.storage,
                clock: self.clock,
            }
            .execute_select(*select_stmt, ctx)?;

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

                let mut temp_row = crate::storage::StoredRow {
                    values: final_values.clone(),
                    deleted: false,
                };
                self.apply_missing_values(&table, &mut temp_row.values, ctx)?;
                enforce_unique_on_insert(&table, self.storage, table_id, &temp_row)?;
                enforce_foreign_keys_on_insert(&table, self.catalog, self.storage, &temp_row)?;
                enforce_checks_on_row(
                    &table,
                    &temp_row,
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                self.storage.insert_row(table_id, temp_row.clone())?;
                if collect_rows {
                    inserted_rows_for_output.push(temp_row);
                }
            }
        } else {
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
                enforce_foreign_keys_on_insert(&table, self.catalog, self.storage, &row)?;
                enforce_checks_on_row(&table, &row, ctx, self.catalog, self.storage, self.clock)?;
                self.storage.insert_row(table_id, row.clone())?;
                if collect_rows {
                    inserted_rows_for_output.push(row);
                }
            }
        }

        self.execute_triggers(&table, crate::ast::TriggerEvent::Insert, false, &inserted_rows_for_output, &[], ctx)?;

        if let Some(output) = stmt.output {
            let inserted: Vec<&crate::storage::StoredRow> = inserted_rows_for_output.iter().collect();
            return build_output_result(&output, &table, &inserted, &[]);
        }

        Ok(None)
    }

    pub(crate) fn build_insert_row(
        &mut self,
        table: &TableDef,
        insert_columns: &[String],
        values: Vec<crate::ast::Expr>,
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

    pub(crate) fn apply_missing_values(
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
        Ok(())
    }
}
