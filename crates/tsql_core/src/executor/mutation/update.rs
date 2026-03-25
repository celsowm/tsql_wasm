use std::collections::HashSet;

use crate::ast::{SelectItem, SelectStmt, UpdateStmt};
use crate::error::DbError;

use super::super::context::ExecutionContext;
use super::super::query::QueryExecutor;
use super::super::result::QueryResult;

use super::MutationExecutor;
use super::output::build_output_result;
use super::validation::{
    apply_assignments, enforce_checks_on_row, enforce_foreign_keys_on_delete,
    enforce_foreign_keys_on_insert, enforce_unique_on_update, validate_row_against_table,
};

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_update_with_context(
        &mut self,
        mut stmt: UpdateStmt,
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

        let query_stmt = SelectStmt {
            from: stmt.from.as_ref().and_then(|f| f.tables.get(0).cloned()).or_else(|| {
                Some(crate::ast::TableRef {
                    name: stmt.table.clone(),
                    alias: None,
                })
            }),
            joins: stmt.from.as_ref().map(|f| f.joins.clone()).unwrap_or_default(),
            applies: vec![],
            projection: vec![SelectItem {
                expr: crate::ast::Expr::Wildcard,
                alias: None,
            }],
            distinct: false,
            top: None,
            selection: stmt.selection.clone(),
            group_by: vec![],
            having: None,
            order_by: vec![],
            offset: None,
            fetch: None,
        };

        let query_executor = QueryExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };

        let joined_rows = query_executor.execute_to_joined_rows(query_stmt, ctx)?;

        let mut updated_indices = HashSet::new();
        let mut inserted_rows_for_output = Vec::new();
        let mut deleted_rows_for_output = Vec::new();

        for joined_row in joined_rows {
            let target_ctx = joined_row
                .iter()
                .find(|ct| ct.table.id == table_id)
                .ok_or_else(|| DbError::Execution("target table not found in join context".into()))?;

            if let (Some(stored_row), Some(idx)) = (&target_ctx.row, target_ctx.storage_index) {
                if !updated_indices.contains(&idx) {
                    let mut new_row = stored_row.clone();
                    enforce_foreign_keys_on_delete(&table, self.catalog, self.storage, stored_row)?;
                    apply_assignments(
                        &table,
                        &mut new_row,
                        &stmt.assignments,
                        &joined_row,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?;
                    validate_row_against_table(&table, &new_row.values)?;
                    enforce_foreign_keys_on_insert(&table, self.catalog, self.storage, &new_row)?;
                    enforce_checks_on_row(
                        &table,
                        &new_row,
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?;
                    enforce_unique_on_update(&table, self.storage, table_id, &new_row, idx)?;

                    self.storage.update_row(table_id, idx, new_row.clone())?;
                    updated_indices.insert(idx);

                    if stmt.output.is_some() {
                        inserted_rows_for_output.push(new_row);
                        deleted_rows_for_output.push(stored_row.clone());
                    }
                }
            }
        }

        if let Some(output) = stmt.output {
            let inserted: Vec<&crate::storage::StoredRow> = inserted_rows_for_output.iter().collect();
            let deleted: Vec<&crate::storage::StoredRow> = deleted_rows_for_output.iter().collect();
            return build_output_result(&output, &table, &inserted, &deleted);
        }

        Ok(None)
    }
}
