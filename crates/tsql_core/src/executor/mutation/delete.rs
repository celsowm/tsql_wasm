use std::collections::HashSet;

use crate::ast::{DeleteStmt, SelectItem, SelectStmt};
use crate::error::DbError;

use super::super::context::ExecutionContext;
use super::super::query::QueryExecutor;
use super::super::result::QueryResult;

use super::MutationExecutor;
use super::output::build_output_result;
use super::validation::enforce_foreign_keys_on_delete;

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_delete_with_context(
        &mut self,
        mut stmt: DeleteStmt,
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

        let mut deleted_indices = HashSet::new();
        let mut deleted_rows_for_output = Vec::new();

        for joined_row in joined_rows {
            let target_ctx = joined_row
                .iter()
                .find(|ct| ct.table.id == table_id)
                .ok_or_else(|| DbError::Execution("target table not found in join context".into()))?;

            if let (Some(stored_row), Some(idx)) = (&target_ctx.row, target_ctx.storage_index) {
                if !deleted_indices.contains(&idx) {
                    enforce_foreign_keys_on_delete(&table, self.catalog, self.storage, stored_row)?;
                    deleted_indices.insert(idx);
                    if stmt.output.is_some() {
                        deleted_rows_for_output.push(stored_row.clone());
                    }
                }
            }
        }

        let mut indices_to_delete: Vec<usize> = deleted_indices.into_iter().collect();
        indices_to_delete.sort_unstable_by(|a, b| b.cmp(a));

        for idx in indices_to_delete {
            self.storage.delete_row(table_id, idx)?;
        }

        if let Some(output) = stmt.output {
            let output_rows: Vec<&crate::storage::StoredRow> = deleted_rows_for_output.iter().collect();
            return build_output_result(&output, &table, &[], &output_rows);
        }

        Ok(None)
    }
}
