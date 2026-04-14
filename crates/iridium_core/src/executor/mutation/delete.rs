use std::collections::HashSet;

use crate::ast::DeleteStmt;
use crate::error::DbError;
use crate::types::Value;

use super::super::context::ExecutionContext;
use super::super::query::QueryExecutor;
use super::super::result::QueryResult;
use super::query_source::{build_mutation_query, resolve_table_for_mutation};

use super::output::build_output_result;
use super::shared::{rowcount_limit, visit_target_rows};
use super::validation::enforce_foreign_keys_on_delete;
use super::MutationExecutor;

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_delete_with_context(
        &mut self,
        mut stmt: DeleteStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let (table, resolved_name) =
            resolve_table_for_mutation(stmt.from.as_ref(), &stmt.table, |schema, name| {
                self.catalog.find_table(schema, name).cloned().or_else(|| {
                    ctx.resolve_table_name(name)
                        .and_then(|mapped| self.catalog.find_table("dbo", &mapped).cloned())
                })
            })?;

        let table_id = table.id;
        let target_alias = stmt.table.name.clone();

        // Check for INSTEAD OF DELETE trigger
        let instead_of_triggers = if ctx.frame.skip_instead_of {
            vec![]
        } else {
            self.find_triggers(&table, crate::ast::TriggerEvent::Delete)
                .into_iter()
                .filter(|t| t.is_instead_of)
                .collect::<Vec<_>>()
        };

        let query = build_mutation_query(
            stmt.from.as_ref(),
            &stmt.table,
            &table,
            &resolved_name,
            stmt.selection.clone(),
            stmt.top.clone(),
        );

        let query_executor = QueryExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };

        let joined_rows = query_executor.execute_to_joined_rows(query, ctx)?;

        if !instead_of_triggers.is_empty() {
            let mut deleted_rows = Vec::new();
            let rowcount_limit = rowcount_limit(ctx);
            let mut deleted_indices = HashSet::new();
            let _deleted_count = visit_target_rows(
                joined_rows,
                table_id,
                &target_alias,
                rowcount_limit,
                &mut deleted_indices,
                |stored_row, _idx, _joined_row| {
                    deleted_rows.push(stored_row.clone());
                    Ok(())
                },
            )?;

            self.execute_triggers(
                &table,
                crate::ast::TriggerEvent::Delete,
                true,
                &[],
                &deleted_rows,
                ctx,
            )?;

            if let Some(output) = stmt.output {
                let output_rows: Vec<&crate::storage::StoredRow> = deleted_rows.iter().collect();
                let result = build_output_result(&output, &table, &[], &output_rows)?;
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

        let has_after_triggers = !self
            .find_triggers(&table, crate::ast::TriggerEvent::Delete)
            .into_iter()
            .filter(|t| !t.is_instead_of)
            .collect::<Vec<_>>()
            .is_empty();

        let collect_rows = stmt.output.is_some() || has_after_triggers;
        let mut deleted_rows_for_output = Vec::new();
        let rowcount_limit = rowcount_limit(ctx);
        let mut deleted_indices = HashSet::new();
        let _deleted_count = visit_target_rows(
            joined_rows,
            table_id,
            &target_alias,
            rowcount_limit,
            &mut deleted_indices,
            |stored_row, _idx, _joined_row| {
                enforce_foreign_keys_on_delete(&table, self.catalog, self.storage, stored_row)?;
                if collect_rows {
                    deleted_rows_for_output.push(stored_row.clone());
                }
                Ok(())
            },
        )?;

        let mut indices_to_delete: Vec<usize> = deleted_indices.into_iter().collect();
        indices_to_delete.sort_unstable_by(|a, b| b.cmp(a));

        let rows = self.storage.get_rows(table_id)?;
        let deleted_values: Vec<Vec<Value>> = indices_to_delete
            .iter()
            .filter_map(|&idx| rows.get(idx).map(|r| r.values.clone()))
            .collect();

        for (i, idx) in indices_to_delete.iter().enumerate() {
            self.storage.delete_row(table_id, *idx)?;

            if let Some(index_storage) = self.storage.as_index_storage_mut() {
                if let Some(values) = deleted_values.get(i) {
                    for idx_def in self
                        .catalog
                        .get_indexes()
                        .iter()
                        .filter(|i| i.table_id == table_id)
                    {
                        if let Some(bi) = index_storage.get_index_mut(idx_def.id) {
                            let _ = bi.delete(*idx, values);
                        }
                    }
                }
            }

            self.push_dirty_delete(ctx, &table.name, *idx);
        }

        self.execute_triggers(
            &table,
            crate::ast::TriggerEvent::Delete,
            false,
            &[],
            &deleted_rows_for_output,
            ctx,
        )?;

        if let Some(output) = stmt.output {
            let output_rows: Vec<&crate::storage::StoredRow> =
                deleted_rows_for_output.iter().collect();
            let result = build_output_result(&output, &table, &[], &output_rows)?;
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
}
