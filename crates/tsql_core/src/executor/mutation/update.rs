use std::collections::HashSet;

use crate::ast::UpdateStmt;
use crate::error::DbError;

use super::super::context::ExecutionContext;
use super::super::query::QueryExecutor;
use super::query_source::{build_mutation_query, resolve_table_for_mutation};
use super::super::result::QueryResult;

use super::MutationExecutor;
use super::output::build_output_result;
use super::shared::{rowcount_limit, visit_target_rows};
use super::validation::{
    apply_assignments, enforce_checks_on_row, enforce_foreign_keys_on_delete,
    enforce_foreign_keys_on_insert, enforce_foreign_keys_on_update, enforce_unique_on_update, validate_row_against_table,
};

impl<'a> MutationExecutor<'a> {
    pub(crate) fn execute_update_with_context(
        &mut self,
        mut stmt: UpdateStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
            stmt.table.name = mapped;
            if stmt.table.schema.is_none() {
                stmt.table.schema = Some("dbo".to_string());
            }
        }
        let (table, resolved_name) = resolve_table_for_mutation(stmt.from.as_ref(), &stmt.table, |schema, name| {
            self.catalog.find_table(schema, name).cloned().or_else(|| {
                ctx.resolve_table_name(name)
                    .and_then(|mapped| self.catalog.find_table("dbo", &mapped).cloned())
            })
        })?;

        let table_id = table.id;
        let target_alias = stmt.table.name.clone();

        // Check for INSTEAD OF UPDATE trigger
        let instead_of_triggers = if ctx.frame.skip_instead_of {
            vec![]
        } else {
            self.find_triggers(&table, crate::ast::TriggerEvent::Update)
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
            let mut inserted_rows = Vec::new();
            let mut deleted_rows = Vec::new();
            let rowcount_limit = rowcount_limit(ctx);
            let mut updated_indices = HashSet::new();
            let _updated_count = visit_target_rows(
                joined_rows,
                table_id,
                &target_alias,
                rowcount_limit,
                &mut updated_indices,
                |stored_row, _idx, joined_row| {
                    let mut new_row = stored_row.clone();
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
                    inserted_rows.push(new_row);
                    deleted_rows.push(stored_row.clone());
                    Ok(())
                },
            )?;

            self.execute_triggers(
                &table,
                crate::ast::TriggerEvent::Update,
                true,
                &inserted_rows,
                &deleted_rows,
                ctx,
            )?;

            if let Some(output) = stmt.output {
                let inserted: Vec<&crate::storage::StoredRow> = inserted_rows.iter().collect();
                let deleted: Vec<&crate::storage::StoredRow> = deleted_rows.iter().collect();
                let result = build_output_result(&output, &table, &inserted, &deleted)?;
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

        let has_after_triggers = !self.find_triggers(&table, crate::ast::TriggerEvent::Update)
            .into_iter()
            .filter(|t| !t.is_instead_of)
            .collect::<Vec<_>>()
            .is_empty();

        let collect_rows = stmt.output.is_some() || has_after_triggers;
        let mut inserted_rows_for_output = Vec::new();
        let mut deleted_rows_for_output = Vec::new();
        let rowcount_limit = rowcount_limit(ctx);
        let mut updated_indices = HashSet::new();
        let _updated_count = visit_target_rows(
            joined_rows,
            table_id,
            &target_alias,
            rowcount_limit,
            &mut updated_indices,
            |stored_row, idx, joined_row| {
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
                enforce_foreign_keys_on_update(&table, self.catalog, self.storage, stored_row, &new_row)?;
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
                self.push_dirty_update(ctx, &table.name, idx, &new_row);

                if collect_rows {
                    inserted_rows_for_output.push(new_row);
                    deleted_rows_for_output.push(stored_row.clone());
                }
                Ok(())
            },
        )?;

        self.execute_triggers(&table, crate::ast::TriggerEvent::Update, false, &inserted_rows_for_output, &deleted_rows_for_output, ctx)?;

        if let Some(output) = stmt.output {
            let inserted: Vec<&crate::storage::StoredRow> = inserted_rows_for_output.iter().collect();
            let deleted: Vec<&crate::storage::StoredRow> = deleted_rows_for_output.iter().collect();
            let result = build_output_result(&output, &table, &inserted, &deleted)?;
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
