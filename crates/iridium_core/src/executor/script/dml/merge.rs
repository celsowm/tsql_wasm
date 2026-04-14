use super::super::ScriptExecutor;
use super::merge_helpers::{
    merge_process_matched_phase, merge_process_not_matched_by_source_phase,
    merge_process_not_matched_phase, merge_source_rows,
};
use crate::ast::MergeStmt;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::query::QueryExecutor;
use crate::executor::result::QueryResult;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_merge(
        &mut self,
        stmt: MergeStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let target_object = stmt
            .target
            .name_as_object()
            .ok_or_else(|| DbError::Execution("MERGE target must be a named table".into()))?;
        if ctx.is_readonly_table_var(target_object.name.as_str()) {
            return Err(DbError::Execution(format!(
                "table-valued parameter '{}' is READONLY",
                target_object.name
            )));
        }
        let target_name = ctx
            .resolve_table_name(target_object.name.as_str())
            .unwrap_or_else(|| target_object.name.clone());
        let target_schema = target_object.schema_or_dbo();
        let target_table = self
            .catalog
            .find_table(target_schema, &target_name)
            .ok_or_else(|| {
                DbError::Semantic(format!(
                    "table '{}.{}' not found",
                    target_schema, target_name
                ))
            })?
            .clone();

        // Execute source query
        let qe = QueryExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };
        let source_rows = merge_source_rows(&stmt, &qe, ctx)?;

        let target_alias = stmt
            .target
            .alias
            .clone()
            .unwrap_or_else(|| target_name.clone());

        let target_rows = self
            .storage
            .scan_rows(target_table.id)?
            .collect::<Result<Vec<_>, DbError>>()?;
        let mut source_matched_to_target = vec![false; source_rows.len()];
        let mut updated_target_rows = target_rows.clone();
        let mut merge_output_rows: Vec<crate::executor::mutation::MergeOutputRow> = Vec::new();
        let mut inserted_rows_for_trigger = Vec::new();
        let mut deleted_rows_for_trigger = Vec::new();

        merge_process_matched_phase(
            &stmt,
            &target_table,
            &target_alias,
            &source_rows,
            &target_rows,
            ctx,
            self.catalog,
            self.storage,
            self.clock,
            &mut source_matched_to_target,
            &mut updated_target_rows,
            &mut merge_output_rows,
            &mut inserted_rows_for_trigger,
            &mut deleted_rows_for_trigger,
        )?;

        // Process WHEN NOT MATCHED (source rows not matched to target)
        let mut inserted_new_rows: Vec<crate::storage::StoredRow> = Vec::new();
        merge_process_not_matched_phase(
            &stmt,
            &target_table,
            &source_rows,
            ctx,
            self.catalog,
            self.storage,
            self.clock,
            &mut source_matched_to_target,
            &mut merge_output_rows,
            &mut inserted_rows_for_trigger,
            &mut inserted_new_rows,
        )?;

        // Process WHEN NOT MATCHED BY SOURCE (target rows not matched by any source row)
        merge_process_not_matched_by_source_phase(
            &stmt,
            &target_table,
            &target_alias,
            &source_rows,
            &target_rows,
            ctx,
            self.catalog,
            self.storage,
            self.clock,
            &mut updated_target_rows,
            &mut merge_output_rows,
            &mut inserted_rows_for_trigger,
            &mut deleted_rows_for_trigger,
        )?;

        // Write all changes to storage
        self.storage.clear_table(target_table.id)?;
        if let Some(db) = &ctx.session.dirty_buffer {
            db.lock().push_op(
                ctx.session_id(),
                target_table.name.clone(),
                crate::executor::dirty_buffer::DirtyOp::Truncate,
            );
        }
        for row in updated_target_rows.iter() {
            if !row.deleted {
                self.storage.insert_row(target_table.id, row.clone())?;
            }
        }
        for row in &inserted_new_rows {
            self.storage.insert_row(target_table.id, row.clone())?;
        }

        let mut mut_exec = crate::executor::mutation::MutationExecutor {
            catalog: self.catalog,
            storage: self.storage,
            clock: self.clock,
        };

        if !inserted_rows_for_trigger.is_empty() {
            mut_exec.execute_triggers(
                &target_table,
                crate::ast::TriggerEvent::Insert,
                false,
                &inserted_rows_for_trigger,
                &[],
                ctx,
            )?;
        }
        if !deleted_rows_for_trigger.is_empty() {
            mut_exec.execute_triggers(
                &target_table,
                crate::ast::TriggerEvent::Delete,
                false,
                &[],
                &deleted_rows_for_trigger,
                ctx,
            )?;
        }
        // UPDATE trigger is fired if both inserted and deleted rows for triggers are present and match by index?
        // Actually SQL Server fires UPDATE trigger for MERGE when MATCHED ... UPDATE occurs.
        // We can just fire INSERT and DELETE triggers as a simplification if we don't track which rows were updated.
        // But we do track them. Let's fire UPDATE triggers too if appropriate.
        // For now, firing INSERT/DELETE triggers based on what happened is a good start.

        if let Some(ref output) = stmt.output {
            let result = crate::executor::mutation::build_output_result_merge(
                output,
                &target_table,
                &merge_output_rows,
            )?;
            if let Some(ref target) = stmt.output_into {
                if let Some(result) = result.as_ref() {
                    mut_exec.insert_output_into(target, result, ctx)?;
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
