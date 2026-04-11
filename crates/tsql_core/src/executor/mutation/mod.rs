mod delete;
mod insert_source;
mod insert;
pub(crate) mod query_source;
mod shared;
pub(crate) mod output;
mod update;
pub(crate) mod validation;

pub(crate) use output::{build_output_result_merge, MergeOutputRow};

use crate::catalog::Catalog;
use crate::storage::Storage;
use crate::error::DbError;

use super::clock::Clock;
use super::context::{ModuleFrame, ModuleKind};

pub(crate) struct MutationExecutor<'a> {
    pub(crate) catalog: &'a mut dyn Catalog,
    pub(crate) storage: &'a mut dyn Storage,
    pub(crate) clock: &'a dyn Clock,
}

impl<'a> MutationExecutor<'a> {
    fn find_triggers(
        &self,
        table: &crate::catalog::TableDef,
        event: crate::ast::TriggerEvent,
    ) -> Vec<crate::catalog::TriggerDef> {
        self.catalog
            .find_triggers_for_table(table.schema_or_dbo(), &table.name)
            .into_iter()
            .filter(|t| t.events.contains(&event))
            .cloned()
            .collect()
    }

    pub(crate) fn execute_triggers(
        &mut self,
        table: &crate::catalog::TableDef,
        event: crate::ast::TriggerEvent,
        is_instead_of: bool,
        inserted_rows: &[crate::storage::StoredRow],
        deleted_rows: &[crate::storage::StoredRow],
        ctx: &mut super::context::ExecutionContext<'_>,
    ) -> Result<(), crate::error::DbError> {
        let triggers: Vec<crate::catalog::TriggerDef> = self.catalog
            .find_triggers_for_table(table.schema_or_dbo(), &table.name)
            .into_iter()
            .cloned()
            .collect();

        for trigger in triggers {
            if trigger.events.contains(&event) && trigger.is_instead_of == is_instead_of {
                if ctx.trigger_depth() >= 16 {
                    return Err(DbError::Execution("Maximum trigger nesting level (16) exceeded.".into()));
                }
                // Setup inserted/deleted pseudo-tables
                let mut trigger_ctx = ctx.subquery();
                trigger_ctx.frame.trigger_depth += 1;
                if is_instead_of {
                    trigger_ctx.frame.skip_instead_of = true;
                }
                let scope_depth = trigger_ctx.frame.scope_vars.len();
                trigger_ctx.enter_scope();

                let dbo_schema_id = self.catalog.get_schema_id("dbo").unwrap_or(1);

                let mut ins_physical = None;
                if !inserted_rows.is_empty() {
                    let ins_name = format!("__inserted_{}", uuid::Uuid::new_v4().simple());
                    let table_id = self.catalog.alloc_table_id();
                    let ins_table = crate::catalog::TableDef {
                        id: table_id,
                        schema_id: dbo_schema_id,
                        schema_name: "dbo".to_string(),
                        name: ins_name.clone(),
                        columns: table.columns.clone(),
                        check_constraints: vec![],
                        foreign_keys: vec![],
                    };
                    self.catalog.register_table(ins_table);
                    self.storage.ensure_table(table_id);
                    for row in inserted_rows {
                        self.storage.insert_row(table_id, row.clone())?;
                    }
                    trigger_ctx.session.temp_map.insert("INSERTED".to_string(), ins_name.clone());
                    ins_physical = Some((table_id, ins_name));
                }

                let mut del_physical = None;
                if !deleted_rows.is_empty() {
                    let del_name = format!("__deleted_{}", uuid::Uuid::new_v4().simple());
                    let table_id = self.catalog.alloc_table_id();
                    let del_table = crate::catalog::TableDef {
                        id: table_id,
                        schema_id: dbo_schema_id,
                        schema_name: "dbo".to_string(),
                        name: del_name.clone(),
                        columns: table.columns.clone(),
                        check_constraints: vec![],
                        foreign_keys: vec![],
                    };
                    self.catalog.register_table(del_table);
                    self.storage.ensure_table(table_id);
                    for row in deleted_rows {
                        self.storage.insert_row(table_id, row.clone())?;
                    }
                    trigger_ctx.session.temp_map.insert("DELETED".to_string(), del_name.clone());
                    del_physical = Some((table_id, del_name));
                }

                let mut script_executor = super::script::ScriptExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                };
                trigger_ctx.push_module(ModuleFrame {
                    object_id: trigger.object_id,
                    schema: trigger.schema.clone(),
                    name: trigger.name.clone(),
                    kind: ModuleKind::Trigger,
                });
                let res = script_executor.execute_batch(&trigger.body, &mut trigger_ctx);
                while trigger_ctx.frame.scope_vars.len() > scope_depth {
                    trigger_ctx.leave_scope();
                }
                trigger_ctx.pop_module();

                // Cleanup
                if let Some((id, _name)) = ins_physical {
                    self.catalog.unregister_table_by_id(id);
                    let _ = self.storage.clear_table(id);
                }
                if let Some((id, _name)) = del_physical {
                    self.catalog.unregister_table_by_id(id);
                    let _ = self.storage.clear_table(id);
                }

                res?;
            }
        }
        Ok(())
    }

    pub(crate) fn push_dirty_insert(
        &self,
        ctx: &mut super::context::ExecutionContext<'_>,
        table_name: &str,
        row: &crate::storage::StoredRow,
    ) {
        super::dirty_buffer::push_dirty_op(
            &ctx.session.dirty_buffer,
            ctx.session_id(),
            table_name.to_string(),
            super::dirty_buffer::DirtyOp::Insert { row: row.clone() },
        );
    }

    pub(crate) fn push_dirty_update(
        &self,
        ctx: &mut super::context::ExecutionContext<'_>,
        table_name: &str,
        row_index: usize,
        new_row: &crate::storage::StoredRow,
    ) {
        super::dirty_buffer::push_dirty_op(
            &ctx.session.dirty_buffer,
            ctx.session_id(),
            table_name.to_string(),
            super::dirty_buffer::DirtyOp::Update {
                row_index,
                new_row: new_row.clone(),
            },
        );
    }

    pub(crate) fn push_dirty_delete(
        &self,
        ctx: &mut super::context::ExecutionContext<'_>,
        table_name: &str,
        row_index: usize,
    ) {
        super::dirty_buffer::push_dirty_op(
            &ctx.session.dirty_buffer,
            ctx.session_id(),
            table_name.to_string(),
            super::dirty_buffer::DirtyOp::Delete { row_index },
        );
    }

    pub(crate) fn push_dirty_truncate(
        &self,
        ctx: &mut super::context::ExecutionContext<'_>,
        table_name: &str,
    ) {
        super::dirty_buffer::push_dirty_op(
            &ctx.session.dirty_buffer,
            ctx.session_id(),
            table_name.to_string(),
            super::dirty_buffer::DirtyOp::Truncate,
        );
    }

    pub(crate) fn push_dirty_replace(
        &self,
        ctx: &mut super::context::ExecutionContext<'_>,
        table_name: &str,
        rows: Vec<crate::storage::StoredRow>,
    ) {
        super::dirty_buffer::push_dirty_op(
            &ctx.session.dirty_buffer,
            ctx.session_id(),
            table_name.to_string(),
            super::dirty_buffer::DirtyOp::ReplaceTable { rows },
        );
    }

    pub(crate) fn insert_output_into(
        &mut self,
        target: &crate::ast::ObjectName,
        output_result: &crate::executor::result::QueryResult,
        ctx: &mut super::context::ExecutionContext<'_>,
    ) -> Result<(), crate::error::DbError> {
        let mut target_name = target.name.clone();
        let mut target_schema = target.schema_or_dbo().to_string();

        if let Some(mapped) = ctx.resolve_table_name(&target_name) {
            target_name = mapped;
            target_schema = "dbo".to_string();
        }

        let table = self
            .catalog
            .find_table(&target_schema, &target_name)
            .ok_or_else(|| DbError::table_not_found(&target_schema, &target_name))?
            .clone();

        for row_values in &output_result.rows {
            let mut final_values = vec![crate::types::Value::Null; table.columns.len()];
            for (idx, val) in row_values.iter().enumerate() {
                if idx < table.columns.len() {
                    final_values[idx] = val.clone();
                }
            }

            let mut stored_row = crate::storage::StoredRow {
                values: final_values,
                deleted: false,
            };
            self.apply_missing_values(&table, &mut stored_row.values, ctx)?;

            // Re-validate constraints on the target table
            validation::enforce_unique_on_insert(&table, self.storage, table.id, &stored_row)?;
            validation::enforce_foreign_keys_on_insert(&table, self.catalog, self.storage, &stored_row)?;
            validation::enforce_checks_on_row(&table, &stored_row, ctx, self.catalog, self.storage, self.clock)?;

            self.storage.insert_row(table.id, stored_row.clone())?;
            self.push_dirty_insert(ctx, &table.name, &stored_row);
        }

        Ok(())
    }
}
