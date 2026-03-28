mod delete;
mod insert;
pub(crate) mod output;
mod update;
pub(crate) mod validation;

pub(crate) use output::{build_output_result_merge, MergeOutputRow};

use crate::catalog::Catalog;
use crate::storage::Storage;
use crate::error::DbError;

use super::clock::Clock;

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
        ctx: &mut super::context::ExecutionContext,
    ) -> Result<(), crate::error::DbError> {
        let triggers: Vec<crate::catalog::TriggerDef> = self.catalog
            .find_triggers_for_table(table.schema_or_dbo(), &table.name)
            .into_iter()
            .cloned()
            .collect();

        for trigger in triggers {
            if trigger.events.contains(&event) && trigger.is_instead_of == is_instead_of {
                if ctx.trigger_depth >= 16 {
                    return Err(DbError::Execution("Maximum trigger nesting level (16) exceeded.".into()));
                }
                // Setup inserted/deleted pseudo-tables
                let mut trigger_ctx = ctx.subquery();
                trigger_ctx.trigger_depth += 1;
                trigger_ctx.enter_scope();

                let dbo_schema_id = self.catalog.get_schema_id("dbo").unwrap_or(1);

                let mut ins_physical = None;
                if !inserted_rows.is_empty() {
                    let ins_name = format!("__inserted_{}", uuid::Uuid::new_v4().simple());
                    let table_id = self.catalog.alloc_table_id();
                    let ins_table = crate::catalog::TableDef {
                        id: table_id,
                        schema_id: dbo_schema_id,
                        name: ins_name.clone(),
                        columns: table.columns.clone(),
                        check_constraints: vec![],
                        foreign_keys: vec![],
                    };
                    self.catalog.get_tables_mut().push(ins_table);
                    self.storage.ensure_table(table_id);
                    for row in inserted_rows {
                        self.storage.insert_row(table_id, row.clone())?;
                    }
                    trigger_ctx.temp_table_map.insert("INSERTED".to_string(), ins_name.clone());
                    trigger_ctx.temp_table_map.insert("INSERTED".to_uppercase(), ins_name.clone());
                    ins_physical = Some((table_id, ins_name));
                }

                let mut del_physical = None;
                if !deleted_rows.is_empty() {
                    let del_name = format!("__deleted_{}", uuid::Uuid::new_v4().simple());
                    let table_id = self.catalog.alloc_table_id();
                    let del_table = crate::catalog::TableDef {
                        id: table_id,
                        schema_id: dbo_schema_id,
                        name: del_name.clone(),
                        columns: table.columns.clone(),
                        check_constraints: vec![],
                        foreign_keys: vec![],
                    };
                    self.catalog.get_tables_mut().push(del_table);
                    self.storage.ensure_table(table_id);
                    for row in deleted_rows {
                        self.storage.insert_row(table_id, row.clone())?;
                    }
                    trigger_ctx.temp_table_map.insert("DELETED".to_string(), del_name.clone());
                    trigger_ctx.temp_table_map.insert("DELETED".to_uppercase(), del_name.clone());
                    del_physical = Some((table_id, del_name));
                }

                let mut script_executor = super::script::ScriptExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                };
                let res = script_executor.execute_batch(&trigger.body, &mut trigger_ctx);

                // Cleanup
                if let Some((id, _name)) = ins_physical {
                    self.catalog.get_tables_mut().retain(|t| t.id != id);
                    let _ = self.storage.clear_table(id);
                }
                if let Some((id, _name)) = del_physical {
                    self.catalog.get_tables_mut().retain(|t| t.id != id);
                    let _ = self.storage.clear_table(id);
                }

                if let Err(e) = res {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn insert_output_into(
        &mut self,
        target: &crate::ast::ObjectName,
        output_result: &crate::executor::result::QueryResult,
        ctx: &mut super::context::ExecutionContext,
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
            .ok_or_else(|| {
                crate::error::DbError::Semantic(format!(
                    "table '{}.{}' not found for OUTPUT INTO",
                    target_schema, target_name
                ))
            })?
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

            self.storage.insert_row(table.id, stored_row)?;
        }

        Ok(())
    }
}
