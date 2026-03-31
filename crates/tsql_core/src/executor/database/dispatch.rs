use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ast::{DropTableStmt, IsolationLevel, ObjectName, Statement};
use crate::catalog::Catalog;
use crate::error::{DbError, StmtOutcome, StmtResult};
use crate::storage::Storage;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::dirty_buffer;
use super::super::journal::{Journal, JournalEvent};
use super::super::locks::{LockTable, SessionId, TxWorkspace};
use super::super::result::QueryResult;
use super::super::schema::SchemaExecutor;
use super::super::script::ScriptExecutor;
use super::super::session::{SessionSnapshot, SharedState};
use super::super::table_util::collect_write_tables;
use super::super::tooling::{apply_set_option, SessionOptions};
use super::super::transaction::TransactionManager;
use super::super::transaction_exec;

pub(crate) fn execute_non_transaction_statement<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S, SessionSnapshot>,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    clock: &dyn Clock,
    session_options: &mut SessionOptions,
    stmt: Statement,
    ctx: &mut ExecutionContext,
) -> StmtResult<Option<QueryResult>>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    if let Statement::SetOption(opt) = &stmt {
        let apply = apply_set_option(opt, session_options)?;
        ctx.ansi_nulls = session_options.ansi_nulls;
        ctx.datefirst = session_options.datefirst;
        for warn in apply.warnings {
            journal.record(JournalEvent::Info { message: warn });
        }
        return Ok(StmtOutcome::Ok(None));
    }

    if let Statement::SetIdentityInsert(ref id_stmt) = stmt {
        let table_name = id_stmt.table.name.to_uppercase();
        if id_stmt.on {
            session_options.identity_insert.insert(table_name);
        } else {
            session_options.identity_insert.remove(&table_name);
        }
        return Ok(StmtOutcome::Ok(None));
    }

    let isolation_level = tx_manager
        .active
        .as_ref()
        .map(|tx| tx.isolation_level)
        .unwrap_or(tx_manager.session_isolation_level);
    let is_select = matches!(stmt, Statement::Select(_));
    let read_uncommitted_dirty = isolation_level == IsolationLevel::ReadUncommitted && is_select;
    let read_committed_select = isolation_level == IsolationLevel::ReadCommitted && is_select;

    if tx_manager.active.is_some() {
        LockTable::acquire_statement_locks(
            &state.table_locks,
            session_id,
            tx_manager,
            workspace_slot,
            &stmt,
            session_options.lock_timeout_ms,
        )?;

        let out = if read_uncommitted_dirty {
            let (mut dirty_catalog, mut dirty_storage) =
                dirty_buffer::build_dirty_read_storage(state, session_id, workspace_slot);
            let mut script = ScriptExecutor {
                catalog: &mut dirty_catalog,
                storage: &mut dirty_storage,
                clock,
            };
            script.execute(stmt.clone(), ctx)
        } else if read_committed_select {
            let workspace = workspace_slot.as_mut().ok_or_else(|| {
                DbError::Execution("internal error: missing transaction workspace".into())
            })?;

            {
                let storage_guard = state.storage.read();
                for table_def in storage_guard.catalog.get_tables() {
                    let tname = table_def.name.to_uppercase();
                    if workspace.write_tables.contains(&tname) {
                        continue;
                    }
                    let tid = table_def.id;
                    if let Ok(committed_rows) = storage_guard.storage.get_rows(tid) {
                        let _ = workspace.storage.update_rows(tid, committed_rows);
                    }
                }
                for table_def in storage_guard.catalog.get_tables() {
                    let tname = table_def.name.to_uppercase();
                    if workspace.write_tables.contains(&tname) {
                        continue;
                    }
                    if workspace.catalog.find_table(table_def.schema_or_dbo(), &table_def.name).is_none() {
                        workspace.catalog.get_tables_mut().push(table_def.clone());
                    }
                }
            }

            let mut script = ScriptExecutor {
                catalog: &mut workspace.catalog,
                storage: &mut workspace.storage,
                clock,
            };
            script.execute(stmt.clone(), ctx)
        } else {
            let workspace = workspace_slot.as_mut().ok_or_else(|| {
                DbError::Execution("internal error: missing transaction workspace".into())
            })?;
            let mut script = ScriptExecutor {
                catalog: &mut workspace.catalog,
                storage: &mut workspace.storage,
                clock,
            };
            script.execute(stmt.clone(), ctx)
        };
        // Control flow signals (Break/Continue/Return) should not affect transaction state
        let is_control_flow = out.as_ref().map_or(false, |o| o.is_control_flow());
        if out.is_ok() && !is_control_flow {
            transaction_exec::register_read_tables(workspace_slot, &stmt);
            transaction_exec::register_workspace_write_tables(workspace_slot, &stmt);
            transaction_exec::register_write_intent(tx_manager, journal, &stmt);
            if tx_manager.xact_state != -1 {
                tx_manager.xact_state = 1;
            }
        } else if out.is_err() && session_options.xact_abort {
            transaction_exec::force_xact_abort(
                state,
                session_id,
                tx_manager,
                journal,
                workspace_slot,
                ctx,
                session_options,
            );
        } else if out.is_err() {
            tx_manager.xact_state = -1;
        }
        out
    } else {
        if read_uncommitted_dirty {
            let (mut dirty_catalog, mut dirty_storage) =
                dirty_buffer::build_dirty_read_storage(state, session_id, workspace_slot);
            let mut script = ScriptExecutor {
                catalog: &mut dirty_catalog,
                storage: &mut dirty_storage,
                clock,
            };
            return script.execute(stmt, ctx);
        }

        let written_tables = collect_write_tables(&stmt);
        if written_tables.is_empty() {
            LockTable::acquire_statement_locks(
                &state.table_locks,
                session_id,
                tx_manager,
                workspace_slot,
                &stmt,
                session_options.lock_timeout_ms,
            )?;

            let mut storage_guard = state.storage.write();
            let (cat, stor) = storage_guard.get_mut_refs();
            let mut script = ScriptExecutor {
                catalog: cat,
                storage: stor,
                clock,
            };
            let out = script.execute(stmt, ctx);
            state.table_locks.lock().release_all_for_session(session_id);
            return out;
        }

        LockTable::acquire_statement_locks(
            &state.table_locks,
            session_id,
            tx_manager,
            workspace_slot,
            &stmt,
            session_options.lock_timeout_ms,
        )?;

        let mut storage_guard = state.storage.write();
        let before_catalog = storage_guard.catalog.clone();
        let before_storage = storage_guard.storage.clone();
        let before_versions = storage_guard.table_versions.clone();
        let before_commit_ts = storage_guard.commit_ts;
        let (cat, stor) = storage_guard.get_mut_refs();
        let mut script = ScriptExecutor {
            catalog: cat,
            storage: stor,
            clock,
        };
        let out = script.execute(stmt, ctx);
        let is_control_flow = out.as_ref().map_or(false, |o| o.is_control_flow());
        if out.is_ok() && !is_control_flow {
            storage_guard.commit_ts += 1;
            for table in &written_tables {
                let ts = storage_guard.commit_ts;
                storage_guard.table_versions.insert(table.clone(), ts);
            }
            let checkpoint = state.to_checkpoint_internal(&storage_guard);
            if let Err(e) = state.durability.lock().persist_checkpoint(&checkpoint) {
                storage_guard.catalog = before_catalog;
                storage_guard.storage = before_storage;
                storage_guard.table_versions = before_versions;
                storage_guard.commit_ts = before_commit_ts;
                state.table_locks.lock().release_all_for_session(session_id);
                return Err(e);
            }
        }
        state.table_locks.lock().release_all_for_session(session_id);
        out
    }
}

pub(crate) fn cleanup_scope_table_vars(
    catalog: &mut dyn Catalog,
    storage: &mut dyn Storage,
    ctx: &mut ExecutionContext,
) -> Result<(), DbError> {
    let dropped_physical = ctx.leave_scope_collect_table_vars();
    for physical in dropped_physical {
        if catalog.find_table("dbo", &physical).is_none() {
            continue;
        }
        let mut schema = SchemaExecutor { catalog, storage };
        schema.drop_table(DropTableStmt {
            name: ObjectName {
                schema: Some("dbo".to_string()),
                name: physical,
            },
        })?;
    }
    Ok(())
}
