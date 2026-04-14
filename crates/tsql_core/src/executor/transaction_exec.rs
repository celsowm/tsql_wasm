use std::collections::HashSet;

use crate::ast::{DdlStatement, DmlStatement, SessionStatement, Statement, TransactionStatement};
use crate::error::DbError;

use super::conflict::detect_conflicts;
use super::database::{EngineCatalog, EngineStorage};
use super::journal::{Journal, JournalEvent, WriteKind};
use super::locks::{SessionId, TxWorkspace};
use super::session::SharedState;
use super::table_util::{collect_read_tables, collect_write_tables};
use super::transaction::{TransactionManager, WriteIntentKind};

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_transaction_statement<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S, super::session::SessionSnapshot>,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    ctx: &mut super::context::ExecutionContext,
    session_options: &mut super::tooling::SessionOptions,
    stmt: Statement,
) -> Result<Option<super::result::QueryResult>, DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
{
    match stmt {
        Statement::Transaction(TransactionStatement::Begin(name)) => {
            if tx_manager.depth == 0 {
                let (workspace_catalog, workspace_storage, commit_ts, table_versions) = {
                    let storage_guard = state.storage.read();
                    (
                        storage_guard.catalog.clone(),
                        storage_guard.storage.clone(),
                        storage_guard.commit_ts,
                        storage_guard.table_versions.clone(),
                    )
                };
                tx_manager.commit_ts = commit_ts;
                let extra = ctx.create_snapshot(session_options);
                let tx_id = state.allocate_tx_id();
                tx_manager.begin(name.clone(), commit_ts, tx_id, extra)?;
                *workspace_slot = Some(TxWorkspace {
                    catalog: workspace_catalog,
                    storage: workspace_storage,
                    base_table_versions: table_versions,
                    read_tables: HashSet::new(),
                    write_tables: HashSet::new(),
                    acquired_locks: Vec::new(),
                });
                state.wal_begin(tx_id, &format!("{:?}", tx_manager.session_isolation_level));
                journal.record(JournalEvent::Begin {
                    isolation_level: tx_manager.session_isolation_level,
                    name,
                });
            } else {
                tx_manager.depth += 1;
            }
            Ok(None)
        }
        Statement::Transaction(TransactionStatement::Commit(_)) => {
            if tx_manager.depth == 0 {
                return Err(DbError::Execution(
                    "COMMIT without active transaction".into(),
                ));
            }
            if tx_manager.xact_state == -1 {
                return Err(DbError::Execution(
                    "The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction.".into(),
                ));
            }
            tx_manager.depth -= 1;
            if tx_manager.depth > 0 {
                return Ok(None);
            }
            let tx = tx_manager
                .active
                .as_ref()
                .ok_or_else(|| DbError::Execution("COMMIT without active transaction".into()))?;
            let tx_id = tx.id;
            let workspace = workspace_slot.as_ref().ok_or_else(|| {
                DbError::Execution("internal error: missing transaction workspace".into())
            })?;

            let mut storage_guard = state.storage.write();

            let conflicts = detect_conflicts(
                tx.isolation_level,
                &workspace.base_table_versions,
                &workspace.read_tables,
                &workspace.write_tables,
                &storage_guard.table_versions,
            );
            if conflicts {
                return Err(DbError::Execution(
                    "transaction conflict detected during COMMIT".into(),
                ));
            }

            let next_commit_ts = storage_guard.commit_ts + 1;
            let mut next_table_versions = storage_guard.table_versions.clone();
            for table in &workspace.write_tables {
                next_table_versions.insert(table.clone(), next_commit_ts);
            }
            let checkpoint = super::durability::RecoveryCheckpoint {
                catalog: workspace.catalog.clone(),
                storage_data: workspace.storage.get_checkpoint_data(),
                commit_ts: next_commit_ts,
                table_versions: next_table_versions.clone(),
            };
            state.durability.lock().persist_checkpoint(&checkpoint)?;
            state.wal_commit(tx_id);

            storage_guard.catalog = workspace.catalog.clone();
            storage_guard.storage = workspace.storage.clone();
            storage_guard.commit_ts = next_commit_ts;
            for table in &workspace.write_tables {
                storage_guard
                    .table_versions
                    .insert(table.clone(), next_commit_ts);
            }
            state
                .table_locks
                .lock()
                .release_workspace_locks(session_id, workspace_slot, 0);
            state.dirty_buffer.lock().clear_session(session_id);
            if session_options.cursor_close_on_commit {
                ctx.session.cursors.clear();
                *ctx.session.fetch_status = -1;
            }
            tx_manager.active = None;
            tx_manager.commit_ts = storage_guard.commit_ts;
            tx_manager.xact_state = 0;
            *workspace_slot = None;
            journal.record(JournalEvent::Commit);
            Ok(None)
        }
        Statement::Transaction(TransactionStatement::Rollback(savepoint)) => {
            {
                let tx_id = tx_manager.active.as_ref().map(|tx| tx.id).unwrap_or(0);
                let workspace = workspace_slot.as_mut().ok_or_else(|| {
                    DbError::Execution("ROLLBACK without active transaction".into())
                })?;
                let mut extra = ctx.create_snapshot(session_options);
                let full_rollback = tx_manager.rollback(
                    savepoint.clone(),
                    &mut workspace.catalog,
                    &mut workspace.storage,
                    &mut extra,
                )?;
                ctx.restore_snapshot(extra, session_options);

                if full_rollback {
                    state.wal_rollback(tx_id);
                    {
                        let durability = state.durability.lock();
                        if let Some(checkpoint) = durability.latest_checkpoint() {
                            drop(durability);
                            let mut storage_guard = state.storage.write();
                            storage_guard.catalog = checkpoint.catalog;
                            let _ = storage_guard
                                .storage
                                .restore_from_checkpoint(checkpoint.storage_data);
                            storage_guard.commit_ts = checkpoint.commit_ts;
                            storage_guard.table_versions = checkpoint.table_versions;
                            drop(storage_guard);
                        }
                    }
                    tx_manager.active = None;
                    tx_manager.depth = 0;
                    tx_manager.xact_state = 0;
                    state
                        .table_locks
                        .lock()
                        .release_workspace_locks(session_id, workspace_slot, 0);
                    state.dirty_buffer.lock().clear_session(session_id);
                    if session_options.cursor_close_on_commit {
                        ctx.session.cursors.clear();
                        *ctx.session.fetch_status = -1;
                    }
                    *workspace_slot = None;
                } else {
                    if let Some(ref sp_name) = savepoint {
                        state.wal_savepoint(tx_id, sp_name);
                    }
                    if let Some(ref active_tx) = tx_manager.active {
                        let keep = active_tx.write_set.len();
                        if workspace.write_tables.len() > keep {
                            let mut names: Vec<_> =
                                workspace.write_tables.iter().cloned().collect();
                            names.sort();
                            names.truncate(keep);
                            workspace.write_tables = names.into_iter().collect();
                        }
                        let keep_depth = active_tx.savepoints.len();
                        state.table_locks.lock().release_workspace_locks(
                            session_id,
                            workspace_slot,
                            keep_depth,
                        );
                    }
                }
            }
            journal.record(JournalEvent::Rollback { savepoint });
            Ok(None)
        }
        Statement::Transaction(TransactionStatement::Save(name)) => {
            let tx_id = tx_manager.active.as_ref().map(|tx| tx.id).unwrap_or(0);
            let workspace = workspace_slot.as_ref().ok_or_else(|| {
                DbError::Execution("SAVE TRANSACTION without active transaction".into())
            })?;
            let extra = ctx.create_snapshot(session_options);
            tx_manager.save(name.clone(), &workspace.catalog, &workspace.storage, &extra)?;
            state.wal_savepoint(tx_id, &name);
            journal.record(JournalEvent::Savepoint { name });
            Ok(None)
        }
        Statement::Session(SessionStatement::SetTransactionIsolationLevel(level)) => {
            tx_manager.set_isolation_level(level)?;
            journal.record(JournalEvent::SetIsolationLevel {
                isolation_level: level,
            });
            Ok(None)
        }
        _ => Err(DbError::Execution(
            "internal error while executing transaction statement".into(),
        )),
    }
}

pub(crate) fn force_xact_abort<C, S>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S, super::session::SessionSnapshot>,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    ctx: &mut super::context::ExecutionContext,
    session_options: &mut super::tooling::SessionOptions,
) where
    C: EngineCatalog,
    S: EngineStorage,
{
    if tx_manager.active.is_none() {
        return;
    }
    let tx_id = tx_manager.active.as_ref().map(|tx| tx.id).unwrap_or(0);
    if let Some(workspace) = workspace_slot.as_mut() {
        let mut extra = ctx.create_snapshot(session_options);
        let _ = tx_manager.rollback(
            None,
            &mut workspace.catalog,
            &mut workspace.storage,
            &mut extra,
        );
        ctx.restore_snapshot(extra, session_options);
    }
    state.wal_rollback(tx_id);
    state
        .table_locks
        .lock()
        .release_workspace_locks(session_id, workspace_slot, 0);
    state.dirty_buffer.lock().clear_session(session_id);
    if session_options.cursor_close_on_commit {
        ctx.session.cursors.clear();
        *ctx.session.fetch_status = -1;
    }
    *workspace_slot = None;
    tx_manager.active = None;
    tx_manager.depth = 0;
    tx_manager.commit_ts = state.storage.read().commit_ts;
    tx_manager.xact_state = 0;
    journal.record(JournalEvent::Rollback { savepoint: None });
}

pub(crate) fn register_read_tables<C, S>(
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    stmt: &Statement,
) {
    if let Some(workspace) = workspace_slot.as_mut() {
        for table in collect_read_tables(stmt) {
            workspace.read_tables.insert(table);
        }
    }
}

pub(crate) fn register_workspace_write_tables<C, S>(
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    stmt: &Statement,
) {
    if let Some(workspace) = workspace_slot.as_mut() {
        for table in collect_write_tables(stmt) {
            workspace.write_tables.insert(table);
        }
    }
}

pub(crate) fn register_write_intent<C, S>(
    tx_manager: &mut TransactionManager<C, S, super::session::SessionSnapshot>,
    journal: &mut dyn Journal,
    stmt: &Statement,
) where
    C: EngineCatalog,
    S: EngineStorage,
{
    if tx_manager.active.is_none() {
        return;
    }

    let (kind, table) = match stmt {
        Statement::Dml(DmlStatement::Insert(s)) => {
            (WriteIntentKind::Insert, Some(s.table.name.clone()))
        }
        Statement::Dml(DmlStatement::Update(s)) => {
            (WriteIntentKind::Update, Some(s.table.name.clone()))
        }
        Statement::Dml(DmlStatement::Delete(s)) => {
            (WriteIntentKind::Delete, Some(s.table.name.clone()))
        }
        Statement::Ddl(DdlStatement::CreateTable(s)) => {
            (WriteIntentKind::Ddl, Some(s.name.name.clone()))
        }
        Statement::Ddl(DdlStatement::DropTable(s)) => {
            (WriteIntentKind::Ddl, Some(s.name.name.clone()))
        }
        Statement::Ddl(DdlStatement::AlterTable(s)) => {
            (WriteIntentKind::Ddl, Some(s.table.name.clone()))
        }
        Statement::Ddl(DdlStatement::TruncateTable(s)) => {
            (WriteIntentKind::Ddl, Some(s.name.name.clone()))
        }
        Statement::Ddl(DdlStatement::CreateIndex(s)) => {
            (WriteIntentKind::Ddl, Some(s.table.name.clone()))
        }
        Statement::Ddl(DdlStatement::DropIndex(s)) => {
            (WriteIntentKind::Ddl, Some(s.table.name.clone()))
        }
        Statement::Ddl(DdlStatement::CreateSchema(_))
        | Statement::Ddl(DdlStatement::DropSchema(_)) => (WriteIntentKind::Ddl, None),
        _ => return,
    };

    tx_manager.register_write_intent(kind, table.clone());
    journal.record(JournalEvent::WriteIntent {
        kind: map_write_kind(kind),
        table,
    });
}

fn map_write_kind(kind: WriteIntentKind) -> WriteKind {
    match kind {
        WriteIntentKind::Insert => WriteKind::Insert,
        WriteIntentKind::Update => WriteKind::Update,
        WriteIntentKind::Delete => WriteKind::Delete,
        WriteIntentKind::Ddl => WriteKind::Ddl,
    }
}
