use std::collections::HashSet;

use crate::ast::Statement;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::conflict::detect_conflicts;
use super::journal::{Journal, JournalEvent, WriteKind};
use super::locks::{SessionId, TxWorkspace};
use super::session::SharedState;
use super::table_util::{collect_read_tables, collect_write_tables};
use super::transaction::{TransactionManager, WriteIntentKind};

pub(crate) fn execute_transaction_statement<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S>,
    journal: &mut Box<dyn Journal>,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    stmt: Statement,
) -> Result<Option<super::result::QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static,
{
    match stmt {
        Statement::BeginTransaction(name) => {
            let workspace_catalog = state.catalog.clone();
            let workspace_storage = state.storage.clone();
            tx_manager.commit_ts = state.commit_ts;
            let begin_name = tx_manager.begin(&workspace_catalog, &workspace_storage, name)?;
            *workspace_slot = Some(TxWorkspace {
                catalog: workspace_catalog,
                storage: workspace_storage,
                base_table_versions: state.table_versions.clone(),
                read_tables: HashSet::new(),
                write_tables: HashSet::new(),
                acquired_locks: Vec::new(),
            });
            journal.record(JournalEvent::Begin {
                isolation_level: tx_manager.session_isolation_level,
                name: begin_name,
            });
            Ok(None)
        }
        Statement::CommitTransaction(_) => {
            let tx = tx_manager
                .active
                .as_ref()
                .ok_or_else(|| DbError::Execution("COMMIT without active transaction".into()))?;
            let workspace = workspace_slot.as_ref().ok_or_else(|| {
                DbError::Execution("internal error: missing transaction workspace".into())
            })?;

            let conflicts = detect_conflicts(
                tx.isolation_level,
                &workspace.base_table_versions,
                &workspace.read_tables,
                &workspace.write_tables,
                &state.table_versions,
            );
            if conflicts {
                return Err(DbError::Execution(
                    "transaction conflict detected during COMMIT".into(),
                ));
            }

            let next_commit_ts = state.commit_ts + 1;
            let mut next_table_versions = state.table_versions.clone();
            for table in &workspace.write_tables {
                next_table_versions.insert(table.clone(), next_commit_ts);
            }
            let checkpoint = super::durability::RecoveryCheckpoint {
                catalog: workspace.catalog.clone(),
                storage_data: workspace.storage.get_checkpoint_data(),
                commit_ts: next_commit_ts,
                table_versions: next_table_versions.clone(),
            };
            state.durability.persist_checkpoint(&checkpoint)?;

            state.catalog = workspace.catalog.clone();
            state.storage = workspace.storage.clone();
            state.commit_ts = next_commit_ts;
            for table in &workspace.write_tables {
                state.table_versions.insert(table.clone(), state.commit_ts);
            }
            state.table_locks.release_workspace_locks(session_id, workspace_slot, 0);
            tx_manager.active = None;
            tx_manager.commit_ts = state.commit_ts;
            *workspace_slot = None;
            journal.record(JournalEvent::Commit);
            Ok(None)
        }
        Statement::RollbackTransaction(savepoint) => {
            {
                let workspace = workspace_slot.as_mut().ok_or_else(|| {
                    DbError::Execution("ROLLBACK without active transaction".into())
                })?;
                tx_manager.rollback(
                    savepoint.clone(),
                    &mut workspace.catalog,
                    &mut workspace.storage,
                )?;
                if let Some(ref active_tx) = tx_manager.active {
                    let keep = active_tx.write_set.len();
                    if workspace.write_tables.len() > keep {
                        let mut names: Vec<_> = workspace.write_tables.iter().cloned().collect();
                        names.sort();
                        names.truncate(keep);
                        workspace.write_tables = names.into_iter().collect();
                    }
                }
            }
            if let Some(ref active_tx) = tx_manager.active {
                let keep_depth = active_tx.savepoints.len();
                state.table_locks.release_workspace_locks(session_id, workspace_slot, keep_depth);
            } else {
                state.table_locks.release_workspace_locks(session_id, workspace_slot, 0);
                *workspace_slot = None;
            }
            journal.record(JournalEvent::Rollback { savepoint });
            Ok(None)
        }
        Statement::SaveTransaction(name) => {
            let workspace = workspace_slot.as_ref().ok_or_else(|| {
                DbError::Execution("SAVE TRANSACTION without active transaction".into())
            })?;
            tx_manager.save(name.clone(), &workspace.catalog, &workspace.storage)?;
            journal.record(JournalEvent::Savepoint { name });
            Ok(None)
        }
        Statement::SetTransactionIsolationLevel(level) => {
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
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S>,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
)
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static,
{
    if tx_manager.active.is_none() {
        return;
    }
    if let Some(workspace) = workspace_slot.as_mut() {
        let _ = tx_manager.rollback(None, &mut workspace.catalog, &mut workspace.storage);
    }
    state.table_locks.release_workspace_locks(session_id, workspace_slot, 0);
    *workspace_slot = None;
    tx_manager.active = None;
    tx_manager.commit_ts = state.commit_ts;
    journal.record(JournalEvent::Rollback { savepoint: None });
}

pub(crate) fn register_read_tables<C, S>(workspace_slot: &mut Option<TxWorkspace<C, S>>, stmt: &Statement) {
    if let Some(workspace) = workspace_slot.as_mut() {
        for table in collect_read_tables(stmt) {
            workspace.read_tables.insert(table);
        }
    }
}

pub(crate) fn register_workspace_write_tables<C, S>(workspace_slot: &mut Option<TxWorkspace<C, S>>, stmt: &Statement) {
    if let Some(workspace) = workspace_slot.as_mut() {
        for table in collect_write_tables(stmt) {
            workspace.write_tables.insert(table);
        }
    }
}

pub(crate) fn register_write_intent<C, S>(
    tx_manager: &mut TransactionManager<C, S>,
    journal: &mut dyn Journal,
    stmt: &Statement,
)
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static,
{
    if tx_manager.active.is_none() {
        return;
    }

    let (kind, table) = match stmt {
        Statement::Insert(s) => (WriteIntentKind::Insert, Some(s.table.name.clone())),
        Statement::Update(s) => (WriteIntentKind::Update, Some(s.table.name.clone())),
        Statement::Delete(s) => (WriteIntentKind::Delete, Some(s.table.name.clone())),
        Statement::CreateTable(s) => (WriteIntentKind::Ddl, Some(s.name.name.clone())),
        Statement::DropTable(s) => (WriteIntentKind::Ddl, Some(s.name.name.clone())),
        Statement::AlterTable(s) => (WriteIntentKind::Ddl, Some(s.table.name.clone())),
        Statement::TruncateTable(s) => (WriteIntentKind::Ddl, Some(s.name.name.clone())),
        Statement::CreateIndex(s) => (WriteIntentKind::Ddl, Some(s.table.name.clone())),
        Statement::DropIndex(s) => (WriteIntentKind::Ddl, Some(s.table.name.clone())),
        Statement::CreateSchema(_) | Statement::DropSchema(_) => (WriteIntentKind::Ddl, None),
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
