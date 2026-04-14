use std::collections::HashSet;

use crate::ast::IsolationLevel;
use crate::error::DbError;

use super::string_norm::normalize_identifier;

#[derive(Debug, Clone)]
pub struct Savepoint<C, S, X> {
    pub name: String,
    /// Snapshot of the workspace catalog at savepoint time (needed for rollback).
    pub catalog_snapshot: C,
    /// Snapshot of the workspace storage at savepoint time (needed for rollback).
    pub storage_snapshot: S,
    pub extra_snapshot: X,
    pub write_intent_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteIntentKind {
    Insert,
    Update,
    Delete,
    Ddl,
}

#[derive(Debug, Clone)]
pub struct WriteIntent {
    pub kind: WriteIntentKind,
    pub table: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct WriteIntentTracker {
    read_locks: HashSet<String>,
    write_locks: HashSet<String>,
}

impl WriteIntentTracker {
    pub fn acquire_read(&mut self, resource: &str) {
        self.read_locks.insert(normalize_identifier(resource));
    }

    pub fn acquire_write(&mut self, resource: &str) {
        self.write_locks.insert(normalize_identifier(resource));
    }

    pub fn clear(&mut self) {
        self.read_locks.clear();
        self.write_locks.clear();
    }
}

/// P1 #17: TxState no longer stores begin_catalog/begin_storage snapshots.
/// The workspace holds the initial transaction state. On full rollback,
/// the workspace is simply discarded. Savepoint snapshots are still needed
/// for partial rollback within a transaction.
/// The `begin_extra` stores the session state at BEGIN time for full rollback restoration.
#[derive(Debug, Clone)]
pub struct TxState<C, S, X> {
    pub id: u64,
    pub isolation_level: IsolationLevel,
    pub savepoints: Vec<Savepoint<C, S, X>>,
    pub lock_manager: WriteIntentTracker,
    pub write_set: Vec<WriteIntent>,
    pub snapshot_ts: u64,
    pub begin_extra: X,
}

impl<C, S, X> TxState<C, S, X> {
    pub fn new(
        tx_id: u64,
        isolation_level: IsolationLevel,
        snapshot_ts: u64,
        begin_extra: X,
    ) -> Self {
        Self {
            id: tx_id,
            isolation_level,
            savepoints: vec![],
            lock_manager: WriteIntentTracker::default(),
            write_set: vec![],
            snapshot_ts,
            begin_extra,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionManager<C, S, X> {
    pub active: Option<TxState<C, S, X>>,
    pub session_isolation_level: IsolationLevel,
    pub commit_ts: u64,
    pub depth: u32,
    pub xact_state: i8,
}

impl<C, S, X> Default for TransactionManager<C, S, X> {
    fn default() -> Self {
        Self {
            active: None,
            session_isolation_level: IsolationLevel::default(),
            commit_ts: 0,
            depth: 0,
            xact_state: 0,
        }
    }
}

impl<C, S, X> TransactionManager<C, S, X>
where
    C: Clone,
    S: Clone,
    X: Clone,
{
    /// P1 #17: No longer clones catalog/storage. The workspace holds the
    /// transaction state. TxState only tracks metadata and the begin-time extra snapshot.
    pub fn begin(
        &mut self,
        explicit_name: Option<String>,
        snapshot_ts: u64,
        tx_id: u64,
        extra: X,
    ) -> Result<Option<String>, DbError> {
        if self.depth == 0 {
            let tx = TxState::new(tx_id, self.session_isolation_level, snapshot_ts, extra);
            self.active = Some(tx);
            self.xact_state = 1;
        }
        self.depth += 1;
        Ok(explicit_name)
    }

    pub fn commit(&mut self) -> Result<(), DbError> {
        if self.depth == 0 {
            return Err(DbError::Execution(
                "COMMIT without active transaction".into(),
            ));
        }
        if self.xact_state == -1 {
            return Err(DbError::Execution(
                "The current transaction cannot be committed and cannot support operations that write to the log file. Roll back the transaction.".into(),
            ));
        }
        self.depth -= 1;
        if self.depth > 0 {
            return Ok(());
        }
        let tx = self
            .active
            .take()
            .ok_or_else(|| DbError::Execution("active tx must exist at depth > 0".into()))?;
        if tx.isolation_level == IsolationLevel::Snapshot
            && !tx.write_set.is_empty()
            && tx.snapshot_ts != self.commit_ts
        {
            self.active = Some(tx);
            self.depth = 1;
            return Err(DbError::Execution(
                "snapshot write conflict detected during COMMIT".into(),
            ));
        }
        self.commit_ts += 1;
        self.xact_state = 0;
        Ok(())
    }

    /// P1 #17: Rollback restores workspace from savepoint snapshots (for savepoint rollback)
    /// or returns the begin-time extra snapshot for full rollback restoration.
    pub fn rollback(
        &mut self,
        savepoint: Option<String>,
        catalog: &mut C,
        storage: &mut S,
        extra: &mut X,
    ) -> Result<bool, DbError> {
        let Some(tx) = self.active.as_mut() else {
            return Err(DbError::Execution(
                "ROLLBACK without active transaction".into(),
            ));
        };

        if let Some(sp_name) = savepoint {
            // Savepoint rollback: restore from snapshot
            let Some(pos) = tx
                .savepoints
                .iter()
                .rposition(|sp| sp.name.eq_ignore_ascii_case(&sp_name))
            else {
                return Err(DbError::Execution(format!(
                    "savepoint '{}' not found",
                    sp_name
                )));
            };
            let snapshot = tx.savepoints[pos].clone();
            *catalog = snapshot.catalog_snapshot;
            *storage = snapshot.storage_snapshot;
            *extra = snapshot.extra_snapshot;
            tx.write_set.truncate(snapshot.write_intent_len);
            tx.savepoints.truncate(pos + 1);
            Ok(false) // partial rollback, workspace stays
        } else {
            // Full rollback: restore to begin-time state
            *extra = tx.begin_extra.clone();
            tx.savepoints.clear();
            tx.write_set.clear();
            Ok(true) // full rollback, workspace should be discarded
        }
    }

    /// P1 #17: Savepoint records workspace catalog/storage snapshots.
    pub fn save(
        &mut self,
        name: String,
        catalog: &C,
        storage: &S,
        extra: &X,
    ) -> Result<(), DbError> {
        let Some(tx) = self.active.as_mut() else {
            return Err(DbError::Execution(
                "SAVE TRANSACTION without active transaction".into(),
            ));
        };
        tx.savepoints.push(Savepoint {
            name,
            catalog_snapshot: catalog.clone(),
            storage_snapshot: storage.clone(),
            extra_snapshot: extra.clone(),
            write_intent_len: tx.write_set.len(),
        });
        Ok(())
    }

    pub fn set_isolation_level(&mut self, isolation_level: IsolationLevel) -> Result<(), DbError> {
        if let Some(tx) = self.active.as_mut() {
            tx.isolation_level = isolation_level;
        }
        self.session_isolation_level = isolation_level;
        Ok(())
    }

    pub fn register_write_intent(&mut self, kind: WriteIntentKind, table: Option<String>) {
        if let Some(tx) = self.active.as_mut() {
            let table_name = table.clone().unwrap_or_else(|| "__global__".to_string());
            tx.lock_manager.acquire_write(&table_name);
            tx.write_set.push(WriteIntent { kind, table });
        }
    }
}
