use std::collections::HashSet;

use crate::ast::IsolationLevel;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;

#[derive(Debug, Clone)]
pub struct Savepoint<C, S, X> {
    pub name: String,
    pub catalog_snapshot: C,
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
pub struct LockManager {
    read_locks: HashSet<String>,
    write_locks: HashSet<String>,
}

impl LockManager {
    pub fn acquire_read(&mut self, resource: &str) {
        self.read_locks.insert(resource.to_uppercase());
    }

    pub fn acquire_write(&mut self, resource: &str) {
        self.write_locks.insert(resource.to_uppercase());
    }

    pub fn clear(&mut self) {
        self.read_locks.clear();
        self.write_locks.clear();
    }
}

#[derive(Debug, Clone)]
pub struct TxState<C, S, X> {
    pub isolation_level: IsolationLevel,
    pub begin_catalog: C,
    pub begin_storage: S,
    pub begin_extra: X,
    pub savepoints: Vec<Savepoint<C, S, X>>,
    pub lock_manager: LockManager,
    pub write_set: Vec<WriteIntent>,
    pub snapshot_ts: u64,
}

impl<C, S, X> TxState<C, S, X> {
    pub fn new(
        isolation_level: IsolationLevel,
        begin_catalog: C,
        begin_storage: S,
        begin_extra: X,
        snapshot_ts: u64,
    ) -> Self {
        Self {
            isolation_level,
            begin_catalog,
            begin_storage,
            begin_extra,
            savepoints: vec![],
            lock_manager: LockManager::default(),
            write_set: vec![],
            snapshot_ts,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionManager<C, S, X> {
    pub active: Option<TxState<C, S, X>>,
    pub session_isolation_level: IsolationLevel,
    pub commit_ts: u64,
    pub depth: u32,
}

impl<C, S, X> Default for TransactionManager<C, S, X> {
    fn default() -> Self {
        Self {
            active: None,
            session_isolation_level: IsolationLevel::default(),
            commit_ts: 0,
            depth: 0,
        }
    }
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted
    }
}

impl<C, S, X> TransactionManager<C, S, X>
where
    C: Catalog + Clone,
    S: Storage + Clone,
    X: Clone,
{
    pub fn begin(
        &mut self,
        catalog: &C,
        storage: &S,
        extra: &X,
        explicit_name: Option<String>,
    ) -> Result<Option<String>, DbError> {
        if self.depth == 0 {
            let tx = TxState::new(
                self.session_isolation_level,
                catalog.clone(),
                storage.clone(),
                extra.clone(),
                self.commit_ts,
            );
            self.active = Some(tx);
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
        self.depth -= 1;
        if self.depth > 0 {
            return Ok(());
        }
        let tx = self.active.take().expect("active tx must exist at depth > 0");
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
        Ok(())
    }

    pub fn rollback(
        &mut self,
        savepoint: Option<String>,
        catalog: &mut C,
        storage: &mut S,
        extra: &mut X,
    ) -> Result<(), DbError> {
        let Some(tx) = self.active.as_mut() else {
            return Err(DbError::Execution(
                "ROLLBACK without active transaction".into(),
            ));
        };

        if let Some(sp_name) = savepoint {
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
            // tx.lock_manager.clear(); // Removed as it was clearing all locks, which is incorrect for savepoints
            return Ok(());
        }

        *catalog = tx.begin_catalog.clone();
        *storage = tx.begin_storage.clone();
        *extra = tx.begin_extra.clone();
        self.active = None;
        self.depth = 0;
        Ok(())
    }

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
