use std::collections::HashSet;

use crate::ast::IsolationLevel;
use crate::catalog::CatalogImpl;
use crate::error::DbError;
use crate::storage::InMemoryStorage;

#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,
    pub catalog_snapshot: CatalogImpl,
    pub storage_snapshot: InMemoryStorage,
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
pub struct TxState {
    pub isolation_level: IsolationLevel,
    pub begin_catalog: CatalogImpl,
    pub begin_storage: InMemoryStorage,
    pub savepoints: Vec<Savepoint>,
    pub lock_manager: LockManager,
    pub write_set: Vec<WriteIntent>,
    pub snapshot_ts: u64,
}

impl TxState {
    pub fn new(
        isolation_level: IsolationLevel,
        begin_catalog: CatalogImpl,
        begin_storage: InMemoryStorage,
        snapshot_ts: u64,
    ) -> Self {
        Self {
            isolation_level,
            begin_catalog,
            begin_storage,
            savepoints: vec![],
            lock_manager: LockManager::default(),
            write_set: vec![],
            snapshot_ts,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct TransactionManager {
    pub active: Option<TxState>,
    pub session_isolation_level: IsolationLevel,
    pub commit_ts: u64,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted
    }
}

impl TransactionManager {
    pub fn begin(
        &mut self,
        catalog: &CatalogImpl,
        storage: &InMemoryStorage,
        explicit_name: Option<String>,
    ) -> Result<Option<String>, DbError> {
        if self.active.is_some() {
            return Err(DbError::Execution(
                "transaction already active; nested BEGIN TRANSACTION is not supported".into(),
            ));
        }
        let tx = TxState::new(
            self.session_isolation_level,
            catalog.clone(),
            storage.clone(),
            self.commit_ts,
        );
        self.active = Some(tx);
        Ok(explicit_name)
    }

    pub fn commit(&mut self) -> Result<(), DbError> {
        let Some(tx) = self.active.take() else {
            return Err(DbError::Execution(
                "COMMIT without active transaction".into(),
            ));
        };
        if tx.isolation_level == IsolationLevel::Snapshot
            && !tx.write_set.is_empty()
            && tx.snapshot_ts != self.commit_ts
        {
            self.active = Some(tx);
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
        catalog: &mut CatalogImpl,
        storage: &mut InMemoryStorage,
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
            tx.write_set.truncate(snapshot.write_intent_len);
            tx.savepoints.truncate(pos + 1);
            tx.lock_manager.clear();
            return Ok(());
        }

        *catalog = tx.begin_catalog.clone();
        *storage = tx.begin_storage.clone();
        self.active = None;
        Ok(())
    }

    pub fn save(
        &mut self,
        name: String,
        catalog: &CatalogImpl,
        storage: &InMemoryStorage,
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
