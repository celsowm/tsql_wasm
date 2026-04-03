use std::sync::Arc;
use std::collections::HashSet;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::catalog::{Catalog, CatalogImpl};
use crate::error::DbError;
use crate::storage::Storage;

use super::super::durability::{DurabilitySink, RecoveryCheckpoint};
use super::super::locks::SessionId;
use super::super::session::{SessionRuntime, SharedState, SharedStorage};
use super::CheckpointManager;

#[derive(Clone)]
pub struct DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub inner: Arc<SharedState<C, S>>,
}

impl DatabaseInner<CatalogImpl, crate::storage::RedbStorage> {
    pub fn new_persistent(path: &std::path::Path) -> Result<Self, DbError> {
        let storage = crate::storage::RedbStorage::new(path.join("data.redb"))?;
        let durability = super::super::durability::FileDurability::new(path.join("catalog.json"))?;

        let state = if let Some(checkpoint) = durability.latest_checkpoint() {
            SharedState::from_checkpoint(checkpoint, Box::new(durability), storage)
        } else {
            let mut state = SharedState::with_initial(CatalogImpl::new(), storage);
            *state.durability.get_mut() = Box::new(durability);
            state
        };
        Ok(Self {
            inner: Arc::new(state),
        })
    }
}

impl<C, S> DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub fn new() -> Self {
        let mut catalog = C::default();
        let _ = catalog.create_schema("dbo");
        let state = SharedState::with_initial(catalog, S::default());
        Self {
            inner: Arc::new(state),
        }
    }

    pub fn new_with_durability(
        durability: Box<dyn DurabilitySink<C>>,
    ) -> Self {
        let state = if let Some(checkpoint) = durability.latest_checkpoint() {
            SharedState::from_checkpoint(checkpoint, durability, S::default())
        } else {
            let mut catalog = C::default();
            let _ = catalog.create_schema("dbo");
            let mut state = SharedState::with_initial(catalog, S::default());
            *state.durability.get_mut() = durability;
            state
        };
        Self {
            inner: Arc::new(state),
        }
    }

    pub fn from_checkpoint(payload: &str) -> Result<Self, DbError> {
        let checkpoint = RecoveryCheckpoint::<C>::from_json(payload)?;
        let state = SharedState::from_checkpoint_internal(checkpoint);
        Ok(Self {
            inner: Arc::new(state),
        })
    }

    pub fn reset(&self) {
        let mut storage = self.inner.storage.write();
        let mut catalog = C::default();
        let _ = catalog.create_schema("dbo");
        storage.catalog = catalog;
        storage.storage = S::default();
        storage.commit_ts = 0;
        storage.table_versions.clear();
        self.inner.table_locks.lock().clear();
        for mut session_lock in self.inner.sessions.iter_mut() {
            session_lock.value_mut().get_mut().reset();
        }
    }

    pub fn set_durability_sink(
        &self,
        durability: Box<dyn DurabilitySink<C>>,
    ) {
        let mut guard = self.inner.durability.lock();
        *guard = durability;
    }
}

impl<C, S> SharedState<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub fn from_checkpoint(
        checkpoint: RecoveryCheckpoint<C>,
        durability: Box<dyn DurabilitySink<C>>,
        mut storage: S,
    ) -> Self {
        let _ = storage.restore_from_checkpoint(checkpoint.storage_data);
        Self {
            storage: parking_lot::RwLock::new(SharedStorage {
                catalog: checkpoint.catalog,
                storage,
                commit_ts: checkpoint.commit_ts,
                table_versions: checkpoint.table_versions,
            }),
            table_locks: parking_lot::Mutex::new(super::super::locks::LockTable::new()),
            durability: parking_lot::Mutex::new(durability),
            sessions: dashmap::DashMap::new(),
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            dirty_buffer: std::sync::Arc::new(parking_lot::Mutex::new(super::super::dirty_buffer::DirtyBuffer::new())),
        }
    }

    pub fn from_checkpoint_internal(checkpoint: RecoveryCheckpoint<C>) -> Self {
        let mut storage = S::default();
        let _ = storage.restore_from_checkpoint(checkpoint.storage_data);
        Self {
            storage: parking_lot::RwLock::new(SharedStorage {
                catalog: checkpoint.catalog,
                storage,
                commit_ts: checkpoint.commit_ts,
                table_versions: checkpoint.table_versions,
            }),
            table_locks: parking_lot::Mutex::new(super::super::locks::LockTable::new()),
            durability: parking_lot::Mutex::new(Box::new(super::super::durability::NoopDurability::default())),
            sessions: dashmap::DashMap::new(),
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            dirty_buffer: std::sync::Arc::new(parking_lot::Mutex::new(super::super::dirty_buffer::DirtyBuffer::new())),
        }
    }

    pub fn apply_checkpoint(&self, checkpoint: RecoveryCheckpoint<C>) {
        let mut storage = self.storage.write();
        storage.catalog = checkpoint.catalog;
        let _ = storage.storage.restore_from_checkpoint(checkpoint.storage_data);
        storage.commit_ts = checkpoint.commit_ts;
        storage.table_versions = checkpoint.table_versions;
        self.table_locks.lock().clear();
        for mut session_lock in self.sessions.iter_mut() {
            session_lock.value_mut().get_mut().reset();
        }
    }

    pub fn to_checkpoint(&self) -> RecoveryCheckpoint<C> {
        let storage = self.storage.read();
        self.to_checkpoint_internal(&storage)
    }

    pub fn to_checkpoint_internal(&self, storage: &SharedStorage<C, S>) -> RecoveryCheckpoint<C> {
        RecoveryCheckpoint {
            catalog: storage.catalog.clone(),
            storage_data: storage.storage.get_checkpoint_data(),
            commit_ts: storage.commit_ts,
            table_versions: storage.table_versions.clone(),
        }
    }
}

impl<C, S> CheckpointManager for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn export_checkpoint(&self) -> Result<String, DbError> {
        self.inner.to_checkpoint().to_json()
    }

    fn import_checkpoint(&self, payload: &str) -> Result<(), DbError> {
        let checkpoint = RecoveryCheckpoint::<C>::from_json(payload)?;
        self.inner.apply_checkpoint(checkpoint);
        Ok(())
    }
}

impl<C, S> super::super::session::SessionManager for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn create_session(&self) -> SessionId {
        let id = self.inner.next_session_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.sessions.insert(id, parking_lot::Mutex::new(SessionRuntime::new()));
        id
    }

    fn reset_session(&self, session_id: SessionId) -> Result<(), DbError> {
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        let mut physical_tables = HashSet::new();
        for table in session.tables.temp_map.values() {
            physical_tables.insert(table.clone());
        }
        for table in session.tables.var_map.values() {
            physical_tables.insert(table.clone());
        }
        session.reset();
        drop(session);

        self.inner.table_locks.lock().release_all_for_session(session_id);

        if !physical_tables.is_empty() {
            let mut storage = self.inner.storage.write();
            for table_name in physical_tables {
                if let Some(table) = storage.catalog.find_table("dbo", &table_name).cloned() {
                    let _ = storage.catalog.drop_table("dbo", &table_name);
                    storage.storage.remove_table(table.id);
                    storage
                        .table_versions
                        .remove(&format!("DBO.{}", table_name.to_uppercase()));
                }
            }
        }
        Ok(())
    }

    fn close_session(&self, session_id: SessionId) -> Result<(), DbError> {
        self.inner.table_locks.lock().release_all_for_session(session_id);
        let removed = self.inner.sessions.remove(&session_id);
        if removed.is_none() {
            return Err(DbError::Execution(format!(
                "session {} not found",
                session_id
            )));
        }
        Ok(())
    }

    fn set_session_journal(
        &self,
        session_id: SessionId,
        journal: Box<dyn super::super::journal::Journal>,
    ) -> Result<(), DbError> {
        let session_mutex = self.inner.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        session.journal = journal;
        Ok(())
    }
}
