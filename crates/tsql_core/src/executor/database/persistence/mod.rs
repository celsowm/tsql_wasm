use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::catalog::{Catalog, CatalogImpl};
use crate::error::DbError;
use crate::storage::Storage;

use super::super::durability::{DurabilitySink, RecoveryCheckpoint};
use super::super::session::{SharedState, SharedStorage};

mod checkpoint;
mod session;

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
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
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

    pub fn executor(&self) -> super::StatementExecutorService<C, S> {
        super::StatementExecutorService { state: self.inner.clone() }
    }

    pub fn checkpoint_manager(&self) -> super::CheckpointManagerService<C, S> {
        super::CheckpointManagerService { state: self.inner.clone() }
    }

    pub fn analyzer(&self) -> super::SqlAnalyzerService<C, S> {
        super::SqlAnalyzerService { state: self.inner.clone() }
    }

    pub fn session_manager(&self) -> super::SessionManagerService<C, S> {
        super::SessionManagerService { state: self.inner.clone() }
    }
}


impl<C, S> SharedState<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
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
