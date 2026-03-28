use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::catalog::{Catalog, CatalogImpl};
use crate::error::DbError;
use crate::storage::Storage;

use super::super::durability::{DurabilitySink, RecoveryCheckpoint};
use super::super::locks::{LockTable, SessionId};
use super::super::session::{SessionRuntime, SharedState};
use super::CheckpointManager;

#[derive(Clone)]
pub struct DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub inner: Arc<Mutex<SharedState<C, S>>>,
}

impl DatabaseInner<CatalogImpl, crate::storage::RedbStorage> {
    pub fn new_persistent(path: &std::path::Path) -> Result<Self, DbError> {
        let storage = crate::storage::RedbStorage::new(path.join("data.redb"))?;
        let durability = super::super::durability::FileDurability::new(path.join("catalog.json"))?;

        let state = if let Some(checkpoint) = durability.latest_checkpoint() {
            SharedState::from_checkpoint(checkpoint, Box::new(durability), storage)
        } else {
            let mut state = SharedState::with_initial(CatalogImpl::new(), storage);
            state.durability = Box::new(durability);
            state
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(state)),
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
            inner: Arc::new(Mutex::new(state)),
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
            state.durability = durability;
            state
        };
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }

    pub fn from_checkpoint(payload: &str) -> Result<Self, DbError> {
        let checkpoint = RecoveryCheckpoint::<C>::from_json(payload)?;
        let state = SharedState::from_checkpoint_internal(checkpoint);
        Ok(Self {
            inner: Arc::new(Mutex::new(state)),
        })
    }

    pub fn reset(&self) {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        let mut catalog = C::default();
        let _ = catalog.create_schema("dbo");
        guard.catalog = catalog;
        guard.storage = S::default();
        guard.commit_ts = 0;
        guard.table_versions.clear();
        guard.table_locks.clear();
        for session in guard.sessions.values_mut() {
            session.reset();
        }
    }

    pub fn set_durability_sink(
        &self,
        durability: Box<dyn DurabilitySink<C>>,
    ) {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.durability = durability;
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
            catalog: checkpoint.catalog,
            storage,
            commit_ts: checkpoint.commit_ts,
            table_versions: checkpoint.table_versions,
            table_locks: LockTable::new(),
            durability,
            sessions: HashMap::new(),
            next_session_id: 1,
        }
    }

    pub fn from_checkpoint_internal(checkpoint: RecoveryCheckpoint<C>) -> Self {
        let mut storage = S::default();
        let _ = storage.restore_from_checkpoint(checkpoint.storage_data);
        Self {
            catalog: checkpoint.catalog,
            storage,
            commit_ts: checkpoint.commit_ts,
            table_versions: checkpoint.table_versions,
            table_locks: LockTable::new(),
            durability: Box::new(super::super::durability::NoopDurability::default()),
            sessions: HashMap::new(),
            next_session_id: 1,
        }
    }

    pub fn apply_checkpoint(&mut self, checkpoint: RecoveryCheckpoint<C>) {
        self.catalog = checkpoint.catalog;
        let _ = self.storage.restore_from_checkpoint(checkpoint.storage_data);
        self.commit_ts = checkpoint.commit_ts;
        self.table_versions = checkpoint.table_versions;
        self.table_locks.clear();
        for session in self.sessions.values_mut() {
            session.reset();
        }
    }

    pub fn to_checkpoint(&self) -> RecoveryCheckpoint<C> {
        RecoveryCheckpoint {
            catalog: self.catalog.clone(),
            storage_data: self.storage.get_checkpoint_data(),
            commit_ts: self.commit_ts,
            table_versions: self.table_versions.clone(),
        }
    }
}

impl<C, S> CheckpointManager for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn export_checkpoint(&self) -> Result<String, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        guard.to_checkpoint().to_json()
    }

    fn import_checkpoint(&self, payload: &str) -> Result<(), DbError> {
        let checkpoint = RecoveryCheckpoint::<C>::from_json(payload)?;
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.apply_checkpoint(checkpoint);
        Ok(())
    }
}

impl<C, S> super::super::session::SessionManager for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn create_session(&self) -> SessionId {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        let id = guard.next_session_id;
        guard.next_session_id += 1;
        guard.sessions.insert(id, SessionRuntime::new());
        id
    }

    fn close_session(&self, session_id: SessionId) -> Result<(), DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.table_locks.release_all_for_session(session_id);
        let removed = guard.sessions.remove(&session_id);
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
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |_, session| {
            session.journal = journal;
            Ok(())
        })
    }
}
