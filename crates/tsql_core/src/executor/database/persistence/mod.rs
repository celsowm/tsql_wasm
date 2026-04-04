use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ast::Statement;
use crate::catalog::{Catalog, CatalogImpl};
use crate::error::DbError;
use crate::executor::locks::SessionId;
use crate::executor::result::QueryResult;
use crate::executor::session::SessionManager as SessionManagerTrait;
use crate::executor::tooling::{ExecutionTrace, ExplainPlan, SessionOptions};
use crate::storage::Storage;

use super::super::durability::{DurabilitySink, RecoveryReader, RecoveryCheckpoint};
use super::super::session::{SharedState, SharedStorage};
use super::{
    CheckpointManager as CheckpointManagerTrait,
    RandomSeed as RandomSeedTrait,
    SqlAnalyzer as SqlAnalyzerTrait,
    StatementExecutor as StatementExecutorTrait,
};

mod checkpoint;
mod session;

/// Unified facade over focused service structs (`StatementExecutorService`,
/// `CheckpointManagerService`, `SqlAnalyzerService`, `SessionManagerService`).
///
/// Trait implementations (`StatementExecutor`, `CheckpointManager`, `SqlAnalyzer`,
/// `SessionManager`) delegate to the corresponding service obtained via `executor()`,
/// `checkpoint_manager()`, `analyzer()`, and `session_manager()`. This keeps each service's
/// internals isolated while exposing a single API surface to callers.
#[derive(Clone)]
pub struct DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub(crate) inner: Arc<SharedState<C, S>>,
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

    pub fn print_output(&self, session_id: SessionId) -> Vec<String> {
        self.inner.sessions.get(&session_id)
            .map(|s| s.lock().diagnostics.print_output.clone())
            .unwrap_or_default()
    }

    pub fn create_session(&self) -> SessionId {
        self.session_manager().create_session()
    }

    pub fn close_session(&self, session_id: SessionId) -> Result<(), DbError> {
        self.session_manager().close_session(session_id)
    }

    pub fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError> {
        self.executor().execute_session(session_id, stmt)
    }

    pub fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        self.executor().execute_session_batch(session_id, stmts)
    }

    pub fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        self.executor().execute_session_batch_sql(session_id, sql)
    }

    pub fn execute_session_batch_sql_multi(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Vec<Option<QueryResult>>, DbError> {
        self.executor().execute_session_batch_sql_multi(session_id, sql)
    }

    pub fn set_session_seed(&self, session_id: SessionId, seed: u64) -> Result<(), DbError> {
        self.analyzer().set_session_seed(session_id, seed)
    }

    pub fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError> {
        self.analyzer().explain_sql(sql)
    }

    pub fn trace_execute_session_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<ExecutionTrace, DbError> {
        self.analyzer().trace_execute_session_sql(session_id, sql)
    }

    pub fn session_options(&self, session_id: SessionId) -> Result<SessionOptions, DbError> {
        self.analyzer().session_options(session_id)
    }

    pub fn export_checkpoint(&self) -> Result<String, DbError> {
        self.checkpoint_manager().export_checkpoint()
    }

    pub fn import_checkpoint(&self, payload: &str) -> Result<(), DbError> {
        self.checkpoint_manager().import_checkpoint(payload)
    }
}

impl<C, S> SessionManagerTrait for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn create_session(&self) -> SessionId {
        self.session_manager().create_session()
    }

    fn reset_session(&self, session_id: SessionId) -> Result<(), DbError> {
        self.session_manager().reset_session(session_id)
    }

    fn close_session(&self, session_id: SessionId) -> Result<(), DbError> {
        self.session_manager().close_session(session_id)
    }

    fn set_session_journal(
        &self,
        session_id: SessionId,
        journal: Box<dyn crate::executor::journal::Journal>,
    ) -> Result<(), DbError> {
        self.session_manager().set_session_journal(session_id, journal)
    }
}

impl<C, S> StatementExecutorTrait for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError> {
        self.executor().execute_session(session_id, stmt)
    }

    fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        self.executor().execute_session_batch(session_id, stmts)
    }

    fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        self.executor().execute_session_batch_sql(session_id, sql)
    }

    fn execute_session_batch_sql_multi(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Vec<Option<QueryResult>>, DbError> {
        self.executor().execute_session_batch_sql_multi(session_id, sql)
    }

    fn set_session_metadata(
        &self,
        session_id: SessionId,
        user: Option<String>,
        app_name: Option<String>,
        host_name: Option<String>,
    ) -> Result<(), DbError> {
        self.executor()
            .set_session_metadata(session_id, user, app_name, host_name)
    }
}

impl<C, S> SqlAnalyzerTrait for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError> {
        self.analyzer().explain_sql(sql)
    }

    fn trace_execute_session_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<ExecutionTrace, DbError> {
        self.analyzer().trace_execute_session_sql(session_id, sql)
    }

    fn session_isolation_level(&self, session_id: SessionId) -> Result<crate::ast::IsolationLevel, DbError> {
        self.analyzer().session_isolation_level(session_id)
    }

    fn transaction_is_active(&self, session_id: SessionId) -> Result<bool, DbError> {
        self.analyzer().transaction_is_active(session_id)
    }

    fn session_options(&self, session_id: SessionId) -> Result<SessionOptions, DbError> {
        self.analyzer().session_options(session_id)
    }
}

impl<C, S> RandomSeedTrait for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn set_session_seed(&self, session_id: SessionId, seed: u64) -> Result<(), DbError> {
        self.analyzer().set_session_seed(session_id, seed)
    }
}

impl<C, S> super::CheckpointManager for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + crate::storage::CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn export_checkpoint(&self) -> Result<String, DbError> {
        self.checkpoint_manager().export_checkpoint()
    }

    fn import_checkpoint(&self, payload: &str) -> Result<(), DbError> {
        self.checkpoint_manager().import_checkpoint(payload)
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
