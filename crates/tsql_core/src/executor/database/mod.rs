pub(crate) mod analyzer;
pub(crate) mod dispatch;
pub(crate) mod engine;
pub(crate) mod execution;
pub(crate) mod persistence;

use crate::ast::{IsolationLevel, Statement};
use crate::catalog::CatalogImpl;
use crate::error::DbError;
use crate::storage::InMemoryStorage;

use super::result::QueryResult;
use super::session::SharedState;
use super::tooling::{ExecutionTrace, ExplainPlan, SessionOptions};
use crate::executor::locks::SessionId;
use std::sync::Arc;

pub trait CheckpointManager {
    fn export_checkpoint(&self) -> Result<String, DbError>;
    fn import_checkpoint(&self, payload: &str) -> Result<(), DbError>;
}

pub trait StatementExecutor {
    fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError>;
    fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError>;
    fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError>;
    fn execute_session_batch_sql_multi(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Vec<Option<QueryResult>>, DbError>;
    fn set_session_metadata(
        &self,
        session_id: SessionId,
        user: Option<String>,
        app_name: Option<String>,
        host_name: Option<String>,
    ) -> Result<(), DbError>;
}

pub trait SqlAnalyzer {
    fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError>;
    fn trace_execute_session_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<ExecutionTrace, DbError>;
    fn session_isolation_level(&self, session_id: SessionId) -> Result<IsolationLevel, DbError>;
    fn transaction_is_active(&self, session_id: SessionId) -> Result<bool, DbError>;
    fn session_options(&self, session_id: SessionId) -> Result<SessionOptions, DbError>;
}

pub trait RandomSeed {
    fn set_session_seed(&self, session_id: SessionId, seed: u64) -> Result<(), DbError>;
}

pub struct StatementExecutorService<C, S> {
    pub(crate) state: Arc<SharedState<C, S>>,
}

pub struct CheckpointManagerService<C, S> {
    pub(crate) state: Arc<SharedState<C, S>>,
}

pub struct SqlAnalyzerService<C, S> {
    pub(crate) state: Arc<SharedState<C, S>>,
}

pub struct SessionManagerService<C, S> {
    pub(crate) state: Arc<SharedState<C, S>>,
}

pub use engine::EngineInner;
pub use persistence::DatabaseInner;

/// Opaque wrapper around `DatabaseInner` with in-memory storage.
/// Hides the concrete storage backend from public API consumers.
pub struct Database(pub(crate) DatabaseInner<CatalogImpl, InMemoryStorage>);

impl std::ops::Deref for Database {
    type Target = DatabaseInner<CatalogImpl, InMemoryStorage>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl std::ops::DerefMut for Database {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}
impl Database {
    pub fn new() -> Self { Self(DatabaseInner::new()) }
    pub fn from_checkpoint(payload: &str) -> Result<Self, DbError> {
        DatabaseInner::from_checkpoint(payload).map(Self)
    }
}
impl Default for Database {
    fn default() -> Self { Self::new() }
}

/// Opaque wrapper around `DatabaseInner` with persistent (redb) storage.
pub struct PersistentDatabase(pub(crate) DatabaseInner<CatalogImpl, crate::storage::RedbStorage>);

impl std::ops::Deref for PersistentDatabase {
    type Target = DatabaseInner<CatalogImpl, crate::storage::RedbStorage>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl std::ops::DerefMut for PersistentDatabase {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}
impl PersistentDatabase {
    pub fn new_persistent(path: &std::path::Path) -> Result<Self, DbError> {
        DatabaseInner::new_persistent(path).map(Self)
    }
}

/// Opaque wrapper around `EngineInner` with in-memory storage.
pub struct Engine(pub(crate) EngineInner<CatalogImpl, InMemoryStorage>);

impl std::ops::Deref for Engine {
    type Target = EngineInner<CatalogImpl, InMemoryStorage>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl std::ops::DerefMut for Engine {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}
impl Engine {
    pub fn new() -> Self { Self(EngineInner::new()) }
}
impl Default for Engine {
    fn default() -> Self { Self::new() }
}

/// Opaque wrapper around `EngineInner` with persistent (redb) storage.
pub struct PersistentEngine(pub(crate) EngineInner<CatalogImpl, crate::storage::RedbStorage>);

impl std::ops::Deref for PersistentEngine {
    type Target = EngineInner<CatalogImpl, crate::storage::RedbStorage>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl std::ops::DerefMut for PersistentEngine {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

// Delegate public traits for opaque Database wrapper so callers can use
// `SessionManager::method(&db, …)` etc. without knowing the inner type.
macro_rules! delegate_db_traits {
    ($wrapper:ty) => {
        impl super::session::SessionManager for $wrapper {
            fn create_session(&self) -> SessionId { self.0.create_session() }
            fn reset_session(&self, sid: SessionId) -> Result<(), DbError> { self.0.reset_session(sid) }
            fn close_session(&self, sid: SessionId) -> Result<(), DbError> { self.0.close_session(sid) }
            fn set_session_journal(&self, sid: SessionId, j: Box<dyn super::journal::Journal>) -> Result<(), DbError> { self.0.set_session_journal(sid, j) }
        }
        impl StatementExecutor for $wrapper {
            fn execute_session(&self, sid: SessionId, stmt: Statement) -> Result<Option<QueryResult>, DbError> { self.0.execute_session(sid, stmt) }
            fn execute_session_batch(&self, sid: SessionId, stmts: Vec<Statement>) -> Result<Option<QueryResult>, DbError> { self.0.execute_session_batch(sid, stmts) }
            fn execute_session_batch_sql(&self, sid: SessionId, sql: &str) -> Result<Option<QueryResult>, DbError> { self.0.execute_session_batch_sql(sid, sql) }
            fn execute_session_batch_sql_multi(&self, sid: SessionId, sql: &str) -> Result<Vec<Option<QueryResult>>, DbError> { self.0.execute_session_batch_sql_multi(sid, sql) }
            fn set_session_metadata(&self, sid: SessionId, user: Option<String>, app: Option<String>, host: Option<String>) -> Result<(), DbError> { self.0.set_session_metadata(sid, user, app, host) }
        }
        impl SqlAnalyzer for $wrapper {
            fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError> { self.0.explain_sql(sql) }
            fn trace_execute_session_sql(&self, sid: SessionId, sql: &str) -> Result<ExecutionTrace, DbError> { self.0.trace_execute_session_sql(sid, sql) }
            fn session_isolation_level(&self, sid: SessionId) -> Result<IsolationLevel, DbError> { self.0.session_isolation_level(sid) }
            fn transaction_is_active(&self, sid: SessionId) -> Result<bool, DbError> { self.0.transaction_is_active(sid) }
            fn session_options(&self, sid: SessionId) -> Result<SessionOptions, DbError> { self.0.session_options(sid) }
        }
        impl CheckpointManager for $wrapper {
            fn export_checkpoint(&self) -> Result<String, DbError> { self.0.export_checkpoint() }
            fn import_checkpoint(&self, payload: &str) -> Result<(), DbError> { self.0.import_checkpoint(payload) }
        }
        impl RandomSeed for $wrapper {
            fn set_session_seed(&self, sid: SessionId, seed: u64) -> Result<(), DbError> { self.0.set_session_seed(sid, seed) }
        }
    };
}

delegate_db_traits!(Database);
delegate_db_traits!(PersistentDatabase);
