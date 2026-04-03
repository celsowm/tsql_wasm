pub(crate) mod analyzer;
pub(crate) mod execution;
pub(crate) mod dispatch;
pub(crate) mod engine;
pub(crate) mod persistence;

use crate::ast::{IsolationLevel, Statement};
use crate::catalog::CatalogImpl;
use crate::error::DbError;
use crate::storage::InMemoryStorage;

use super::locks::SessionId;
use super::result::QueryResult;
use super::tooling::{CompatibilityReport, ExecutionTrace, ExplainPlan, SessionOptions};

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
}

pub trait SqlAnalyzer {
    fn analyze_sql_batch(&self, sql: &str) -> CompatibilityReport;
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

pub use persistence::DatabaseInner;
pub use engine::EngineInner;

pub type Database = persistence::DatabaseInner<CatalogImpl, InMemoryStorage>;
pub type PersistentDatabase = persistence::DatabaseInner<CatalogImpl, crate::storage::RedbStorage>;

pub type Engine = engine::EngineInner<CatalogImpl, InMemoryStorage>;
pub type PersistentEngine = engine::EngineInner<CatalogImpl, crate::storage::RedbStorage>;
