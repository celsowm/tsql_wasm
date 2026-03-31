use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ast::{IsolationLevel, Statement};
use crate::catalog::{Catalog, CatalogImpl};
use crate::error::DbError;
use crate::parser::parse_sql_with_quoted_ident;
use crate::storage::{InMemoryStorage, Storage};

use super::super::durability::DurabilitySink;
use super::super::journal::Journal;
use super::super::locks::SessionId;
use super::super::result::QueryResult;
use super::super::session::SessionManager;
use super::super::tooling::{CompatibilityReport, ExecutionTrace, ExplainPlan, SessionOptions};
use super::persistence::DatabaseInner;
use super::{CheckpointManager, SqlAnalyzer, StatementExecutor};

pub struct EngineInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub db: DatabaseInner<C, S>,
    pub default_session: SessionId,
}

impl<C, S> std::fmt::Debug for EngineInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("default_session", &self.default_session)
            .finish()
    }
}

impl Default for EngineInner<CatalogImpl, InMemoryStorage> {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineInner<CatalogImpl, InMemoryStorage> {
    pub fn new() -> Self {
        let db = DatabaseInner::new();
        let default_session = db.create_session();
        Self {
            db,
            default_session,
        }
    }

    pub fn from_checkpoint(payload: &str) -> Result<Self, DbError> {
        let db = DatabaseInner::from_checkpoint(payload)?;
        let default_session = db.create_session();
        Ok(Self {
            db,
            default_session,
        })
    }

    pub fn reset(&self) {
        self.db.reset();
    }

    pub fn new_with_durability(
        durability: Box<dyn DurabilitySink<CatalogImpl>>,
    ) -> Self {
        let db = DatabaseInner::new_with_durability(durability);
        let default_session = db.create_session();
        Self {
            db,
            default_session,
        }
    }

    pub fn database(&self) -> DatabaseInner<CatalogImpl, InMemoryStorage> {
        self.db.clone()
    }

    pub fn execute(&self, stmt: Statement) -> Result<Option<QueryResult>, DbError> {
        StatementExecutor::execute_session(&self.db, self.default_session, stmt)
    }

    pub fn exec(&self, sql: &str) -> Result<(), DbError> {
        let quoted_ident = self.session_options().quoted_identifier;
        let stmt = parse_sql_with_quoted_ident(sql, quoted_ident)?;
        let res = StatementExecutor::execute_session(&self.db, self.default_session, stmt)?;
        if res.is_some() {
            return Err(DbError::Execution("exec() received a query statement; use query()".into()));
        }
        Ok(())
    }

    pub fn query(&self, sql: &str) -> Result<QueryResult, DbError> {
        let quoted_ident = self.session_options().quoted_identifier;
        let stmt = parse_sql_with_quoted_ident(sql, quoted_ident)?;
        let res = StatementExecutor::execute_session(&self.db, self.default_session, stmt)?;
        res.ok_or_else(|| DbError::Execution("query() expected a result set".into()))
    }

    pub fn execute_batch(
        &self,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        StatementExecutor::execute_session_batch(&self.db, self.default_session, stmts)
    }

    pub fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        StatementExecutor::execute_session_batch_sql(&self.db, session_id, sql)
    }

    pub fn set_journal(&self, journal: Box<dyn Journal>) {
        let _ = self.db.set_session_journal(self.default_session, journal);
    }

    pub fn set_durability_sink(
        &self,
        durability: Box<dyn DurabilitySink<CatalogImpl>>,
    ) {
        self.db.set_durability_sink(durability);
    }

    pub fn export_checkpoint(&self) -> Result<String, DbError> {
        CheckpointManager::export_checkpoint(&self.db)
    }

    pub fn import_checkpoint(&self, payload: &str) -> Result<(), DbError> {
        CheckpointManager::import_checkpoint(&self.db, payload)
    }

    pub fn session_isolation_level(&self) -> IsolationLevel {
        SqlAnalyzer::session_isolation_level(&self.db, self.default_session)
            .unwrap_or(IsolationLevel::ReadCommitted)
    }

    pub fn transaction_is_active(&self) -> bool {
        SqlAnalyzer::transaction_is_active(&self.db, self.default_session).unwrap_or(false)
    }

    pub fn session_options(&self) -> SessionOptions {
        SqlAnalyzer::session_options(&self.db, self.default_session).unwrap_or_default()
    }

    pub fn analyze_sql_batch(&self, sql: &str) -> CompatibilityReport {
        SqlAnalyzer::analyze_sql_batch(&self.db, sql)
    }

    pub fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError> {
        SqlAnalyzer::explain_sql(&self.db, sql)
    }

    pub fn trace_execute_sql(&self, sql: &str) -> Result<ExecutionTrace, DbError> {
        SqlAnalyzer::trace_execute_session_sql(&self.db, self.default_session, sql)
    }

    pub fn print_output(&self) -> Vec<String> {
        let session_mutex = self.db.inner.sessions.get(&self.default_session);
        session_mutex.map(|s| s.lock().diagnostics.print_output.clone()).unwrap_or_default()
    }
}

impl<C, S> EngineInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub fn create_session(&self) -> SessionId {
        self.db.create_session()
    }

    pub fn close_session(&self, session_id: SessionId) -> Result<(), DbError> {
        self.db.close_session(session_id)
    }
}
