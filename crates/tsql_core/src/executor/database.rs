use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::ast::{DropTableStmt, IsolationLevel, ObjectName, Statement};
use crate::catalog::{Catalog, CatalogImpl};
use crate::error::DbError;
use crate::parser::{parse_batch, parse_sql};
use crate::storage::{InMemoryStorage, Storage};

use super::clock::Clock;
use super::context::ExecutionContext;
use super::durability::{DurabilitySink, RecoveryCheckpoint};
use super::journal::{Journal, JournalEvent};
use super::locks::{LockTable, SessionId, TxWorkspace};
use super::result::QueryResult;
use super::schema::SchemaExecutor;
use super::script::ScriptExecutor;
use super::session::{SessionManager, SessionRuntime, SharedState};
use super::table_util::{collect_write_tables, is_transaction_statement};
use super::tooling::{
    analyze_sql_batch, apply_set_option,
    collect_read_tables as collect_read_tables_tooling,
    collect_write_tables as collect_write_tables_tooling, explain_statement,
    split_sql_statements, statement_compat_warnings, CompatibilityReport, ExecutionTrace,
    ExplainPlan, SessionOptions, TraceStatementEvent,
};
use super::transaction::TransactionManager;
use super::transaction_exec;

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
    /// Execute a batch and return ALL result sets (one per SELECT/statement).
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

#[derive(Clone)]
pub struct DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub inner: Arc<Mutex<SharedState<C, S>>>,
}

pub type Database = DatabaseInner<CatalogImpl, InMemoryStorage>;

pub type Engine = EngineInner<CatalogImpl, InMemoryStorage>;

pub type PersistentDatabase = DatabaseInner<CatalogImpl, crate::storage::RedbStorage>;

pub type PersistentEngine = EngineInner<CatalogImpl, crate::storage::RedbStorage>;

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

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Self {
        let db = Database::new();
        let default_session = db.create_session();
        Self {
            db,
            default_session,
        }
    }

    pub fn from_checkpoint(payload: &str) -> Result<Self, DbError> {
        let db = Database::from_checkpoint(payload)?;
        let default_session = db.create_session();
        Ok(Self {
            db,
            default_session,
        })
    }

    pub fn reset(&mut self) {
        self.db.reset();
    }

    pub fn new_with_durability(
        durability: Box<dyn DurabilitySink<CatalogImpl>>,
    ) -> Self {
        let db = Database::new_with_durability(durability);
        let default_session = db.create_session();
        Self {
            db,
            default_session,
        }
    }

    pub fn database(&self) -> DatabaseInner<CatalogImpl, InMemoryStorage> {
        self.db.clone()
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<Option<QueryResult>, DbError> {
        StatementExecutor::execute_session(&self.db, self.default_session, stmt)
    }

    pub fn execute_batch(
        &mut self,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        StatementExecutor::execute_session_batch(&self.db, self.default_session, stmts)
    }

    pub fn set_journal(&mut self, journal: Box<dyn Journal>) {
        let _ = SessionManager::set_session_journal(&self.db, self.default_session, journal);
    }

    pub fn set_durability_sink(
        &mut self,
        durability: Box<dyn DurabilitySink<CatalogImpl>>,
    ) {
        self.db.set_durability_sink(durability);
    }

    pub fn export_checkpoint(&self) -> Result<String, DbError> {
        CheckpointManager::export_checkpoint(&self.db)
    }

    pub fn import_checkpoint(&mut self, payload: &str) -> Result<(), DbError> {
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
        let guard = self.db.inner.lock().unwrap();
        guard.sessions.get(&self.default_session).map(|s| s.print_output.clone()).unwrap_or_default()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    pub fn new() -> Self {
        let state = SharedState::with_initial(CatalogImpl::new(), InMemoryStorage::default());
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }
}

impl PersistentDatabase {
    pub fn new_persistent(path: &std::path::Path) -> Result<Self, DbError> {
        let storage = crate::storage::RedbStorage::new(path.join("data.redb"))?;
        let durability = super::durability::FileDurability::new(path.join("catalog.json"))?;

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
    pub fn new_with_durability(
        durability: Box<dyn DurabilitySink<C>>,
    ) -> Self {
        let state = if let Some(checkpoint) = durability.latest_checkpoint() {
            SharedState::from_checkpoint(checkpoint, durability, S::default())
        } else {
            let mut state = SharedState::with_initial(C::default(), S::default());
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
        guard.catalog = C::default();
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
            durability: Box::new(super::durability::NoopDurability::default()),
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

impl<C, S> SessionManager for DatabaseInner<C, S>
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
        journal: Box<dyn Journal>,
    ) -> Result<(), DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |_, session| {
            session.journal = journal;
            Ok(())
        })
    }
}

pub trait RandomSeed {
    fn set_session_seed(&self, session_id: SessionId, seed: u64) -> Result<(), DbError>;
}

impl<C, S> RandomSeed for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn set_session_seed(&self, session_id: SessionId, seed: u64) -> Result<(), DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |_, session| {
            session.random_state = seed;
            Ok(())
        })
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

impl<C, S> StatementExecutor for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_single_statement(state, session_id, session, stmt)
        })
    }

    fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_batch_statements(state, session_id, session, stmts)
        })
    }

    fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        let stmts = parse_batch(sql)?;
        self.execute_session_batch(session_id, stmts)
    }

    fn execute_session_batch_sql_multi(
        &self,
        session_id: SessionId,
        sql: &str) -> Result<Vec<Option<QueryResult>>, DbError> {
        let stmts = parse_batch(sql)?;
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_batch_statements_multi(state, session_id, session, stmts)
        })
    }
}

impl<C, S> SqlAnalyzer for DatabaseInner<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn session_isolation_level(&self, session_id: SessionId) -> Result<IsolationLevel, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.tx_manager.session_isolation_level)
    }

    fn transaction_is_active(&self, session_id: SessionId) -> Result<bool, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.tx_manager.active.is_some())
    }

    fn session_options(&self, session_id: SessionId) -> Result<SessionOptions, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.options.clone())
    }

    fn analyze_sql_batch(&self, sql: &str) -> CompatibilityReport {
        analyze_sql_batch(sql)
    }

    fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError> {
        let stmt = parse_sql(sql)?;
        Ok(explain_statement(&stmt))
    }

    fn trace_execute_session_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<ExecutionTrace, DbError> {
        let slices = split_sql_statements(sql);
        let mut events = Vec::with_capacity(slices.len());
        let mut stopped_on_error = false;

        for slice in slices {
            match parse_sql(&slice.sql) {
                Ok(stmt) => {
                    let mut warnings = statement_compat_warnings(&stmt);
                    let mut read_tables: Vec<String> =
                        collect_read_tables_tooling(&stmt).into_iter().collect();
                    let mut write_tables: Vec<String> =
                        collect_write_tables_tooling(&stmt).into_iter().collect();
                    read_tables.sort();
                    write_tables.sort();

                    match self.execute_session(session_id, stmt) {
                        Ok(result) => {
                            let options = self.session_options(session_id)?;
                            let row_count = if options.nocount {
                                None
                            } else {
                                result.as_ref().map(|r| r.rows.len())
                            };
                            events.push(TraceStatementEvent {
                                index: slice.index,
                                sql: slice.sql,
                                normalized_sql: slice.normalized_sql,
                                span: slice.span,
                                status: "ok".to_string(),
                                warnings: std::mem::take(&mut warnings),
                                error: None,
                                row_count,
                                read_tables,
                                write_tables,
                            });
                        }
                        Err(err) => {
                            events.push(TraceStatementEvent {
                                index: slice.index,
                                sql: slice.sql,
                                normalized_sql: slice.normalized_sql,
                                span: slice.span,
                                status: "error".to_string(),
                                warnings,
                                error: Some(err.to_string()),
                                row_count: None,
                                read_tables,
                                write_tables,
                            });
                            stopped_on_error = true;
                            break;
                        }
                    }
                }
                Err(err) => {
                    events.push(TraceStatementEvent {
                        index: slice.index,
                        sql: slice.sql,
                        normalized_sql: slice.normalized_sql,
                        span: slice.span,
                        status: "unsupported".to_string(),
                        warnings: Vec::new(),
                        error: Some(err.to_string()),
                        row_count: None,
                        read_tables: Vec::new(),
                        write_tables: Vec::new(),
                    });
                    stopped_on_error = true;
                    break;
                }
            }
        }
        Ok(ExecutionTrace {
            events,
            stopped_on_error,
        })
    }
}

fn execute_batch_statements<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmts: Vec<Statement>,
) -> Result<Option<QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let mut out = Ok(None);
    let mut ctx = ExecutionContext::new(
        &mut session.variables,
        &mut session.session_last_identity,
        &mut session.scope_identity_stack,
        &mut session.temp_table_map,
        &mut session.table_var_map,
        &mut session.table_var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors,
        &mut session.fetch_status,
        &mut session.print_output,
    );
    ctx.enter_scope();

    for stmt in stmts {
        if is_transaction_statement(&stmt) {
            match transaction_exec::execute_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                &mut session.journal,
                &mut session.workspace,
                stmt,
            ) {
                Ok(r) => out = Ok(r),
                Err(e) => {
                    out = Err(e);
                    break;
                }
            }
        } else {
            match execute_non_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                session.journal.as_mut(),
                &mut session.workspace,
                session.clock.as_ref(),
                &mut session.options,
                stmt,
                &mut ctx,
            ) {
                Ok(r) => out = Ok(r),
                Err(DbError::Return(_)) => {
                    out = Ok(None);
                    break;
                }
                Err(e) => {
                    out = Err(e);
                    break;
                }
            }
        }
    }

    if session.tx_manager.active.is_some() {
        if let Some(workspace) = session.workspace.as_mut() {
            cleanup_scope_table_vars(&mut workspace.catalog, &mut workspace.storage, &mut ctx)?;
        } else {
            let _ = ctx.leave_scope_collect_table_vars();
        }
    } else {
        cleanup_scope_table_vars(&mut state.catalog, &mut state.storage, &mut ctx)?;
    }
    out
}

fn execute_batch_statements_multi<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmts: Vec<Statement>,
) -> Result<Vec<Option<QueryResult>>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    let mut results: Vec<Option<QueryResult>> = Vec::new();
    let mut ctx = ExecutionContext::new(
        &mut session.variables,
        &mut session.session_last_identity,
        &mut session.scope_identity_stack,
        &mut session.temp_table_map,
        &mut session.table_var_map,
        &mut session.table_var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors,
        &mut session.fetch_status,
        &mut session.print_output,
    );
    ctx.enter_scope();

    for stmt in stmts {
        if is_transaction_statement(&stmt) {
            match transaction_exec::execute_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                &mut session.journal,
                &mut session.workspace,
                stmt,
            ) {
                Ok(r) => results.push(r),
                Err(e) => {
                    cleanup_scope_table_vars(&mut state.catalog, &mut state.storage, &mut ctx)?;
                    return Err(e);
                }
            }
        } else {
            match execute_non_transaction_statement(
                state,
                session_id,
                &mut session.tx_manager,
                session.journal.as_mut(),
                &mut session.workspace,
                session.clock.as_ref(),
                &mut session.options,
                stmt,
                &mut ctx,
            ) {
                Ok(r) => results.push(r),
                Err(DbError::Return(_)) => {
                    results.push(None);
                    break;
                }
                Err(e) => {
                    cleanup_scope_table_vars(&mut state.catalog, &mut state.storage, &mut ctx)?;
                    return Err(e);
                }
            }
        }
    }

    if session.tx_manager.active.is_some() {
        if let Some(workspace) = session.workspace.as_mut() {
            cleanup_scope_table_vars(&mut workspace.catalog, &mut workspace.storage, &mut ctx)?;
        } else {
            let _ = ctx.leave_scope_collect_table_vars();
        }
    } else {
        cleanup_scope_table_vars(&mut state.catalog, &mut state.storage, &mut ctx)?;
    }

    Ok(results)
}

fn execute_single_statement<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    session: &mut SessionRuntime<C, S>,
    stmt: Statement,
) -> Result<Option<QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    if is_transaction_statement(&stmt) {
        return transaction_exec::execute_transaction_statement(
            state,
            session_id,
            &mut session.tx_manager,
            &mut session.journal,
            &mut session.workspace,
            stmt,
        );
    }

    let mut ctx = ExecutionContext::new(
        &mut session.variables,
        &mut session.session_last_identity,
        &mut session.scope_identity_stack,
        &mut session.temp_table_map,
        &mut session.table_var_map,
        &mut session.table_var_counter,
        session.options.ansi_nulls,
        session.options.datefirst,
        &mut session.random_state,
        &mut session.cursors,
        &mut session.fetch_status,
        &mut session.print_output,
    );

    match execute_non_transaction_statement(
        state,
        session_id,
        &mut session.tx_manager,
        session.journal.as_mut(),
        &mut session.workspace,
        session.clock.as_ref(),
        &mut session.options,
        stmt,
        &mut ctx,
    ) {
        Err(DbError::Return(_)) => Ok(None),
        other => other,
    }
}

fn execute_non_transaction_statement<C, S>(
    state: &mut SharedState<C, S>,
    session_id: SessionId,
    tx_manager: &mut TransactionManager<C, S>,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace<C, S>>,
    clock: &dyn Clock,
    session_options: &mut SessionOptions,
    stmt: Statement,
    ctx: &mut ExecutionContext,
) -> Result<Option<QueryResult>, DbError>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    if let Statement::SetOption(opt) = &stmt {
        let apply = apply_set_option(opt, session_options);
        ctx.ansi_nulls = session_options.ansi_nulls;
        ctx.datefirst = session_options.datefirst;
        for warn in apply.warnings {
            journal.record(JournalEvent::Info { message: warn });
        }
        return Ok(None);
    }

    if tx_manager.active.is_some() {
        let isolation_level = tx_manager
            .active
            .as_ref()
            .map(|tx| tx.isolation_level)
            .unwrap_or(IsolationLevel::ReadCommitted);
        let read_from_shared = matches!(
            isolation_level,
            IsolationLevel::ReadCommitted | IsolationLevel::ReadUncommitted
        ) && matches!(stmt, Statement::Select(_));

        state.table_locks.acquire_statement_locks(
            session_id,
            tx_manager,
            workspace_slot,
            &stmt,
        )?;

        let mut script = if read_from_shared {
            ScriptExecutor {
                catalog: &mut state.catalog,
                storage: &mut state.storage,
                clock,
            }
        } else {
            let workspace = workspace_slot.as_mut().ok_or_else(|| {
                DbError::Execution("internal error: missing transaction workspace".into())
            })?;
            ScriptExecutor {
                catalog: &mut workspace.catalog,
                storage: &mut workspace.storage,
                clock,
            }
        };
        let out = script.execute(stmt.clone(), ctx);
        if out.is_ok() {
            transaction_exec::register_read_tables(workspace_slot, &stmt);
            transaction_exec::register_workspace_write_tables(workspace_slot, &stmt);
            transaction_exec::register_write_intent(tx_manager, journal, &stmt);
        } else if session_options.xact_abort
            && !matches!(out, Err(DbError::Break | DbError::Continue | DbError::Return(_)))
        {
            transaction_exec::force_xact_abort(
                state,
                session_id,
                tx_manager,
                journal,
                workspace_slot,
            );
        }
        out
    } else {
        let written_tables = collect_write_tables(&stmt);
        if written_tables.is_empty() {
            let mut script = ScriptExecutor {
                catalog: &mut state.catalog,
                storage: &mut state.storage,
                clock,
            };
            return script.execute(stmt, ctx);
        }
        let before_catalog = state.catalog.clone();
        let before_storage = state.storage.clone();
        let before_versions = state.table_versions.clone();
        let before_commit_ts = state.commit_ts;
        let mut script = ScriptExecutor {
            catalog: &mut state.catalog,
            storage: &mut state.storage,
            clock,
        };
        let out = script.execute(stmt, ctx);
        if out.is_ok() {
            state.commit_ts += 1;
            for table in &written_tables {
                state.table_versions.insert(table.clone(), state.commit_ts);
            }
            let checkpoint = state.to_checkpoint();
            if let Err(e) = state.durability.persist_checkpoint(&checkpoint) {
                state.catalog = before_catalog;
                state.storage = before_storage;
                state.table_versions = before_versions;
                state.commit_ts = before_commit_ts;
                return Err(e);
            }
        }
        out
    }
}

fn cleanup_scope_table_vars(
    catalog: &mut dyn Catalog,
    storage: &mut dyn crate::storage::Storage,
    ctx: &mut ExecutionContext,
) -> Result<(), DbError> {
    let dropped_physical = ctx.leave_scope_collect_table_vars();
    for physical in dropped_physical {
        if catalog.find_table("dbo", &physical).is_none() {
            continue;
        }
        let mut schema = SchemaExecutor { catalog, storage };
        schema.drop_table(DropTableStmt {
            name: ObjectName {
                schema: Some("dbo".to_string()),
                name: physical,
            },
        })?;
    }
    Ok(())
}
