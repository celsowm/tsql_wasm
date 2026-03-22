use crate::ast::{DropTableStmt, IsolationLevel, ObjectName, SelectStmt, Statement, TableRef};
use crate::catalog::CatalogImpl;
use crate::error::DbError;
use crate::parser::{parse_batch, parse_sql};
use crate::storage::InMemoryStorage;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::clock::{Clock, SystemClock};
use super::context::{ExecutionContext, Variables};
use super::durability::{DurabilitySink, NoopDurability, RecoveryCheckpoint};
use super::journal::{Journal, JournalEvent, NoopJournal, WriteKind};
use super::projection::deduplicate_projected_rows;
use super::result::QueryResult;
use super::script::ScriptExecutor;
use super::tooling::{
    analyze_sql_batch, apply_set_option, collect_read_tables as collect_read_tables_tooling,
    collect_write_tables as collect_write_tables_tooling, explain_statement, statement_compat_warnings,
    split_sql_statements, CompatibilityReport, ExecutionTrace, ExplainPlan, SessionOptions,
    TraceStatementEvent,
};
use super::transaction::{TransactionManager, WriteIntentKind};

pub type SessionId = u64;

#[derive(Debug, Clone)]
struct TxWorkspace {
    catalog: CatalogImpl,
    storage: InMemoryStorage,
    base_table_versions: HashMap<String, u64>,
    read_tables: HashSet<String>,
    write_tables: HashSet<String>,
    acquired_locks: Vec<AcquiredLock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockMode {
    Read,
    Write,
}

#[derive(Debug, Clone)]
struct AcquiredLock {
    table: String,
    mode: LockMode,
    savepoint_depth: usize,
}

#[derive(Debug, Default, Clone)]
struct TableLockState {
    readers: HashMap<SessionId, u32>,
    writer: Option<(SessionId, u32)>,
}

struct SessionRuntime {
    clock: Box<dyn Clock>,
    tx_manager: TransactionManager,
    journal: Box<dyn Journal>,
    variables: Variables,
    session_last_identity: Option<i64>,
    scope_identity_stack: Vec<Option<i64>>,
    temp_table_map: HashMap<String, String>,
    table_var_map: HashMap<String, String>,
    table_var_counter: u64,
    workspace: Option<TxWorkspace>,
    options: SessionOptions,
}

impl SessionRuntime {
    fn new() -> Self {
        Self {
            clock: Box::new(SystemClock),
            tx_manager: TransactionManager::default(),
            journal: Box::new(NoopJournal),
            variables: Variables::new(),
            session_last_identity: None,
            scope_identity_stack: vec![None],
            temp_table_map: HashMap::new(),
            table_var_map: HashMap::new(),
            table_var_counter: 0,
            workspace: None,
            options: SessionOptions::default(),
        }
    }

    fn reset(&mut self) {
        self.tx_manager = TransactionManager::default();
        self.variables.clear();
        self.session_last_identity = None;
        self.scope_identity_stack = vec![None];
        self.temp_table_map.clear();
        self.table_var_map.clear();
        self.table_var_counter = 0;
        self.workspace = None;
        self.options = SessionOptions::default();
    }
}

struct SharedState {
    catalog: CatalogImpl,
    storage: InMemoryStorage,
    commit_ts: u64,
    table_versions: HashMap<String, u64>,
    table_locks: HashMap<String, TableLockState>,
    durability: Box<dyn DurabilitySink>,
    sessions: HashMap<SessionId, SessionRuntime>,
    next_session_id: SessionId,
}

impl SharedState {
    fn new() -> Self {
        Self {
            catalog: CatalogImpl::new(),
            storage: InMemoryStorage::default(),
            commit_ts: 0,
            table_versions: HashMap::new(),
            table_locks: HashMap::new(),
            durability: Box::new(NoopDurability),
            sessions: HashMap::new(),
            next_session_id: 1,
        }
    }

    fn from_checkpoint(checkpoint: RecoveryCheckpoint) -> Self {
        let mut state = Self::new();
        state.apply_checkpoint(checkpoint);
        state
    }

    fn apply_checkpoint(&mut self, checkpoint: RecoveryCheckpoint) {
        self.catalog = checkpoint.catalog;
        self.storage = checkpoint.storage;
        self.commit_ts = checkpoint.commit_ts;
        self.table_versions = checkpoint.table_versions;
        self.table_locks.clear();
        for session in self.sessions.values_mut() {
            session.reset();
        }
    }

    fn to_checkpoint(&self) -> RecoveryCheckpoint {
        RecoveryCheckpoint {
            catalog: self.catalog.clone(),
            storage: self.storage.clone(),
            commit_ts: self.commit_ts,
            table_versions: self.table_versions.clone(),
        }
    }

    fn with_session_mut<T, F>(&mut self, session_id: SessionId, f: F) -> Result<T, DbError>
    where
        F: FnOnce(&mut SharedState, &mut SessionRuntime) -> Result<T, DbError>,
    {
        let mut session = self
            .sessions
            .remove(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let result = f(self, &mut session);
        self.sessions.insert(session_id, session);
        result
    }
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<Mutex<SharedState>>,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedState::new())),
        }
    }

    pub fn new_with_durability(durability: Box<dyn DurabilitySink>) -> Self {
        let state = if let Some(checkpoint) = durability.latest_checkpoint() {
            let mut restored = SharedState::from_checkpoint(checkpoint);
            restored.durability = durability;
            restored
        } else {
            let mut fresh = SharedState::new();
            fresh.durability = durability;
            fresh
        };
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }

    pub fn from_checkpoint(payload: &str) -> Result<Self, DbError> {
        let checkpoint = RecoveryCheckpoint::from_json(payload)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(SharedState::from_checkpoint(checkpoint))),
        })
    }

    pub fn create_session(&self) -> SessionId {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        let id = guard.next_session_id;
        guard.next_session_id += 1;
        guard.sessions.insert(id, SessionRuntime::new());
        id
    }

    pub fn close_session(&self, session_id: SessionId) -> Result<(), DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        release_all_locks_for_session(&mut guard, session_id);
        let removed = guard.sessions.remove(&session_id);
        if removed.is_none() {
            return Err(DbError::Execution(format!(
                "session {} not found",
                session_id
            )));
        }
        Ok(())
    }

    pub fn set_session_journal(
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

    pub fn reset(&self) {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.catalog = CatalogImpl::new();
        guard.storage = InMemoryStorage::default();
        guard.commit_ts = 0;
        guard.table_versions.clear();
        guard.table_locks.clear();
        for session in guard.sessions.values_mut() {
            session.reset();
        }
    }

    pub fn set_durability_sink(&self, durability: Box<dyn DurabilitySink>) {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.durability = durability;
    }

    pub fn export_checkpoint(&self) -> Result<String, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        guard.to_checkpoint().to_json()
    }

    pub fn import_checkpoint(&self, payload: &str) -> Result<(), DbError> {
        let checkpoint = RecoveryCheckpoint::from_json(payload)?;
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.apply_checkpoint(checkpoint.clone());
        guard.durability.persist_checkpoint(&checkpoint)?;
        Ok(())
    }

    pub fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_single_statement(state, session_id, session, stmt)
        })
    }

    pub fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_batch_statements(state, session_id, session, stmts)
        })
    }

    pub fn session_isolation_level(
        &self,
        session_id: SessionId,
    ) -> Result<IsolationLevel, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.tx_manager.session_isolation_level)
    }

    pub fn transaction_is_active(&self, session_id: SessionId) -> Result<bool, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.tx_manager.active.is_some())
    }

    pub fn session_options(&self, session_id: SessionId) -> Result<SessionOptions, DbError> {
        let guard = self.inner.lock().expect("database mutex poisoned");
        let session = guard
            .sessions
            .get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        Ok(session.options.clone())
    }

    pub fn analyze_sql_batch(&self, sql: &str) -> CompatibilityReport {
        analyze_sql_batch(sql)
    }

    pub fn explain_sql(&self, sql: &str) -> Result<super::tooling::ExplainPlan, DbError> {
        let stmt = parse_sql(sql)?;
        Ok(explain_statement(&stmt))
    }

    pub fn trace_execute_session_sql(
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

    pub fn execute_session_batch_sql(
        &self,
        session_id: SessionId,
        sql: &str,
    ) -> Result<Option<QueryResult>, DbError> {
        let stmts = parse_batch(sql)?;
        self.execute_session_batch(session_id, stmts)
    }
}

pub struct Engine {
    db: Database,
    default_session: SessionId,
}

impl std::fmt::Debug for Engine {
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

    pub fn new_with_durability(durability: Box<dyn DurabilitySink>) -> Self {
        let db = Database::new_with_durability(durability);
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

    pub fn database(&self) -> Database {
        self.db.clone()
    }

    pub fn reset(&mut self) {
        self.db.reset();
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<Option<QueryResult>, DbError> {
        self.db.execute_session(self.default_session, stmt)
    }

    pub fn execute_batch(&mut self, stmts: Vec<Statement>) -> Result<Option<QueryResult>, DbError> {
        self.db.execute_session_batch(self.default_session, stmts)
    }

    pub fn set_journal(&mut self, journal: Box<dyn Journal>) {
        let _ = self.db.set_session_journal(self.default_session, journal);
    }

    pub fn set_durability_sink(&mut self, durability: Box<dyn DurabilitySink>) {
        self.db.set_durability_sink(durability);
    }

    pub fn export_checkpoint(&self) -> Result<String, DbError> {
        self.db.export_checkpoint()
    }

    pub fn import_checkpoint(&mut self, payload: &str) -> Result<(), DbError> {
        self.db.import_checkpoint(payload)
    }

    pub fn session_isolation_level(&self) -> IsolationLevel {
        self.db
            .session_isolation_level(self.default_session)
            .unwrap_or(IsolationLevel::ReadCommitted)
    }

    pub fn transaction_is_active(&self) -> bool {
        self.db
            .transaction_is_active(self.default_session)
            .unwrap_or(false)
    }

    pub fn session_options(&self) -> SessionOptions {
        self.db
            .session_options(self.default_session)
            .unwrap_or_default()
    }

    pub fn analyze_sql_batch(&self, sql: &str) -> CompatibilityReport {
        self.db.analyze_sql_batch(sql)
    }

    pub fn explain_sql(&self, sql: &str) -> Result<ExplainPlan, DbError> {
        self.db.explain_sql(sql)
    }

    pub fn trace_execute_sql(&self, sql: &str) -> Result<ExecutionTrace, DbError> {
        self.db.trace_execute_session_sql(self.default_session, sql)
    }
}

fn execute_batch_statements(
    state: &mut SharedState,
    session_id: SessionId,
    session: &mut SessionRuntime,
    stmts: Vec<Statement>,
) -> Result<Option<QueryResult>, DbError> {
    let mut out = Ok(None);
    let mut ctx = ExecutionContext::new(
        &mut session.variables,
        &mut session.session_last_identity,
        &mut session.scope_identity_stack,
        &mut session.temp_table_map,
        &mut session.table_var_map,
        &mut session.table_var_counter,
    );
    ctx.enter_scope();

    for stmt in stmts {
        if is_transaction_statement(&stmt) {
            match execute_transaction_statement(
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

fn execute_single_statement(
    state: &mut SharedState,
    session_id: SessionId,
    session: &mut SessionRuntime,
    stmt: Statement,
) -> Result<Option<QueryResult>, DbError> {
    if is_transaction_statement(&stmt) {
        return execute_transaction_statement(
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

fn execute_non_transaction_statement(
    state: &mut SharedState,
    session_id: SessionId,
    tx_manager: &mut TransactionManager,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace>,
    clock: &dyn Clock,
    session_options: &mut SessionOptions,
    stmt: Statement,
    ctx: &mut ExecutionContext,
) -> Result<Option<QueryResult>, DbError> {
    if let Statement::SetOption(opt) = &stmt {
        let apply = apply_set_option(opt, session_options);
        for warn in apply.warnings {
            journal.record(JournalEvent::Info {
                message: warn,
            });
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

        acquire_statement_locks(state, session_id, tx_manager, workspace_slot, &stmt)?;

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
            register_read_tables(workspace_slot, &stmt);
            register_workspace_write_tables(workspace_slot, &stmt);
            register_write_intent(tx_manager, journal, &stmt);
        } else if session_options.xact_abort
            && !matches!(out, Err(DbError::Break | DbError::Continue | DbError::Return(_)))
        {
            force_xact_abort(state, session_id, tx_manager, journal, workspace_slot);
        }
        out
    } else {
        let written_tables = collect_write_tables(&stmt);
        let before_catalog = if written_tables.is_empty() {
            None
        } else {
            Some(state.catalog.clone())
        };
        let before_storage = if written_tables.is_empty() {
            None
        } else {
            Some(state.storage.clone())
        };
        let before_versions = if written_tables.is_empty() {
            None
        } else {
            Some(state.table_versions.clone())
        };
        let before_commit_ts = state.commit_ts;
        let mut script = ScriptExecutor {
            catalog: &mut state.catalog,
            storage: &mut state.storage,
            clock,
        };
        let out = script.execute(stmt, ctx);
        if out.is_ok() && !written_tables.is_empty() {
            state.commit_ts += 1;
            for table in written_tables {
                state.table_versions.insert(table, state.commit_ts);
            }
            let checkpoint = state.to_checkpoint();
            if let Err(e) = state.durability.persist_checkpoint(&checkpoint) {
                if let (Some(c), Some(s), Some(v)) = (before_catalog, before_storage, before_versions) {
                    state.catalog = c;
                    state.storage = s;
                    state.table_versions = v;
                    state.commit_ts = before_commit_ts;
                }
                return Err(e);
            }
        }
        out
    }
}

fn cleanup_scope_table_vars(
    catalog: &mut dyn crate::catalog::Catalog,
    storage: &mut dyn crate::storage::Storage,
    ctx: &mut ExecutionContext,
) -> Result<(), DbError> {
    let dropped_physical = ctx.leave_scope_collect_table_vars();
    for physical in dropped_physical {
        if catalog.find_table("dbo", &physical).is_none() {
            continue;
        }
        let mut schema = super::schema::SchemaExecutor { catalog, storage };
        schema.drop_table(DropTableStmt {
            name: ObjectName {
                schema: Some("dbo".to_string()),
                name: physical,
            },
        })?;
    }
    Ok(())
}

fn execute_transaction_statement(
    state: &mut SharedState,
    session_id: SessionId,
    tx_manager: &mut TransactionManager,
    journal: &mut Box<dyn Journal>,
    workspace_slot: &mut Option<TxWorkspace>,
    stmt: Statement,
) -> Result<Option<QueryResult>, DbError> {
    match stmt {
        Statement::BeginTransaction(name) => {
            let workspace_catalog = state.catalog.clone();
            let workspace_storage = state.storage.clone();
            tx_manager.commit_ts = state.commit_ts;
            let begin_name = tx_manager.begin(&workspace_catalog, &workspace_storage, name)?;
            *workspace_slot = Some(TxWorkspace {
                catalog: workspace_catalog,
                storage: workspace_storage,
                base_table_versions: state.table_versions.clone(),
                read_tables: HashSet::new(),
                write_tables: HashSet::new(),
                acquired_locks: Vec::new(),
            });
            journal.record(JournalEvent::Begin {
                isolation_level: tx_manager.session_isolation_level,
                name: begin_name,
            });
            Ok(None)
        }
        Statement::CommitTransaction => {
            let tx = tx_manager
                .active
                .as_ref()
                .ok_or_else(|| DbError::Execution("COMMIT without active transaction".into()))?;
            let workspace = workspace_slot.as_ref().ok_or_else(|| {
                DbError::Execution("internal error: missing transaction workspace".into())
            })?;

            let conflicts = detect_conflicts(
                tx.isolation_level,
                &workspace.base_table_versions,
                &workspace.read_tables,
                &workspace.write_tables,
                &state.table_versions,
            );
            if conflicts {
                return Err(DbError::Execution(
                    "transaction conflict detected during COMMIT".into(),
                ));
            }

            let next_commit_ts = state.commit_ts + 1;
            let mut next_table_versions = state.table_versions.clone();
            for table in &workspace.write_tables {
                next_table_versions.insert(table.clone(), next_commit_ts);
            }
            let checkpoint = RecoveryCheckpoint {
                catalog: workspace.catalog.clone(),
                storage: workspace.storage.clone(),
                commit_ts: next_commit_ts,
                table_versions: next_table_versions.clone(),
            };
            state.durability.persist_checkpoint(&checkpoint)?;

            state.catalog = workspace.catalog.clone();
            state.storage = workspace.storage.clone();
            state.commit_ts = next_commit_ts;
            for table in &workspace.write_tables {
                state.table_versions.insert(table.clone(), state.commit_ts);
            }
            release_workspace_locks(state, session_id, workspace_slot, 0);
            tx_manager.active = None;
            tx_manager.commit_ts = state.commit_ts;
            *workspace_slot = None;
            journal.record(JournalEvent::Commit);
            Ok(None)
        }
        Statement::RollbackTransaction(savepoint) => {
            {
                let workspace = workspace_slot.as_mut().ok_or_else(|| {
                    DbError::Execution("ROLLBACK without active transaction".into())
                })?;
                tx_manager.rollback(
                    savepoint.clone(),
                    &mut workspace.catalog,
                    &mut workspace.storage,
                )?;
                if let Some(ref active_tx) = tx_manager.active {
                    let keep = active_tx.write_set.len();
                    if workspace.write_tables.len() > keep {
                        let mut names: Vec<_> = workspace.write_tables.iter().cloned().collect();
                        names.sort();
                        names.truncate(keep);
                        workspace.write_tables = names.into_iter().collect();
                    }
                }
            }
            if let Some(ref active_tx) = tx_manager.active {
                let keep_depth = active_tx.savepoints.len();
                release_workspace_locks(state, session_id, workspace_slot, keep_depth);
            } else {
                release_workspace_locks(state, session_id, workspace_slot, 0);
                *workspace_slot = None;
            }
            journal.record(JournalEvent::Rollback { savepoint });
            Ok(None)
        }
        Statement::SaveTransaction(name) => {
            let workspace = workspace_slot.as_ref().ok_or_else(|| {
                DbError::Execution("SAVE TRANSACTION without active transaction".into())
            })?;
            tx_manager.save(name.clone(), &workspace.catalog, &workspace.storage)?;
            journal.record(JournalEvent::Savepoint { name });
            Ok(None)
        }
        Statement::SetTransactionIsolationLevel(level) => {
            tx_manager.set_isolation_level(level)?;
            journal.record(JournalEvent::SetIsolationLevel {
                isolation_level: level,
            });
            Ok(None)
        }
        _ => Err(DbError::Execution(
            "internal error while executing transaction statement".into(),
        )),
    }
}

fn force_xact_abort(
    state: &mut SharedState,
    session_id: SessionId,
    tx_manager: &mut TransactionManager,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace>,
) {
    if tx_manager.active.is_none() {
        return;
    }
    if let Some(workspace) = workspace_slot.as_mut() {
        let _ = tx_manager.rollback(None, &mut workspace.catalog, &mut workspace.storage);
    }
    release_workspace_locks(state, session_id, workspace_slot, 0);
    *workspace_slot = None;
    tx_manager.active = None;
    tx_manager.commit_ts = state.commit_ts;
    journal.record(JournalEvent::Rollback { savepoint: None });
}

fn acquire_statement_locks(
    state: &mut SharedState,
    session_id: SessionId,
    tx_manager: &TransactionManager,
    workspace_slot: &mut Option<TxWorkspace>,
    stmt: &Statement,
) -> Result<(), DbError> {
    let read_tables = collect_read_tables(stmt);
    let write_tables = collect_write_tables(stmt);
    let depth = tx_manager
        .active
        .as_ref()
        .map(|tx| tx.savepoints.len())
        .unwrap_or(0);
    let isolation_level = tx_manager
        .active
        .as_ref()
        .map(|tx| tx.isolation_level)
        .unwrap_or(IsolationLevel::ReadCommitted);

    let read_lock_required = !read_tables.is_empty()
        && write_tables.is_empty()
        && matches!(
            isolation_level,
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable | IsolationLevel::Snapshot
        );

    if read_lock_required {
        for table in read_tables {
            acquire_lock(state, session_id, workspace_slot, &table, LockMode::Read, depth)?;
        }
    }
    for table in write_tables {
        acquire_lock(
            state,
            session_id,
            workspace_slot,
            &table,
            LockMode::Write,
            depth,
        )?;
    }
    Ok(())
}

fn acquire_lock(
    state: &mut SharedState,
    session_id: SessionId,
    workspace_slot: &mut Option<TxWorkspace>,
    table: &str,
    mode: LockMode,
    savepoint_depth: usize,
) -> Result<(), DbError> {
    let normalized = table.to_uppercase();
    let lock_state = state.table_locks.entry(normalized.clone()).or_default();

    match mode {
        LockMode::Read => {
            if let Some((writer, _)) = lock_state.writer {
                if writer != session_id {
                    return Err(DbError::Execution(format!(
                        "lock conflict (no-wait): READ lock on '{}' blocked by WRITE lock from session {}",
                        normalized, writer
                    )));
                }
            }
            *lock_state.readers.entry(session_id).or_insert(0) += 1;
        }
        LockMode::Write => {
            if let Some((writer, _)) = lock_state.writer {
                if writer != session_id {
                    return Err(DbError::Execution(format!(
                        "lock conflict (no-wait): WRITE lock on '{}' blocked by WRITE lock from session {}",
                        normalized, writer
                    )));
                }
            }
            if lock_state
                .readers
                .iter()
                .any(|(reader, count)| *reader != session_id && *count > 0)
            {
                return Err(DbError::Execution(format!(
                    "lock conflict (no-wait): WRITE lock on '{}' blocked by active READ lock",
                    normalized
                )));
            }
            match lock_state.writer.as_mut() {
                Some((writer, count)) if *writer == session_id => {
                    *count += 1;
                }
                _ => {
                    lock_state.writer = Some((session_id, 1));
                }
            }
        }
    }

    if let Some(workspace) = workspace_slot.as_mut() {
        workspace.acquired_locks.push(AcquiredLock {
            table: normalized,
            mode,
            savepoint_depth,
        });
    }
    Ok(())
}

fn release_workspace_locks(
    state: &mut SharedState,
    session_id: SessionId,
    workspace_slot: &mut Option<TxWorkspace>,
    keep_depth_inclusive: usize,
) {
    let Some(workspace) = workspace_slot.as_mut() else {
        return;
    };

    let mut retained = Vec::with_capacity(workspace.acquired_locks.len());
    for lock in workspace.acquired_locks.drain(..) {
        if lock.savepoint_depth < keep_depth_inclusive {
            retained.push(lock);
            continue;
        }
        release_lock_count(state, session_id, &lock.table, lock.mode);
    }
    workspace.acquired_locks = retained;
}

fn release_all_locks_for_session(state: &mut SharedState, session_id: SessionId) {
    let tables: Vec<String> = state.table_locks.keys().cloned().collect();
    for table in tables {
        release_all_for_table(state, session_id, &table);
    }
}

fn release_all_for_table(state: &mut SharedState, session_id: SessionId, table: &str) {
    let Some(lock_state) = state.table_locks.get_mut(table) else {
        return;
    };
    lock_state.readers.remove(&session_id);
    if lock_state
        .writer
        .map(|(owner, _)| owner == session_id)
        .unwrap_or(false)
    {
        lock_state.writer = None;
    }
    if lock_state.readers.is_empty() && lock_state.writer.is_none() {
        state.table_locks.remove(table);
    }
}

fn release_lock_count(
    state: &mut SharedState,
    session_id: SessionId,
    table: &str,
    mode: LockMode,
) {
    let Some(lock_state) = state.table_locks.get_mut(table) else {
        return;
    };
    match mode {
        LockMode::Read => {
            if let Some(count) = lock_state.readers.get_mut(&session_id) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    lock_state.readers.remove(&session_id);
                }
            }
        }
        LockMode::Write => {
            if let Some((owner, count)) = lock_state.writer.as_mut() {
                if *owner == session_id {
                    if *count > 1 {
                        *count -= 1;
                    } else {
                        lock_state.writer = None;
                    }
                }
            }
        }
    }
    if lock_state.readers.is_empty() && lock_state.writer.is_none() {
        state.table_locks.remove(table);
    }
}

fn detect_conflicts(
    isolation_level: IsolationLevel,
    base_versions: &HashMap<String, u64>,
    read_tables: &HashSet<String>,
    write_tables: &HashSet<String>,
    current_versions: &HashMap<String, u64>,
) -> bool {
    let has_changed = |table: &str| -> bool {
        let base = base_versions.get(table).copied().unwrap_or(0);
        let now = current_versions.get(table).copied().unwrap_or(0);
        now > base
    };

    match isolation_level {
        IsolationLevel::ReadUncommitted | IsolationLevel::ReadCommitted => false,
        IsolationLevel::Snapshot => write_tables.iter().any(|t| has_changed(t)),
        IsolationLevel::RepeatableRead => read_tables.iter().any(|t| has_changed(t)),
        IsolationLevel::Serializable => {
            read_tables.iter().any(|t| has_changed(t))
                || write_tables.iter().any(|t| has_changed(t))
        }
    }
}

fn register_read_tables(workspace_slot: &mut Option<TxWorkspace>, stmt: &Statement) {
    if let Some(workspace) = workspace_slot.as_mut() {
        for table in collect_read_tables(stmt) {
            workspace.read_tables.insert(table);
        }
    }
}

fn register_workspace_write_tables(workspace_slot: &mut Option<TxWorkspace>, stmt: &Statement) {
    if let Some(workspace) = workspace_slot.as_mut() {
        for table in collect_write_tables(stmt) {
            workspace.write_tables.insert(table);
        }
    }
}

fn collect_read_tables(stmt: &Statement) -> HashSet<String> {
    let mut out = HashSet::new();
    match stmt {
        Statement::Select(s) => collect_tables_from_select(s, &mut out),
        Statement::Update(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Delete(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::SelectAssign(s) => {
            if let Some(from) = &s.from {
                out.insert(normalize_table_ref(from));
            }
            for join in &s.joins {
                out.insert(normalize_table_ref(&join.table));
            }
        }
        Statement::SetOp(s) => {
            out.extend(collect_read_tables(&s.left));
            out.extend(collect_read_tables(&s.right));
        }
        Statement::WithCte(s) => {
            for cte in &s.ctes {
                collect_tables_from_select(&cte.query, &mut out);
            }
            out.extend(collect_read_tables(&s.body));
        }
        _ => {}
    }
    out
}

fn collect_tables_from_select(select: &SelectStmt, out: &mut HashSet<String>) {
    if let Some(from) = &select.from {
        out.insert(normalize_table_ref(from));
    }
    for join in &select.joins {
        out.insert(normalize_table_ref(&join.table));
    }
}

fn collect_write_tables(stmt: &Statement) -> HashSet<String> {
    let mut out = HashSet::new();
    match stmt {
        Statement::Insert(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Update(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::Delete(s) => {
            out.insert(normalize_table_name(&s.table));
        }
        Statement::CreateTable(s) => {
            out.insert(s.name.name.to_uppercase());
        }
        Statement::DropTable(s) => {
            out.insert(s.name.name.to_uppercase());
        }
        Statement::AlterTable(s) => {
            out.insert(s.table.name.to_uppercase());
        }
        Statement::TruncateTable(s) => {
            out.insert(s.name.name.to_uppercase());
        }
        Statement::CreateIndex(s) => {
            out.insert(s.table.name.to_uppercase());
        }
        Statement::DropIndex(s) => {
            out.insert(s.table.name.to_uppercase());
        }
        Statement::CreateSchema(_) | Statement::DropSchema(_) => {
            out.insert("__GLOBAL__".to_string());
        }
        _ => {}
    }
    out
}

fn normalize_table_name(name: &ObjectName) -> String {
    name.name.to_uppercase()
}

fn normalize_table_ref(table_ref: &TableRef) -> String {
    table_ref.name.name.to_uppercase()
}

fn is_transaction_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::BeginTransaction(_)
            | Statement::CommitTransaction
            | Statement::RollbackTransaction(_)
            | Statement::SaveTransaction(_)
            | Statement::SetTransactionIsolationLevel(_)
    )
}

fn register_write_intent(
    tx_manager: &mut TransactionManager,
    journal: &mut dyn Journal,
    stmt: &Statement,
) {
    if tx_manager.active.is_none() {
        return;
    }

    let (kind, table) = match stmt {
        Statement::Insert(s) => (WriteIntentKind::Insert, Some(s.table.name.clone())),
        Statement::Update(s) => (WriteIntentKind::Update, Some(s.table.name.clone())),
        Statement::Delete(s) => (WriteIntentKind::Delete, Some(s.table.name.clone())),
        Statement::CreateTable(s) => (WriteIntentKind::Ddl, Some(s.name.name.clone())),
        Statement::DropTable(s) => (WriteIntentKind::Ddl, Some(s.name.name.clone())),
        Statement::AlterTable(s) => (WriteIntentKind::Ddl, Some(s.table.name.clone())),
        Statement::TruncateTable(s) => (WriteIntentKind::Ddl, Some(s.name.name.clone())),
        Statement::CreateIndex(s) => (WriteIntentKind::Ddl, Some(s.table.name.clone())),
        Statement::DropIndex(s) => (WriteIntentKind::Ddl, Some(s.table.name.clone())),
        Statement::CreateSchema(_) | Statement::DropSchema(_) => (WriteIntentKind::Ddl, None),
        _ => return,
    };

    tx_manager.register_write_intent(kind, table.clone());
    journal.record(JournalEvent::WriteIntent {
        kind: map_write_kind(kind),
        table,
    });
}

fn map_write_kind(kind: WriteIntentKind) -> WriteKind {
    match kind {
        WriteIntentKind::Insert => WriteKind::Insert,
        WriteIntentKind::Update => WriteKind::Update,
        WriteIntentKind::Delete => WriteKind::Delete,
        WriteIntentKind::Ddl => WriteKind::Ddl,
    }
}

pub(crate) fn execute_set_op(
    left: QueryResult,
    right: QueryResult,
    op: crate::ast::SetOpKind,
) -> Result<QueryResult, DbError> {
    if left.columns.len() != right.columns.len() {
        return Err(DbError::Execution(
            "set operations require same number of columns".into(),
        ));
    }

    let rows = match op {
        crate::ast::SetOpKind::Union => {
            let mut rows = left.rows;
            rows.extend(right.rows);
            deduplicate_projected_rows(rows)
        }
        crate::ast::SetOpKind::UnionAll => {
            let mut rows = left.rows;
            rows.extend(right.rows);
            rows
        }
        crate::ast::SetOpKind::Intersect => {
            let left_set: std::collections::HashSet<_> = left.rows.iter().cloned().collect();
            right
                .rows
                .into_iter()
                .filter(|r| left_set.contains(r))
                .collect()
        }
        crate::ast::SetOpKind::Except => {
            let right_set: std::collections::HashSet<_> = right.rows.iter().cloned().collect();
            left.rows
                .into_iter()
                .filter(|r| !right_set.contains(r))
                .collect()
        }
    };

    Ok(QueryResult {
        columns: left.columns,
        rows,
    })
}
