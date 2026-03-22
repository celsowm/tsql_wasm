use crate::ast::{DropTableStmt, IsolationLevel, ObjectName, SelectStmt, Statement, TableRef};
use crate::catalog::CatalogImpl;
use crate::error::DbError;
use crate::storage::InMemoryStorage;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::clock::{Clock, SystemClock};
use super::context::{ExecutionContext, Variables};
use super::journal::{Journal, JournalEvent, NoopJournal, WriteKind};
use super::projection::deduplicate_projected_rows;
use super::result::QueryResult;
use super::script::ScriptExecutor;
use super::transaction::{TransactionManager, WriteIntentKind};

pub type SessionId = u64;

#[derive(Debug, Clone)]
struct TxWorkspace {
    catalog: CatalogImpl,
    storage: InMemoryStorage,
    base_table_versions: HashMap<String, u64>,
    read_tables: HashSet<String>,
    write_tables: HashSet<String>,
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
    }
}

struct SharedState {
    catalog: CatalogImpl,
    storage: InMemoryStorage,
    commit_ts: u64,
    table_versions: HashMap<String, u64>,
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
            sessions: HashMap::new(),
            next_session_id: 1,
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

    pub fn create_session(&self) -> SessionId {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        let id = guard.next_session_id;
        guard.next_session_id += 1;
        guard.sessions.insert(id, SessionRuntime::new());
        id
    }

    pub fn close_session(&self, session_id: SessionId) -> Result<(), DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
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
        for session in guard.sessions.values_mut() {
            session.reset();
        }
    }

    pub fn execute_session(
        &self,
        session_id: SessionId,
        stmt: Statement,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_single_statement(state, session, stmt)
        })
    }

    pub fn execute_session_batch(
        &self,
        session_id: SessionId,
        stmts: Vec<Statement>,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut guard = self.inner.lock().expect("database mutex poisoned");
        guard.with_session_mut(session_id, |state, session| {
            execute_batch_statements(state, session, stmts)
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
}

fn execute_batch_statements(
    state: &mut SharedState,
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
                &mut session.tx_manager,
                session.journal.as_mut(),
                &mut session.workspace,
                session.clock.as_ref(),
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
    session: &mut SessionRuntime,
    stmt: Statement,
) -> Result<Option<QueryResult>, DbError> {
    if is_transaction_statement(&stmt) {
        return execute_transaction_statement(
            state,
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
        &mut session.tx_manager,
        session.journal.as_mut(),
        &mut session.workspace,
        session.clock.as_ref(),
        stmt,
        &mut ctx,
    ) {
        Err(DbError::Return(_)) => Ok(None),
        other => other,
    }
}

fn execute_non_transaction_statement(
    state: &mut SharedState,
    tx_manager: &mut TransactionManager,
    journal: &mut dyn Journal,
    workspace_slot: &mut Option<TxWorkspace>,
    clock: &dyn Clock,
    stmt: Statement,
    ctx: &mut ExecutionContext,
) -> Result<Option<QueryResult>, DbError> {
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

        register_read_tables(workspace_slot, &stmt);
        register_workspace_write_tables(workspace_slot, &stmt);
        register_write_intent(tx_manager, journal, &stmt);

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
        script.execute(stmt, ctx)
    } else {
        let written_tables = collect_write_tables(&stmt);
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

            state.catalog = workspace.catalog.clone();
            state.storage = workspace.storage.clone();
            state.commit_ts += 1;
            for table in &workspace.write_tables {
                state.table_versions.insert(table.clone(), state.commit_ts);
            }
            tx_manager.active = None;
            tx_manager.commit_ts = state.commit_ts;
            *workspace_slot = None;
            journal.record(JournalEvent::Commit);
            Ok(None)
        }
        Statement::RollbackTransaction(savepoint) => {
            let workspace = workspace_slot
                .as_mut()
                .ok_or_else(|| DbError::Execution("ROLLBACK without active transaction".into()))?;
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
            } else {
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
