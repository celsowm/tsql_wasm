use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;

use super::clock::{Clock, SystemClock};
use super::context::Variables;
use super::durability::{DurabilitySink, NoopDurability};
use super::journal::{Journal, NoopJournal};
use super::locks::{LockTable, SessionId, TxWorkspace};
use super::tooling::SessionOptions;
use super::transaction::TransactionManager;

pub trait SessionManager {
    fn create_session(&self) -> SessionId;
    fn reset_session(&self, session_id: SessionId) -> Result<(), DbError>;
    fn close_session(&self, session_id: SessionId) -> Result<(), DbError>;
    fn set_session_journal(
        &self,
        session_id: SessionId,
        journal: Box<dyn Journal>,
    ) -> Result<(), DbError>;
}

#[derive(Debug, Clone)]
pub struct IdentityState {
    pub(crate) last_identity: Option<i64>,
    pub(crate) scope_stack: Vec<Option<i64>>,
}

impl Default for IdentityState {
    fn default() -> Self {
        Self::new()
    }
}

impl IdentityState {
    pub fn new() -> Self {
        Self {
            last_identity: None,
            scope_stack: vec![None],
        }
    }
    pub fn reset(&mut self) {
        self.last_identity = None;
        self.scope_stack = vec![None];
    }
}

#[derive(Debug, Clone)]
pub struct TableState {
    pub(crate) temp_map: HashMap<String, String>,
    pub(crate) var_map: HashMap<String, String>,
    pub(crate) var_counter: u64,
}

impl Default for TableState {
    fn default() -> Self {
        Self::new()
    }
}

impl TableState {
    pub fn new() -> Self {
        Self {
            temp_map: HashMap::new(),
            var_map: HashMap::new(),
            var_counter: 0,
        }
    }
    pub fn reset(&mut self) {
        self.temp_map.clear();
        self.var_map.clear();
        self.var_counter = 0;
    }
}

#[derive(Debug, Clone)]
pub struct CursorState {
    pub(crate) map: HashMap<String, super::model::Cursor>,
    pub(crate) fetch_status: i32,
    pub(crate) next_cursor_handle: i32,
    pub(crate) handle_map: HashMap<i32, String>,
}

impl Default for CursorState {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorState {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            fetch_status: -1,
            next_cursor_handle: 1,
            handle_map: HashMap::new(),
        }
    }
    pub fn reset(&mut self) {
        self.map.clear();
        self.fetch_status = -1;
        self.next_cursor_handle = 1;
        self.handle_map.clear();
    }
}

#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    pub variables: Variables,
    pub identities: IdentityState,
    pub tables: TableState,
    pub cursors: CursorState,
    pub options: SessionOptions,
    pub random_state: u64,
}

#[derive(Debug, Clone)]
pub struct DiagnosticsState {
    pub(crate) print_output: Vec<String>,
}

impl Default for DiagnosticsState {
    fn default() -> Self {
        Self::new()
    }
}

impl DiagnosticsState {
    pub fn new() -> Self {
        Self {
            print_output: Vec::new(),
        }
    }
    pub fn reset(&mut self) {
        self.print_output.clear();
    }
}

pub struct SessionRuntime<C, S> {
    pub(crate) clock: Box<dyn Clock>,
    pub(crate) tx_manager: TransactionManager<C, S, SessionSnapshot>,
    pub(crate) journal: Box<dyn Journal>,
    pub(crate) variables: Variables,
    pub(crate) identities: IdentityState,
    pub(crate) tables: TableState,
    pub(crate) cursors: CursorState,
    pub(crate) diagnostics: DiagnosticsState,
    pub(crate) workspace: Option<TxWorkspace<C, S>>,
    pub(crate) options: SessionOptions,
    pub(crate) random_state: u64,
    pub(crate) current_database: String,
    pub(crate) original_database: String,
    pub(crate) user: Option<String>,
    pub(crate) app_name: Option<String>,
    pub(crate) host_name: Option<String>,
}

impl<C, S> SessionRuntime<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub(crate) fn new() -> Self {
        Self {
            clock: Box::new(SystemClock),
            tx_manager: TransactionManager::default(),
            journal: Box::new(NoopJournal),
            variables: Variables::new(),
            identities: IdentityState::new(),
            tables: TableState::new(),
            cursors: CursorState::new(),
            diagnostics: DiagnosticsState::new(),
            workspace: None,
            options: SessionOptions::default(),
            random_state: 1,
            current_database: "master".to_string(),
            original_database: "master".to_string(),
            user: None,
            app_name: None,
            host_name: None,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.tx_manager = TransactionManager::default();
        self.variables.clear();
        self.identities.reset();
        self.tables.reset();
        self.cursors.reset();
        self.diagnostics.reset();
        self.workspace = None;
        self.options = SessionOptions::default();
        self.random_state = 1;
        self.current_database = "master".to_string();
        self.original_database = "master".to_string();
        self.user = None;
        self.app_name = None;
        self.host_name = None;
    }
}

pub struct SharedStorage<C, S> {
    pub(crate) catalog: C,
    pub(crate) storage: S,
    pub(crate) commit_ts: u64,
    pub(crate) table_versions: HashMap<String, u64>,
}

impl<C, S> SharedStorage<C, S> {
    pub fn get_mut_refs(&mut self) -> (&mut C, &mut S) {
        (&mut self.catalog, &mut self.storage)
    }

    pub fn get_refs(&self) -> (&C, &S) {
        (&self.catalog, &self.storage)
    }
}

pub struct SharedState<C, S> {
    pub(crate) storage: parking_lot::RwLock<SharedStorage<C, S>>,
    pub(crate) table_locks: parking_lot::Mutex<LockTable>,
    pub(crate) durability: parking_lot::Mutex<Box<dyn DurabilitySink<C>>>,
    pub(crate) sessions: dashmap::DashMap<SessionId, parking_lot::Mutex<SessionRuntime<C, S>>>,
    pub(crate) deadlock_priorities: dashmap::DashMap<SessionId, i32>,
    pub(crate) next_session_id: std::sync::atomic::AtomicU64,
    pub(crate) dirty_buffer: std::sync::Arc<parking_lot::Mutex<super::dirty_buffer::DirtyBuffer>>,
}

impl<C, S> SharedState<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    pub fn with_initial(catalog: C, storage: S) -> Self {
        Self {
            storage: parking_lot::RwLock::new(SharedStorage {
                catalog,
                storage,
                commit_ts: 0,
                table_versions: HashMap::new(),
            }),
            table_locks: parking_lot::Mutex::new(LockTable::new()),
            durability: parking_lot::Mutex::new(Box::new(NoopDurability::default())),
            sessions: dashmap::DashMap::new(),
            deadlock_priorities: dashmap::DashMap::new(),
            next_session_id: std::sync::atomic::AtomicU64::new(1),
            dirty_buffer: std::sync::Arc::new(parking_lot::Mutex::new(
                super::dirty_buffer::DirtyBuffer::new(),
            )),
        }
    }
}
