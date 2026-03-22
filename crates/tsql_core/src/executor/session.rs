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
    fn close_session(&self, session_id: SessionId) -> Result<(), DbError>;
    fn set_session_journal(
        &self,
        session_id: SessionId,
        journal: Box<dyn Journal>,
    ) -> Result<(), DbError>;
}

pub struct SessionRuntime<C, S> {
    pub(crate) clock: Box<dyn Clock>,
    pub(crate) tx_manager: TransactionManager<C, S>,
    pub(crate) journal: Box<dyn Journal>,
    pub(crate) variables: Variables,
    pub(crate) session_last_identity: Option<i64>,
    pub(crate) scope_identity_stack: Vec<Option<i64>>,
    pub(crate) temp_table_map: HashMap<String, String>,
    pub(crate) table_var_map: HashMap<String, String>,
    pub(crate) table_var_counter: u64,
    pub(crate) workspace: Option<TxWorkspace<C, S>>,
    pub(crate) options: SessionOptions,
    pub(crate) random_state: u64,
}

impl<C, S> SessionRuntime<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static,
{
    pub(crate) fn new() -> Self {
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
            random_state: 1,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.tx_manager = TransactionManager::default();
        self.variables.clear();
        self.session_last_identity = None;
        self.scope_identity_stack = vec![None];
        self.temp_table_map.clear();
        self.table_var_map.clear();
        self.table_var_counter = 0;
        self.workspace = None;
        self.options = SessionOptions::default();
        self.random_state = 1;
    }
}

pub struct SharedState<C, S> {
    pub catalog: C,
    pub storage: S,
    pub commit_ts: u64,
    pub table_versions: HashMap<String, u64>,
    pub table_locks: LockTable,
    pub durability: Box<dyn DurabilitySink<C, S>>,
    pub sessions: HashMap<SessionId, SessionRuntime<C, S>>,
    pub next_session_id: SessionId,
}

impl<C, S> SharedState<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static,
{
    pub fn with_initial(catalog: C, storage: S) -> Self {
        Self {
            catalog,
            storage,
            commit_ts: 0,
            table_versions: HashMap::new(),
            table_locks: LockTable::new(),
            durability: Box::new(NoopDurability::default()),
            sessions: HashMap::new(),
            next_session_id: 1,
        }
    }

    pub fn with_session_mut<T, F>(&mut self, session_id: SessionId, f: F) -> Result<T, DbError>
    where
        F: FnOnce(&mut SharedState<C, S>, &mut SessionRuntime<C, S>) -> Result<T, DbError>,
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
