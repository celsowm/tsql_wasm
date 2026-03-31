use std::collections::HashMap;

use crate::ast::{IsolationLevel, Statement};
use crate::error::DbError;

use super::table_util::{collect_read_tables, collect_write_tables};

pub type SessionId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Read,
    Write,
}

#[derive(Debug, Clone)]
pub struct AcquiredLock {
    pub table: String,
    pub mode: LockMode,
    pub savepoint_depth: usize,
}

#[derive(Debug, Default, Clone)]
pub struct TableLockState {
    pub readers: HashMap<SessionId, u32>,
    pub writer: Option<(SessionId, u32)>,
}

pub struct TxWorkspace<C, S> {
    pub catalog: C,
    pub storage: S,
    pub base_table_versions: HashMap<String, u64>,
    pub read_tables: std::collections::HashSet<String>,
    pub write_tables: std::collections::HashSet<String>,
    pub acquired_locks: Vec<AcquiredLock>,
}

impl<C: std::fmt::Debug, S: std::fmt::Debug> std::fmt::Debug for TxWorkspace<C, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxWorkspace")
            .field("base_table_versions", &self.base_table_versions)
            .field("read_tables", &self.read_tables)
            .field("write_tables", &self.write_tables)
            .field("acquired_locks", &self.acquired_locks)
            .finish()
    }
}

impl<C: Clone, S: Clone> Clone for TxWorkspace<C, S> {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            storage: self.storage.clone(),
            base_table_versions: self.base_table_versions.clone(),
            read_tables: self.read_tables.clone(),
            write_tables: self.write_tables.clone(),
            acquired_locks: self.acquired_locks.clone(),
        }
    }
}

pub struct LockTable {
    locks: HashMap<String, TableLockState>,
    pub condvar: std::sync::Arc<parking_lot::Condvar>,
    pub wait_for_graph: super::deadlock::WaitForGraph,
}

impl Default for LockTable {
    fn default() -> Self {
        Self {
            locks: HashMap::new(),
            condvar: std::sync::Arc::new(parking_lot::Condvar::new()),
            wait_for_graph: super::deadlock::WaitForGraph::new(),
        }
    }
}

impl LockTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.locks.is_empty()
    }

    pub fn clear(&mut self) {
        self.locks.clear();
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.locks.keys()
    }

    pub fn acquire_statement_locks<C, S>(
        state_lock: &parking_lot::Mutex<LockTable>,
        session_id: SessionId,
        tx_manager: &super::transaction::TransactionManager<C, S, super::session::SessionSnapshot>,
        workspace_slot: &mut Option<TxWorkspace<C, S>>,
        stmt: &Statement,
        timeout_ms: i64,
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
                IsolationLevel::RepeatableRead
                    | IsolationLevel::Serializable
                    | IsolationLevel::Snapshot
            );

        let mut tables_to_lock = Vec::new();
        if read_lock_required {
            for table in read_tables {
                tables_to_lock.push((table, LockMode::Read));
            }
        }
        for table in write_tables {
            tables_to_lock.push((table, LockMode::Write));
        }

        if tables_to_lock.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();
        let mut guard = state_lock.lock();

        loop {
            let mut all_acquired = true;
            let mut conflict_info = None;
            let mut holders = std::collections::HashSet::new();

            // Try to acquire all locks
            for (table, mode) in &tables_to_lock {
                if !guard.can_acquire_lock(session_id, table, *mode) {
                    all_acquired = false;
                    conflict_info = Some((table.clone(), *mode));
                    for h in guard.get_blocking_sessions(session_id, table, *mode) {
                        holders.insert(h);
                    }
                    break;
                }
            }

            if all_acquired {
                guard.wait_for_graph.remove_waiter(session_id);
                for (table, mode) in tables_to_lock {
                    guard.perform_acquire_lock(session_id, workspace_slot, &table, mode, depth);
                }
                return Ok(());
            }

            if timeout_ms == 0 {
                let (table, mode) = conflict_info.unwrap();
                return Err(DbError::Execution(format!(
                    "lock conflict (no-wait): {:?} lock on '{}' is blocked",
                    mode, table
                )));
            }

            // Deadlock detection before waiting
            guard.wait_for_graph.remove_waiter(session_id);
            for &holder in &holders {
                guard.wait_for_graph.add_edge(session_id, holder);
            }

            if let Some(cycle) = guard.wait_for_graph.detect_cycle(session_id) {
                guard.wait_for_graph.remove_waiter(session_id);
                return Err(DbError::Deadlock(format!(
                    "Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Cycle: {:?}",
                    cycle
                )));
            }

            let elapsed = start.elapsed();
            if timeout_ms > 0 && elapsed.as_millis() >= timeout_ms as u128 {
                guard.wait_for_graph.remove_waiter(session_id);
                let (table, mode) = conflict_info.unwrap();
                return Err(DbError::Execution(format!(
                    "lock timeout ({}ms): {:?} lock on '{}' is blocked",
                    timeout_ms, mode, table
                )));
            }

            let condvar = guard.condvar.clone();
            if timeout_ms < 0 {
                condvar.wait(&mut guard);
            } else {
                let remaining = std::time::Duration::from_millis(timeout_ms as u64)
                    .saturating_sub(elapsed);
                if condvar.wait_for(&mut guard, remaining).timed_out() {
                    guard.wait_for_graph.remove_waiter(session_id);
                    let (table, mode) = conflict_info.unwrap();
                    return Err(DbError::Execution(format!(
                        "lock timeout ({}ms): {:?} lock on '{}' is blocked",
                        timeout_ms, mode, table
                    )));
                }
            }
        }
    }

    pub fn get_blocking_sessions(&self, session_id: SessionId, table: &str, mode: LockMode) -> Vec<SessionId> {
        let normalized = table.to_uppercase();
        let Some(lock_state) = self.locks.get(&normalized) else {
            return Vec::new();
        };

        let mut blockers = Vec::new();
        match mode {
            LockMode::Read => {
                if let Some((writer, _)) = lock_state.writer {
                    if writer != session_id {
                        blockers.push(writer);
                    }
                }
            }
            LockMode::Write => {
                if let Some((writer, _)) = lock_state.writer {
                    if writer != session_id {
                        blockers.push(writer);
                    }
                }
                for (reader, count) in &lock_state.readers {
                    if *reader != session_id && *count > 0 {
                        blockers.push(*reader);
                    }
                }
            }
        }
        blockers
    }

    fn can_acquire_lock(&self, session_id: SessionId, table: &str, mode: LockMode) -> bool {
        let normalized = table.to_uppercase();
        let Some(lock_state) = self.locks.get(&normalized) else {
            return true;
        };

        match mode {
            LockMode::Read => {
                if let Some((writer, _)) = lock_state.writer {
                    if writer != session_id {
                        return false;
                    }
                }
                true
            }
            LockMode::Write => {
                if let Some((writer, _)) = lock_state.writer {
                    if writer != session_id {
                        return false;
                    }
                }
                if lock_state
                    .readers
                    .iter()
                    .any(|(reader, count)| *reader != session_id && *count > 0)
                {
                    return false;
                }
                true
            }
        }
    }

    fn perform_acquire_lock<C, S>(
        &mut self,
        session_id: SessionId,
        workspace_slot: &mut Option<TxWorkspace<C, S>>,
        table: &str,
        mode: LockMode,
        savepoint_depth: usize,
    ) {
        let normalized = table.to_uppercase();
        let lock_state = self.locks.entry(normalized.clone()).or_default();

        match mode {
            LockMode::Read => {
                *lock_state.readers.entry(session_id).or_insert(0) += 1;
            }
            LockMode::Write => match lock_state.writer.as_mut() {
                Some((writer, count)) if *writer == session_id => {
                    *count += 1;
                }
                _ => {
                    lock_state.writer = Some((session_id, 1));
                }
            },
        }

        if let Some(workspace) = workspace_slot.as_mut() {
            workspace.acquired_locks.push(AcquiredLock {
                table: normalized,
                mode,
                savepoint_depth,
            });
        }
    }

    pub fn release_workspace_locks<C, S>(
        &mut self,
        session_id: SessionId,
        workspace_slot: &mut Option<TxWorkspace<C, S>>,
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
            self.release_lock_count(session_id, &lock.table, lock.mode);
        }
        workspace.acquired_locks = retained;
        self.condvar.notify_all();
    }

    pub fn release_all_for_session(&mut self, session_id: SessionId) {
        let tables: Vec<String> = self.locks.keys().cloned().collect();
        for table in tables {
            self.release_all_for_table(session_id, &table);
        }
        self.wait_for_graph.remove_waiter(session_id);
        self.condvar.notify_all();
    }

    fn release_all_for_table(&mut self, session_id: SessionId, table: &str) {
        let Some(lock_state) = self.locks.get_mut(table) else {
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
            self.locks.remove(table);
        }
    }

    fn release_lock_count(&mut self, session_id: SessionId, table: &str, mode: LockMode) {
        let Some(lock_state) = self.locks.get_mut(table) else {
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
            self.locks.remove(table);
        }
        self.condvar.notify_all();
    }
}
