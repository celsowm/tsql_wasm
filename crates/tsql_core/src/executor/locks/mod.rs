mod types;
mod workspace;
pub(crate) mod table_locks;
pub(crate) mod row_locks;

pub use types::{AcquiredLock, LockMode, LockResource, SessionId, TableLockState};
pub use workspace::TxWorkspace;

use crate::ast::{IsolationLevel, Statement};
use crate::error::DbError;

use super::table_util::{collect_read_tables, collect_write_tables};
use table_locks::TableLockManager;
use row_locks::RowLockManager;

/// Unified lock manager that delegates to table-level and row-level sub-managers.
pub struct LockTable {
    tables: TableLockManager,
    rows: RowLockManager,
    pub condvar: std::sync::Arc<parking_lot::Condvar>,
    pub wait_for_graph: super::deadlock::WaitForGraph,
}

impl Default for LockTable {
    fn default() -> Self {
        Self {
            tables: TableLockManager::new(),
            rows: RowLockManager::new(),
            condvar: std::sync::Arc::new(parking_lot::Condvar::new()),
            wait_for_graph: super::deadlock::WaitForGraph::new(),
        }
    }
}

// ── Public property access ───────────────────────────────────────

impl LockTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn clear(&mut self) {
        self.tables.clear();
        self.rows.clear();
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.tables.keys()
    }

    pub fn set_escalation_threshold(&mut self, threshold: usize) {
        self.rows.escalation_threshold = threshold;
    }
}

// ── Statement-level table locking ────────────────────────────────

impl LockTable {
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

            for (table, mode) in &tables_to_lock {
                if !guard.tables.can_acquire(session_id, table, *mode) {
                    all_acquired = false;
                    conflict_info = Some((table.clone(), *mode));
                    for h in guard.tables.get_blocking_sessions(session_id, table, *mode) {
                        holders.insert(h);
                    }
                    break;
                }
            }

            if all_acquired {
                guard.wait_for_graph.remove_waiter(session_id);
                for (table, mode) in tables_to_lock {
                    guard
                        .tables
                        .acquire(session_id, workspace_slot, &table, mode, depth);
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

    pub fn get_blocking_sessions(
        &self,
        session_id: SessionId,
        table: &str,
        mode: LockMode,
    ) -> Vec<SessionId> {
        self.tables.get_blocking_sessions(session_id, table, mode)
    }

    pub fn perform_acquire_lock<C, S>(
        &mut self,
        session_id: SessionId,
        workspace_slot: &mut Option<TxWorkspace<C, S>>,
        table: &str,
        mode: LockMode,
        savepoint_depth: usize,
    ) {
        self.tables
            .acquire(session_id, workspace_slot, table, mode, savepoint_depth);
    }
}

// ── Row-level locking ────────────────────────────────────────────

impl LockTable {
    /// Check if a row lock can be acquired (considering both table- and row-level state).
    fn can_acquire_row(
        &self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) -> bool {
        // A conflicting table-level lock blocks any row lock
        if !self.tables.can_acquire(session_id, table, mode) {
            return false;
        }
        self.rows.can_acquire(session_id, table, row_id, mode)
    }

    /// Acquire a row lock and handle escalation if triggered.
    fn do_acquire_row(
        &mut self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) {
        if let Some(req) = self.rows.acquire(session_id, table, row_id, mode) {
            // Escalation triggered: remove row locks, promote to table lock
            self.rows.escalate_remove_rows(session_id, &req.table);
            self.tables.acquire_raw(session_id, &req.table, req.mode);
        }
    }

    /// Collect all sessions blocking a row lock (table-level + row-level).
    fn row_blocking_sessions(
        &self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) -> Vec<SessionId> {
        let mut blockers = self.tables.get_blocking_sessions(session_id, table, mode);
        for b in self.rows.get_blocking_sessions(session_id, table, row_id, mode) {
            if !blockers.contains(&b) {
                blockers.push(b);
            }
        }
        blockers
    }

    pub fn acquire_row_lock(
        state_lock: &parking_lot::Mutex<LockTable>,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
        timeout_ms: i64,
    ) -> Result<(), DbError> {
        let start = std::time::Instant::now();
        let mut guard = state_lock.lock();

        loop {
            if guard.can_acquire_row(session_id, table, row_id, mode) {
                guard.wait_for_graph.remove_waiter(session_id);
                guard.do_acquire_row(session_id, table, row_id, mode);
                return Ok(());
            }

            if timeout_ms == 0 {
                return Err(DbError::Execution(format!(
                    "lock conflict (no-wait): {:?} row lock on '{}' row {} is blocked",
                    mode, table, row_id
                )));
            }

            let holders = guard.row_blocking_sessions(session_id, table, row_id, mode);
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
                return Err(DbError::Execution(format!(
                    "lock timeout ({}ms): {:?} row lock on '{}' row {} is blocked",
                    timeout_ms, mode, table, row_id
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
                    return Err(DbError::Execution(format!(
                        "lock timeout ({}ms): {:?} row lock on '{}' row {} is blocked",
                        timeout_ms, mode, table, row_id
                    )));
                }
            }
        }
    }

    pub fn release_row_lock(
        &mut self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) {
        self.rows.release_one(session_id, table, row_id, mode);
        self.condvar.notify_all();
    }

    pub fn get_row_blocking_sessions(
        &self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) -> Vec<SessionId> {
        self.row_blocking_sessions(session_id, table, row_id, mode)
    }
}

// ── Workspace / session release ──────────────────────────────────

impl LockTable {
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
            match &lock.resource {
                LockResource::Table(t) => {
                    self.tables.release_one(session_id, t, lock.mode);
                }
                LockResource::Row(t, row_id) => {
                    self.rows.release_one(session_id, t, *row_id, lock.mode);
                }
            }
        }
        workspace.acquired_locks = retained;
        self.condvar.notify_all();
    }

    pub fn release_all_for_session(&mut self, session_id: SessionId) {
        self.tables.release_all_for_session(session_id);
        self.rows.release_all_for_session(session_id);
        self.wait_for_graph.remove_waiter(session_id);
        self.condvar.notify_all();
    }
}
