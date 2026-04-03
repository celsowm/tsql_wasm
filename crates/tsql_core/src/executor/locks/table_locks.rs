use std::collections::HashMap;

use super::types::{LockMode, LockResource, SessionId, TableLockState};
use super::workspace::TxWorkspace;
use super::AcquiredLock;

/// Manages table-level lock state.
pub(crate) struct TableLockManager {
    locks: HashMap<String, TableLockState>,
}

impl TableLockManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
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

    pub fn can_acquire(&self, session_id: SessionId, table: &str, mode: LockMode) -> bool {
        let normalized = table.to_uppercase();
        match self.locks.get(&normalized) {
            Some(state) => !state.has_conflict(session_id, mode),
            None => true,
        }
    }

    pub fn get_blocking_sessions(
        &self,
        session_id: SessionId,
        table: &str,
        mode: LockMode,
    ) -> Vec<SessionId> {
        let normalized = table.to_uppercase();
        match self.locks.get(&normalized) {
            Some(state) => state.collect_blockers(session_id, mode),
            None => Vec::new(),
        }
    }

    pub fn acquire<C, S>(
        &mut self,
        session_id: SessionId,
        workspace_slot: &mut Option<TxWorkspace<C, S>>,
        table: &str,
        mode: LockMode,
        savepoint_depth: usize,
    ) {
        let normalized = table.to_uppercase();
        self.locks
            .entry(normalized.clone())
            .or_default()
            .acquire(session_id, mode);

        if let Some(workspace) = workspace_slot.as_mut() {
            workspace.acquired_locks.push(AcquiredLock {
                resource: LockResource::Table(normalized),
                mode,
                savepoint_depth,
            });
        }
    }

    /// Acquire a table lock without recording in workspace (used by escalation).
    pub fn acquire_raw(&mut self, session_id: SessionId, table: &str, mode: LockMode) {
        let normalized = table.to_uppercase();
        self.locks
            .entry(normalized)
            .or_default()
            .acquire(session_id, mode);
    }

    pub fn release_one(&mut self, session_id: SessionId, table: &str, mode: LockMode) {
        let Some(state) = self.locks.get_mut(table) else {
            return;
        };
        state.release_one(session_id, mode);
        if state.is_empty() {
            self.locks.remove(table);
        }
    }

    pub fn release_all_for_session(&mut self, session_id: SessionId) {
        let tables: Vec<String> = self.locks.keys().cloned().collect();
        for table in tables {
            if let Some(state) = self.locks.get_mut(&table) {
                state.release_all(session_id);
                if state.is_empty() {
                    self.locks.remove(&table);
                }
            }
        }
    }
}
