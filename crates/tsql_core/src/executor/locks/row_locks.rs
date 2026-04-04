use std::collections::HashMap;

use super::types::{LockMode, SessionId, TableLockState};
use super::super::string_norm::normalize_identifier;

/// Returned when a row lock acquisition triggers escalation.
pub(crate) struct EscalationRequest {
    pub table: String,
    pub mode: LockMode,
}

/// Manages row-level lock state and lock escalation.
pub(crate) struct RowLockManager {
    locks: HashMap<(String, usize), TableLockState>,
    lock_counts: HashMap<(SessionId, String), usize>,
    pub escalation_threshold: usize,
}

impl RowLockManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
            lock_counts: HashMap::new(),
            escalation_threshold: 5000,
        }
    }

    pub fn clear(&mut self) {
        self.locks.clear();
        self.lock_counts.clear();
    }

    pub fn can_acquire(
        &self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) -> bool {
        let normalized = normalize_identifier(table);
        let key = (normalized, row_id);
        match self.locks.get(&key) {
            Some(state) => !state.has_conflict(session_id, mode),
            None => true,
        }
    }

    /// Acquire a row lock. Returns an `EscalationRequest` if the threshold is reached.
    pub fn acquire(
        &mut self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) -> Option<EscalationRequest> {
        let normalized = normalize_identifier(table);
        let key = (normalized.clone(), row_id);
        self.locks
            .entry(key)
            .or_default()
            .acquire(session_id, mode);

        let count_key = (session_id, normalized.clone());
        let count = self.lock_counts.entry(count_key).or_insert(0);
        *count += 1;

        if *count >= self.escalation_threshold {
            Some(EscalationRequest {
                table: normalized,
                mode,
            })
        } else {
            None
        }
    }

    pub fn get_blocking_sessions(
        &self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) -> Vec<SessionId> {
        let normalized = normalize_identifier(table);
        let key = (normalized, row_id);
        match self.locks.get(&key) {
            Some(state) => state.collect_blockers(session_id, mode),
            None => Vec::new(),
        }
    }

    pub fn release_one(
        &mut self,
        session_id: SessionId,
        table: &str,
        row_id: usize,
        mode: LockMode,
    ) {
        let normalized = normalize_identifier(table);
        let key = (normalized.clone(), row_id);

        if let Some(state) = self.locks.get_mut(&key) {
            state.release_one(session_id, mode);
            if state.is_empty() {
                self.locks.remove(&key);
            }
        }

        let count_key = (session_id, normalized);
        if let Some(count) = self.lock_counts.get_mut(&count_key) {
            if *count > 1 {
                *count -= 1;
            } else {
                self.lock_counts.remove(&count_key);
            }
        }
    }

    pub fn release_all_for_session(&mut self, session_id: SessionId) {
        let row_keys: Vec<(String, usize)> = self.locks.keys().cloned().collect();
        for key in row_keys {
            if let Some(state) = self.locks.get_mut(&key) {
                state.release_all(session_id);
                if state.is_empty() {
                    self.locks.remove(&key);
                }
            }
        }
        self.lock_counts.retain(|(sid, _), _| *sid != session_id);
    }

    /// Perform escalation: remove all row locks for this session+table.
    /// The caller is responsible for adding the table-level lock.
    pub fn escalate_remove_rows(&mut self, session_id: SessionId, table: &str) {
        let normalized = normalize_identifier(table);

        let row_keys: Vec<(String, usize)> = self
            .locks
            .keys()
            .filter(|(t, _)| *t == normalized)
            .cloned()
            .collect();

        for key in row_keys {
            if let Some(state) = self.locks.get_mut(&key) {
                state.release_all(session_id);
                if state.is_empty() {
                    self.locks.remove(&key);
                }
            }
        }

        self.lock_counts.remove(&(session_id, normalized));
    }
}
