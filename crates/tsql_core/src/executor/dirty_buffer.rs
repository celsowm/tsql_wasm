use std::collections::HashMap;
use crate::catalog::Catalog;
use crate::storage::{Storage, StoredRow};

use super::locks::SessionId;
use super::session::SharedState;

#[derive(Debug, Clone)]
pub enum DirtyOp {
    Insert { row: StoredRow },
    Update { row_index: usize, new_row: StoredRow },
    Delete { row_index: usize },
    Truncate,
    ReplaceTable { rows: Vec<StoredRow> },
}

#[derive(Debug, Default, Clone)]
pub struct DirtyBuffer {
    /// session_id -> table_name -> Vec<DirtyOp>
    pub pending: HashMap<SessionId, HashMap<String, Vec<DirtyOp>>>,
}

impl DirtyBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear_session(&mut self, session_id: SessionId) {
        self.pending.remove(&session_id);
    }

    pub fn push_op(&mut self, session_id: SessionId, table_name: String, op: DirtyOp) {
        self.pending
            .entry(session_id)
            .or_default()
            .entry(table_name)
            .or_default()
            .push(op);
    }
}

/// Build a dirty-read view of storage and catalog by overlaying uncommitted
/// writes from the shared dirty_buffer onto the committed shared state.
pub(crate) fn build_dirty_read_storage<C, S>(
    state: &SharedState<C, S>,
    requesting_session_id: SessionId,
    requesting_workspace: &Option<super::locks::TxWorkspace<C, S>>,
) -> (C, S)
where
    C: Catalog + Clone,
    S: Storage + Clone,
{
    let (mut merged_catalog, mut merged_storage) = {
        let storage_guard = state.storage.read();
        (storage_guard.catalog.clone(), storage_guard.storage.clone())
    };

    // 1. Merge catalogs from all active workspaces
    // This allows seeing uncommitted tables/columns.
    for entry in state.sessions.iter() {
        let sid = *entry.key();
        if sid == requesting_session_id {
            if let Some(workspace) = requesting_workspace {
                merge_catalog(&mut merged_catalog, &workspace.catalog);
            }
            continue;
        }
        let session = entry.value().lock();
        if let Some(ref workspace) = session.workspace {
            merge_catalog(&mut merged_catalog, &workspace.catalog);
        }
    }

    // 2. Apply all dirty ops from the shared buffer
    let buffer = state.dirty_buffer.lock();
    for session_ops in buffer.pending.values() {
        for (table_name, ops) in session_ops {
            if let Some(def) = merged_catalog.find_table("dbo", table_name) {
                let table_id = def.id;
                merged_storage.ensure_table(table_id);
                for op in ops {
                    match op {
                        DirtyOp::Insert { row } => {
                            let _ = merged_storage.insert_row(table_id, row.clone());
                        }
                        DirtyOp::Update { row_index, new_row } => {
                            let _ = merged_storage.update_row(table_id, *row_index, new_row.clone());
                        }
                        DirtyOp::Delete { row_index } => {
                            let _ = merged_storage.delete_row(table_id, *row_index);
                        }
                        DirtyOp::Truncate => {
                            let _ = merged_storage.clear_table(table_id);
                        }
                        DirtyOp::ReplaceTable { rows } => {
                            let _ = merged_storage.update_rows(table_id, rows.clone());
                        }
                    }
                }
            }
        }
    }

    (merged_catalog, merged_storage)
}

fn merge_catalog<C: Catalog + Clone>(target: &mut C, source: &C) {
    for table in source.get_tables() {
        if target.find_table(table.schema_or_dbo(), &table.name).is_none() {
            target.get_tables_mut().push(table.clone());
        }
    }
    // Could also merge routines, triggers, etc. if needed
}
