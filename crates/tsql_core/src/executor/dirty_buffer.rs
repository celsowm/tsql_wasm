use crate::catalog::Catalog;
use crate::storage::Storage;

use super::locks::SessionId;
use super::session::SharedState;

/// Build a dirty-read view of storage and catalog by overlaying uncommitted
/// writes from all active transactions onto the committed shared state.
///
/// For each session that has an active workspace with write_tables, we replace
/// the corresponding table data in the cloned storage with the workspace's
/// version (which includes uncommitted changes).
pub(crate) fn build_dirty_read_storage<C, S>(
    state: &SharedState<C, S>,
    requesting_session_id: SessionId,
    requesting_workspace: &Option<super::locks::TxWorkspace<C, S>>,
) -> (C, S)
where
    C: Catalog + Clone,
    S: Storage + Clone,
{
    let mut merged_catalog = state.catalog.clone();
    let mut merged_storage = state.storage.clone();

    // Apply dirty writes from the requesting session's own workspace first
    if let Some(workspace) = requesting_workspace {
        if !workspace.write_tables.is_empty() {
            apply_workspace_writes(
                &workspace.catalog,
                &workspace.storage,
                &workspace.write_tables,
                &mut merged_catalog,
                &mut merged_storage,
            );
        }
    }

    // Apply dirty writes from all other sessions' workspaces
    for (&sid, session) in &state.sessions {
        if sid == requesting_session_id {
            continue;
        }
        if let Some(ref workspace) = session.workspace {
            if !workspace.write_tables.is_empty() {
                apply_workspace_writes(
                    &workspace.catalog,
                    &workspace.storage,
                    &workspace.write_tables,
                    &mut merged_catalog,
                    &mut merged_storage,
                );
            }
        }
    }

    (merged_catalog, merged_storage)
}

/// For each table in `write_tables`, copy table data from the workspace
/// storage/catalog into the merged storage/catalog.
fn apply_workspace_writes<C, S>(
    ws_catalog: &C,
    ws_storage: &S,
    write_tables: &std::collections::HashSet<String>,
    merged_catalog: &mut C,
    merged_storage: &mut S,
) where
    C: Catalog + Clone,
    S: Storage + Clone,
{
    for table_name in write_tables {
        // Look up the table definition in the workspace catalog to get its table_id
        if let Some(def) = ws_catalog.find_table("dbo", table_name) {
            let table_id = def.id;

            // If the table doesn't exist in merged catalog, register it
            // (it was created inside the transaction)
            if merged_catalog.find_table("dbo", table_name).is_none() {
                let tables = merged_catalog.get_tables_mut();
                tables.push(def.clone());
                merged_storage.ensure_table(table_id);
            }

            // Copy the workspace's row data for this table into merged storage
            if let Ok(rows) = ws_storage.get_rows(table_id) {
                merged_storage.ensure_table(table_id);
                // Use update_rows to replace all rows, but ensure_table guarantees entry exists
                let _ = merged_storage.update_rows(table_id, rows);
            }
        }
    }
}
