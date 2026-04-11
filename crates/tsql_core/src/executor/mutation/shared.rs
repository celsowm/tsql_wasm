use std::collections::HashSet;

use crate::error::DbError;

use super::super::context::ExecutionContext;
use crate::executor::model::JoinedRow;
use crate::storage::StoredRow;

pub(crate) fn rowcount_limit(ctx: &ExecutionContext<'_>) -> Option<usize> {
    if ctx.options.rowcount == 0 {
        None
    } else {
        Some(ctx.options.rowcount as usize)
    }
}

pub(crate) fn find_target_row<'a>(
    joined_row: &'a JoinedRow,
    table_id: u32,
    target_alias: &str,
) -> Result<&'a crate::executor::model::ContextTable, DbError> {
    joined_row
        .iter()
        .find(|ct| ct.table.id == table_id && ct.alias.eq_ignore_ascii_case(target_alias))
        .or_else(|| joined_row.iter().find(|ct| ct.table.id == table_id))
        .ok_or_else(|| DbError::Execution("target table not found in join context".into()))
}

pub(crate) fn dedupe_row_index(indices: &mut HashSet<usize>, idx: usize) -> bool {
    if indices.contains(&idx) {
        false
    } else {
        indices.insert(idx);
        true
    }
}

pub(crate) fn visit_target_rows<F>(
    joined_rows: Vec<JoinedRow>,
    table_id: u32,
    target_alias: &str,
    rowcount_limit: Option<usize>,
    visited_indices: &mut HashSet<usize>,
    mut visitor: F,
) -> Result<usize, DbError>
where
    F: FnMut(&StoredRow, usize, &JoinedRow) -> Result<(), DbError>,
{
    let mut processed = 0usize;

    for joined_row in joined_rows {
        if let Some(limit) = rowcount_limit {
            if processed >= limit {
                break;
            }
        }

        let target_ctx = find_target_row(&joined_row, table_id, target_alias)?;

        if let (Some(stored_row), Some(idx)) = (&target_ctx.row, target_ctx.storage_index) {
            if dedupe_row_index(visited_indices, idx) {
                visitor(stored_row, idx, &joined_row)?;
                processed += 1;
            }
        }
    }

    Ok(processed)
}
