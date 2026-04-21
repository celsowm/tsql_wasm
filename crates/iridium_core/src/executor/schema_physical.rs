use crate::error::DbError;
use crate::types::Value;

use super::schema::SchemaExecutor;

pub(crate) fn rebuild_index_for_table(
    executor: &mut SchemaExecutor<'_>,
    index_def_id: u32,
    table_id: u32,
) {
    let rows = match executor.storage.get_rows(table_id) {
        Ok(rows) => rows,
        Err(_) => return,
    };
    let row_refs: Vec<(usize, &[Value])> = rows
        .iter()
        .enumerate()
        .map(|(i, r)| (i, r.values.as_slice()))
        .collect();

    if let Some(index_storage) = executor.storage.as_index_storage_mut() {
        let index_def = executor
            .catalog
            .get_indexes()
            .iter()
            .find(|idx| idx.id == index_def_id)
            .cloned();

        if let Some(index_def) = index_def {
            index_storage.register_index(
                index_def_id,
                index_def.column_ids.clone(),
                index_def.is_unique,
                index_def.is_clustered,
            );

            if let Some(idx) = index_storage.get_index_mut(index_def_id) {
                let _ = idx.rebuild_from_rows(row_refs.as_slice());
            }
        }
    }
}

pub(crate) fn add_null_column_to_table(
    executor: &mut SchemaExecutor<'_>,
    table_id: u32,
) -> Result<(), DbError> {
    let mut rows_vec = {
        let rows = match executor.storage.scan_rows(table_id) {
            Ok(rows) => rows,
            Err(_) => return Ok(()),
        };
        match rows.collect::<Result<Vec<_>, DbError>>() {
            Ok(rows_vec) => rows_vec,
            Err(_) => return Ok(()),
        }
    };
    for row in rows_vec.iter_mut() {
        row.values.push(Value::Null);
    }
    executor.storage.replace_table(table_id, rows_vec)
}

pub(crate) fn drop_column_from_table(
    executor: &mut SchemaExecutor<'_>,
    table_id: u32,
    col_idx: usize,
) -> Result<(), DbError> {
    let mut rows_vec = {
        let rows = match executor.storage.scan_rows(table_id) {
            Ok(rows) => rows,
            Err(_) => return Ok(()),
        };
        match rows.collect::<Result<Vec<_>, DbError>>() {
            Ok(rows_vec) => rows_vec,
            Err(_) => return Ok(()),
        }
    };
    for row in rows_vec.iter_mut() {
        if col_idx < row.values.len() {
            row.values.remove(col_idx);
        }
    }
    executor.storage.replace_table(table_id, rows_vec)
}
