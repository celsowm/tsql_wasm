use redb::{AccessGuard, ReadableTable};

use crate::error::DbError;
use crate::storage::{Storage, StoredRow};

use super::redb_storage::{RedbStorage, ROWS_TABLE, TABLE_META};

impl Storage for RedbStorage {
    fn scan_rows<'a>(
        &'a self,
        table_id: u32,
    ) -> Result<crate::storage::StorageRowStream<'a>, DbError> {
        let db = self.db()?;
        let read_txn = db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read txn", e))?;
        let table = read_txn
            .open_table(ROWS_TABLE)
            .map_err(|e| Self::storage_err("failed to open table", e))?;
        let range = if let Some(next_table_id) = table_id.checked_add(1) {
            table
                .range((table_id, 0)..(next_table_id, 0))
                .map_err(|e| Self::storage_err("failed to scan range", e))?
        } else {
            table
                .range((table_id, 0)..)
                .map_err(|e| Self::storage_err("failed to scan range", e))?
        };

        Ok(Box::new(range.map(|result| {
            let (_key, value): (AccessGuard<'_, (u32, u64)>, AccessGuard<'_, &[u8]>) =
                result.map_err(|e| Self::storage_err("error reading row", e))?;
            Self::deserialize_row(value.value())
        })))
    }

    fn get_row(&self, table_id: u32, index: usize) -> Result<Option<StoredRow>, DbError> {
        let db = self.db()?;
        let read_txn = db
            .begin_read()
            .map_err(|e| Self::storage_err("failed to begin read txn", e))?;
        let table = read_txn
            .open_table(ROWS_TABLE)
            .map_err(|e| Self::storage_err("failed to open table", e))?;
        let result = table
            .get((table_id, index as u64))
            .map_err(|e| Self::storage_err("failed to get row", e))?;
        match result {
            Some(value) => Ok(Some(Self::deserialize_row(value.value())?)),
            None => Ok(None),
        }
    }

    fn insert_row(&mut self, table_id: u32, row: StoredRow) -> Result<(), DbError> {
        let db = self.db()?;
        Self::with_write_txn(db, |write_txn| {
            let mut table = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open table", e))?;
            let mut meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open meta table", e))?;

            let current_next_idx = meta
                .get(table_id)
                .map_err(|e| Self::storage_err("failed to read next row index", e))?
                .map(|value| value.value());

            let next_idx = match current_next_idx {
                Some(next_idx) => next_idx,
                None => {
                    let mut range = if let Some(next_table_id) = table_id.checked_add(1) {
                        table
                            .range((table_id, 0)..(next_table_id, 0))
                            .map_err(|e| Self::storage_err("failed to scan range", e))?
                    } else {
                        table
                            .range((table_id, 0)..)
                            .map_err(|e| Self::storage_err("failed to scan range", e))?
                    };
                    let next_idx = match range.next_back() {
                        Some(Ok((key, _val))) => key
                            .value()
                            .1
                            .checked_add(1)
                            .ok_or_else(|| DbError::Storage("row index overflow".into()))?,
                        _ => 0,
                    };
                    meta.insert(table_id, &next_idx)
                        .map_err(|e| Self::storage_err("failed to initialize next row index", e))?;
                    next_idx
                }
            };

            let row_bytes = Self::serialize_row(&row)?;

            table
                .insert((table_id, next_idx), row_bytes.as_slice())
                .map_err(|e| Self::storage_err("failed to insert row", e))?;

            let updated_next_idx = next_idx
                .checked_add(1)
                .ok_or_else(|| DbError::Storage("row index overflow".into()))?;
            meta.insert(table_id, &updated_next_idx)
                .map_err(|e| Self::storage_err("failed to advance next row index", e))?;
            Ok(())
        })?;
        Ok(())
    }

    fn update_row(&mut self, table_id: u32, index: usize, row: StoredRow) -> Result<(), DbError> {
        let db = self.db()?;
        Self::with_write_txn(db, |write_txn| {
            let mut table = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open table", e))?;
            let _meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open meta table", e))?;

            let row_bytes = Self::serialize_row(&row)?;

            table
                .insert((table_id, index as u64), row_bytes.as_slice())
                .map_err(|e| Self::storage_err("failed to update row", e))?;
            Ok(())
        })?;
        Ok(())
    }

    fn delete_row(&mut self, table_id: u32, index: usize) -> Result<(), DbError> {
        let db = self.db()?;
        Self::with_write_txn(db, |write_txn| {
            let mut table = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open table", e))?;
            let _meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open meta table", e))?;

            let row_opt = {
                let current_val = table
                    .get((table_id, index as u64))
                    .map_err(|e| Self::storage_err("failed to get row", e))?;

                if let Some(val) = current_val {
                    let mut row = Self::deserialize_row(val.value())?;
                    row.deleted = true;
                    Some(row)
                } else {
                    None
                }
            };

            if let Some(row) = row_opt {
                let row_bytes = Self::serialize_row(&row)?;
                table
                    .insert((table_id, index as u64), row_bytes.as_slice())
                    .map_err(|e| Self::storage_err("failed to delete row", e))?;
            }
            Ok(())
        })?;
        Ok(())
    }

    fn replace_table(&mut self, table_id: u32, rows: Vec<StoredRow>) -> Result<(), DbError> {
        let db = self.db()?;
        Self::with_write_txn(db, |write_txn| {
            let mut table = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open table", e))?;
            let mut meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open meta table", e))?;

            RedbStorage::delete_rows_in_table(&mut table, table_id)?;

            let next_idx = rows.len() as u64;
            meta.insert(table_id, &next_idx)
                .map_err(|e| Self::storage_err("failed to reset next row index", e))?;

            for (idx, row) in rows.into_iter().enumerate() {
                let row_bytes = Self::serialize_row(&row)?;
                table
                    .insert((table_id, idx as u64), row_bytes.as_slice())
                    .map_err(|e| Self::storage_err("failed to insert row", e))?;
            }
            Ok(())
        })?;
        Ok(())
    }

    fn clear_table(&mut self, table_id: u32) -> Result<(), DbError> {
        let db = self.db()?;
        Self::with_write_txn(db, |write_txn| {
            let mut table = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open table", e))?;
            let mut meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open meta table", e))?;

            RedbStorage::delete_rows_in_table(&mut table, table_id)?;

            let next_idx = 0u64;
            meta.insert(table_id, &next_idx)
                .map_err(|e| Self::storage_err("failed to reset next row index", e))?;
            Ok(())
        })?;
        Ok(())
    }

    fn remove_table(&mut self, table_id: u32) -> Result<(), DbError> {
        let db = self.db()?;
        Self::with_write_txn(db, |write_txn| {
            let mut table = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open table", e))?;
            let mut meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open meta table", e))?;

            RedbStorage::delete_rows_in_table(&mut table, table_id)?;
            meta.remove(table_id)
                .map_err(|e| Self::storage_err("failed to remove next row index", e))?;
            Ok(())
        })?;
        Ok(())
    }

    fn ensure_table(&mut self, table_id: u32) -> Result<(), DbError> {
        let db = self.db()?;
        Self::with_write_txn(db, |write_txn| {
            let _ = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open rows table", e))?;
            let mut meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open meta table", e))?;
            if meta
                .get(table_id)
                .map_err(|e| Self::storage_err("failed to read next row index", e))?
                .is_none()
            {
                let next_idx = 0u64;
                meta.insert(table_id, &next_idx)
                    .map_err(|e| Self::storage_err("failed to initialize next row index", e))?;
            }
            Ok(())
        })?;
        Ok(())
    }

    fn clone_boxed(&self) -> Box<dyn Storage> {
        Box::new(self.clone())
    }

    fn as_index_storage(&self) -> Option<&dyn crate::storage::IndexStorage> {
        Some(self)
    }

    fn as_index_storage_mut(&mut self) -> Option<&mut dyn crate::storage::IndexStorage> {
        Some(self)
    }
}
