use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

use crate::error::DbError;
use crate::storage::{Storage, StorageCheckpointData, StoredRow};
use redb::{AccessGuard, Database, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

const ROWS_TABLE: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("rows");
const TABLE_META: TableDefinition<u32, u64> = TableDefinition::new("rows_meta");

#[derive(Debug, Clone, Default)]
pub struct RedbStorage {
    db: Option<Arc<Database>>,
}

impl Serialize for RedbStorage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for RedbStorage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _: () = serde::Deserialize::deserialize(deserializer)?;
        Ok(Self { db: None })
    }
}

impl RedbStorage {
    fn storage_err(context: &str, err: impl Display) -> DbError {
        DbError::Storage(format!("{context}: {err}"))
    }

    fn serialize_row(row: &StoredRow) -> Result<Vec<u8>, DbError> {
        bincode::serde::encode_to_vec(row, bincode::config::standard())
            .map_err(|e| Self::storage_err("failed to serialize row", e))
    }

    fn deserialize_row(bytes: &[u8]) -> Result<StoredRow, DbError> {
        match bincode::serde::decode_from_slice(bytes, bincode::config::standard()) {
            Ok((row, _)) => Ok(row),
            Err(_) => serde_json::from_slice(bytes)
                .map_err(|e| Self::storage_err("failed to deserialize row", e)),
        }
    }

    fn with_write_txn<R>(
        db: &Database,
        op: impl FnOnce(&mut WriteTransaction) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        let mut write_txn = db
            .begin_write()
            .map_err(|e| Self::storage_err("failed to begin write txn", e))?;
        let result = op(&mut write_txn)?;
        write_txn
            .commit()
            .map_err(|e| Self::storage_err("failed to commit txn", e))?;
        Ok(result)
    }

    fn delete_rows_in_table(
        table: &mut redb::Table<'_, (u32, u64), &[u8]>,
        table_id: u32,
    ) -> Result<(), DbError> {
        let range = if let Some(next_table_id) = table_id.checked_add(1) {
            table
                .range((table_id, 0)..(next_table_id, 0))
                .map_err(|e| Self::storage_err("failed to scan range", e))?
        } else {
            table
                .range((table_id, 0)..)
                .map_err(|e| Self::storage_err("failed to scan range", e))?
        };
        let keys_to_delete = range
            .flatten()
            .map(|(k, _v): (AccessGuard<'_, (u32, u64)>, AccessGuard<'_, &[u8]>)| k.value())
            .collect::<Vec<_>>();

        for key in keys_to_delete {
            table
                .remove(key)
                .map_err(|e| Self::storage_err("failed to remove row", e))?;
        }

        Ok(())
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DbError> {
        let db = Database::create(path)
            .map_err(|e| Self::storage_err("failed to create redb database", e))?;

        // Ensure physical tables exist.
        Self::with_write_txn(&db, |write_txn| {
            let _ = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open rows table", e))?;
            let _ = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open rows meta table", e))?;
            Ok(())
        })?;

        Ok(Self {
            db: Some(Arc::new(db)),
        })
    }

    fn db(&self) -> Result<&Database, DbError> {
        self.db.as_deref().ok_or_else(|| {
            DbError::Storage("RedbStorage not initialized (handle is missing)".into())
        })
    }
}

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

            Self::delete_rows_in_table(&mut table, table_id)?;

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

            Self::delete_rows_in_table(&mut table, table_id)?;

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

            Self::delete_rows_in_table(&mut table, table_id)?;
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
}

impl crate::storage::CheckpointableStorage for RedbStorage {
    fn get_checkpoint_data(&self) -> StorageCheckpointData {
        StorageCheckpointData::Persistent
    }

    fn restore_from_checkpoint(&mut self, _data: StorageCheckpointData) -> Result<(), DbError> {
        Ok(())
    }

    fn clone_checkpointable(&self) -> Box<dyn crate::storage::CheckpointableStorage> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_row(v: i32) -> StoredRow {
        StoredRow {
            values: vec![crate::types::Value::Int(v)],
            deleted: false,
        }
    }

    fn meta_value(storage: &RedbStorage, table_id: u32) -> Option<u64> {
        let db = storage.db.as_ref().expect("storage db missing");
        let read_txn = db.begin_read().expect("failed to begin read txn");
        let meta = read_txn
            .open_table(TABLE_META)
            .expect("failed to open meta table");
        meta.get(table_id)
            .expect("failed to read meta")
            .map(|value| value.value())
    }

    #[test]
    fn clear_table_preserves_meta_and_allows_reuse() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::new(dir.path().join("data.redb")).unwrap();
        let table_id = 7;

        storage.ensure_table(table_id).unwrap();
        storage.insert_row(table_id, sample_row(10)).unwrap();
        storage.insert_row(table_id, sample_row(20)).unwrap();

        storage.clear_table(table_id).unwrap();

        let rows: Vec<_> = storage.get_rows(table_id).unwrap();
        assert!(rows.is_empty());
        assert_eq!(meta_value(&storage, table_id), Some(0));

        storage.insert_row(table_id, sample_row(30)).unwrap();
        let rows: Vec<_> = storage.get_rows(table_id).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], crate::types::Value::Int(30));
        assert_eq!(meta_value(&storage, table_id), Some(1));
    }

    #[test]
    fn remove_table_drops_meta_and_rows() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::new(dir.path().join("data.redb")).unwrap();
        let table_id = 11;

        storage.ensure_table(table_id).unwrap();
        storage.insert_row(table_id, sample_row(40)).unwrap();

        storage.remove_table(table_id).unwrap();

        let rows: Vec<_> = storage.get_rows(table_id).unwrap();
        assert!(rows.is_empty());
        assert_eq!(meta_value(&storage, table_id), None);

        storage.ensure_table(table_id).unwrap();
        assert_eq!(meta_value(&storage, table_id), Some(0));
        storage.insert_row(table_id, sample_row(50)).unwrap();
        let rows: Vec<_> = storage.get_rows(table_id).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], crate::types::Value::Int(50));
    }
}
