use std::collections::BTreeMap;
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

use crate::error::DbError;
use crate::storage::{BTreeIndex, IndexStorage, Storage, StorageCheckpointData, StoredRow};
use crate::types::Value;
use redb::{AccessGuard, Database, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

const ROWS_TABLE: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("rows");
const TABLE_META: TableDefinition<u32, u64> = TableDefinition::new("rows_meta");

#[derive(Debug, Clone, Default)]
pub struct RedbStorage {
    db: Option<Arc<Database>>,
    indexes: BTreeMap<u32, BTreeIndex>,
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
        Ok(Self {
            db: None,
            indexes: BTreeMap::new(),
        })
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
            indexes: BTreeMap::new(),
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

    fn as_index_storage(&self) -> Option<&dyn IndexStorage> {
        Some(self)
    }

    fn as_index_storage_mut(&mut self) -> Option<&mut dyn IndexStorage> {
        Some(self)
    }
}

impl RedbStorage {
    pub fn register_index(
        &mut self,
        index_id: u32,
        column_ids: Vec<u32>,
        is_unique: bool,
        is_clustered: bool,
    ) {
        self.indexes.insert(
            index_id,
            BTreeIndex::new(column_ids, is_unique, is_clustered),
        );
    }

    pub fn index_for_table(&self, _table_id: u32) -> Vec<&BTreeIndex> {
        self.indexes.values().collect()
    }
}

impl IndexStorage for RedbStorage {
    fn register_index(
        &mut self,
        index_id: u32,
        column_ids: Vec<u32>,
        is_unique: bool,
        is_clustered: bool,
    ) {
        self.indexes.insert(
            index_id,
            BTreeIndex::new(column_ids, is_unique, is_clustered),
        );
    }

    fn get_index(&self, index_id: u32) -> Option<&BTreeIndex> {
        self.indexes.get(&index_id)
    }

    fn get_index_mut(&mut self, index_id: u32) -> Option<&mut BTreeIndex> {
        self.indexes.get_mut(&index_id)
    }

    fn seek_index(&self, index_id: u32, key: &Value) -> Result<Vec<usize>, DbError> {
        let index = self
            .indexes
            .get(&index_id)
            .ok_or_else(|| DbError::Storage(format!("index {} not found", index_id)))?;

        let result = index.seek(key).map(|v| v.clone()).unwrap_or_default();
        Ok(result)
    }

    fn seek_index_range(
        &self,
        index_id: u32,
        lower: Option<&Value>,
        upper: Option<&Value>,
    ) -> Result<Vec<(Value, Vec<usize>)>, DbError> {
        let index = self
            .indexes
            .get(&index_id)
            .ok_or_else(|| DbError::Storage(format!("index {} not found", index_id)))?;

        let entries = index.seek_range(lower, upper);
        let mut result = Vec::new();
        for (key, indices) in entries {
            if let Some(first_val) = key.as_values().first() {
                result.push((first_val.clone(), indices));
            }
        }
        Ok(result)
    }

    fn insert_index_entry(
        &mut self,
        index_id: u32,
        _key: Value,
        _row_index: usize,
    ) -> Result<(), DbError> {
        let _index = self
            .indexes
            .get_mut(&index_id)
            .ok_or_else(|| DbError::Storage(format!("index {} not found", index_id)))?;

        Ok(())
    }

    fn delete_index_entry(
        &mut self,
        index_id: u32,
        _key: &Value,
        _row_index: usize,
    ) -> Result<(), DbError> {
        let _index = self
            .indexes
            .get_mut(&index_id)
            .ok_or_else(|| DbError::Storage(format!("index {} not found", index_id)))?;

        Ok(())
    }

    fn rebuild_index(
        &mut self,
        index_id: u32,
        entries: Vec<(Value, usize)>,
    ) -> Result<(), DbError> {
        let index = self
            .indexes
            .get_mut(&index_id)
            .ok_or_else(|| DbError::Storage(format!("index {} not found", index_id)))?;

        index.clear();
        for (key, row_index) in entries {
            index.insert(row_index, &[key])?;
        }
        Ok(())
    }
}

impl crate::storage::CheckpointableStorage for RedbStorage {
    fn get_checkpoint_data(&self) -> StorageCheckpointData {
        let Ok(db) = self.db() else {
            return StorageCheckpointData::InMemory(std::collections::HashMap::new());
        };
        let read_txn = match db.begin_read() {
            Ok(txn) => txn,
            Err(_) => return StorageCheckpointData::InMemory(std::collections::HashMap::new()),
        };
        let table = match read_txn.open_table(ROWS_TABLE) {
            Ok(t) => t,
            Err(_) => return StorageCheckpointData::InMemory(std::collections::HashMap::new()),
        };
        let meta = match read_txn.open_table(TABLE_META) {
            Ok(m) => m,
            Err(_) => return StorageCheckpointData::InMemory(std::collections::HashMap::new()),
        };
        let range = match table.range((0u32, 0u64)..) {
            Ok(r) => r,
            Err(_) => return StorageCheckpointData::InMemory(std::collections::HashMap::new()),
        };

        let mut rows_map: std::collections::HashMap<u32, Vec<StoredRow>> =
            std::collections::HashMap::new();
        for entry in range {
            let (key, value): (AccessGuard<'_, (u32, u64)>, AccessGuard<'_, &[u8]>) = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let (table_id, _row_idx) = key.value();
            let row = match Self::deserialize_row(value.value()) {
                Ok(r) => r,
                Err(_) => continue,
            };
            rows_map.entry(table_id).or_default().push(row);
        }

        let meta_range = match meta.range(0u32..) {
            Ok(r) => r,
            Err(_) => return StorageCheckpointData::InMemory(rows_map),
        };
        for entry in meta_range {
            let (table_id, _next_idx): (AccessGuard<'_, u32>, AccessGuard<'_, u64>) = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            rows_map.entry(table_id.value()).or_default();
        }

        StorageCheckpointData::InMemory(rows_map)
    }

    fn restore_from_checkpoint(&mut self, data: StorageCheckpointData) -> Result<(), DbError> {
        let rows_map = match data {
            StorageCheckpointData::InMemory(map) => map,
            StorageCheckpointData::Persistent => return Ok(()),
        };
        let db = self.db()?;
        Self::with_write_txn(db, |write_txn| {
            let mut table = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open rows table", e))?;
            let mut meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open meta table", e))?;

            let keys: Vec<(u32, u64)> = table
                .range((0u32, 0u64)..)
                .map_err(|e| Self::storage_err("failed to scan rows", e))?
                .flatten()
                .map(|(k, _)| k.value())
                .collect();
            for key in keys {
                table
                    .remove(key)
                    .map_err(|e| Self::storage_err("failed to remove row", e))?;
            }
            let meta_keys: Vec<u32> = meta
                .range(0u32..)
                .map_err(|e| Self::storage_err("failed to scan meta", e))?
                .flatten()
                .map(|(k, _)| k.value())
                .collect();
            for key in meta_keys {
                meta.remove(key)
                    .map_err(|e| Self::storage_err("failed to remove meta", e))?;
            }

            for (table_id, rows) in &rows_map {
                let next_idx = rows.len() as u64;
                meta.insert(*table_id, &next_idx)
                    .map_err(|e| Self::storage_err("failed to set next_idx", e))?;
                for (idx, row) in rows.iter().enumerate() {
                    let row_bytes = Self::serialize_row(row)?;
                    table
                        .insert((*table_id, idx as u64), row_bytes.as_slice())
                        .map_err(|e| Self::storage_err("failed to insert row", e))?;
                }
            }
            Ok(())
        })
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
