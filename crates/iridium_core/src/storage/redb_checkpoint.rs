use std::collections::HashMap;

use redb::{AccessGuard, ReadableTable, TableDefinition};

use crate::error::DbError;
use crate::storage::{CheckpointableStorage, StorageCheckpointData, StoredRow};

use super::redb_storage::RedbStorage;

const ROWS_TABLE: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("rows");
const TABLE_META: TableDefinition<u32, u64> = TableDefinition::new("rows_meta");

impl CheckpointableStorage for RedbStorage {
    fn get_checkpoint_data(&self) -> StorageCheckpointData {
        let Ok(db) = self.db() else {
            return StorageCheckpointData::InMemory(HashMap::new());
        };
        let read_txn = match db.begin_read() {
            Ok(txn) => txn,
            Err(_) => return StorageCheckpointData::InMemory(HashMap::new()),
        };
        let table = match read_txn.open_table(ROWS_TABLE) {
            Ok(t) => t,
            Err(_) => return StorageCheckpointData::InMemory(HashMap::new()),
        };
        let meta = match read_txn.open_table(TABLE_META) {
            Ok(m) => m,
            Err(_) => return StorageCheckpointData::InMemory(HashMap::new()),
        };
        let range = match table.range((0u32, 0u64)..) {
            Ok(r) => r,
            Err(_) => return StorageCheckpointData::InMemory(HashMap::new()),
        };

        let mut rows_map: HashMap<u32, Vec<StoredRow>> = HashMap::new();
        for entry in range {
            let (key, value): (AccessGuard<'_, (u32, u64)>, AccessGuard<'_, &[u8]>) = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let (table_id, _row_idx) = key.value();
            let row = match RedbStorage::deserialize_row(value.value()) {
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
        RedbStorage::with_write_txn(db, |write_txn| {
            let mut table = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| RedbStorage::storage_err("failed to open rows table", e))?;
            let mut meta = write_txn
                .open_table(TABLE_META)
                .map_err(|e| RedbStorage::storage_err("failed to open meta table", e))?;

            let keys: Vec<(u32, u64)> = table
                .range((0u32, 0u64)..)
                .map_err(|e| RedbStorage::storage_err("failed to scan rows", e))?
                .flatten()
                .map(|(k, _)| k.value())
                .collect();
            for key in keys {
                table
                    .remove(key)
                    .map_err(|e| RedbStorage::storage_err("failed to remove row", e))?;
            }
            let meta_keys: Vec<u32> = meta
                .range(0u32..)
                .map_err(|e| RedbStorage::storage_err("failed to scan meta", e))?
                .flatten()
                .map(|(k, _)| k.value())
                .collect();
            for key in meta_keys {
                meta.remove(key)
                    .map_err(|e| RedbStorage::storage_err("failed to remove meta", e))?;
            }

            for (table_id, rows) in &rows_map {
                let next_idx = rows.len() as u64;
                meta.insert(*table_id, &next_idx)
                    .map_err(|e| RedbStorage::storage_err("failed to set next_idx", e))?;
                for (idx, row) in rows.iter().enumerate() {
                    let row_bytes = RedbStorage::serialize_row(row)?;
                    table
                        .insert((*table_id, idx as u64), row_bytes.as_slice())
                        .map_err(|e| RedbStorage::storage_err("failed to insert row", e))?;
                }
            }
            Ok(())
        })
    }

    fn clone_checkpointable(&self) -> Box<dyn crate::storage::CheckpointableStorage> {
        Box::new(self.clone())
    }
}
