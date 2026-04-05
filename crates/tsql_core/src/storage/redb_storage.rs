use std::path::Path;
use std::sync::Arc;

use crate::error::DbError;
use crate::storage::{Storage, StoredRow, StorageCheckpointData};
use redb::{Database, TableDefinition, ReadableTable, AccessGuard};
use serde::{Serialize, Deserialize};

const ROWS_TABLE: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("rows");

#[derive(Debug, Clone)]
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

impl Default for RedbStorage {
    fn default() -> Self {
        Self { db: None }
    }
}

impl RedbStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DbError> {
        let db = Database::create(path)
            .map_err(|e| DbError::Storage(format!("failed to create redb database: {}", e)))?;

        // Ensure table exists
        let write_txn = db.begin_write()
            .map_err(|e| DbError::Storage(format!("failed to begin write txn: {}", e)))?;
        {
            let _ = write_txn.open_table(ROWS_TABLE)
                .map_err(|e| DbError::Storage(format!("failed to open rows table: {}", e)))?;
        }
        write_txn.commit()
            .map_err(|e| DbError::Storage(format!("failed to commit txn: {}", e)))?;

        Ok(Self { db: Some(Arc::new(db)) })
    }

    fn db(&self) -> Result<&Database, DbError> {
        self.db.as_deref().ok_or_else(|| DbError::Storage("RedbStorage not initialized (handle is missing)".into()))
    }
}

impl Storage for RedbStorage {
    fn scan_rows<'a>(&'a self, table_id: u32) -> Result<crate::storage::StorageRowStream<'a>, DbError> {
        let db = self.db()?;
        let read_txn = db.begin_read()
            .map_err(|e| DbError::Storage(format!("failed to begin read txn: {}", e)))?;
        let table = read_txn.open_table(ROWS_TABLE)
            .map_err(|e| DbError::Storage(format!("failed to open table: {}", e)))?;

        let range = if table_id == u32::MAX {
            table.range((table_id, 0)..)
        } else {
            table.range((table_id, 0)..(table_id + 1, 0))
        }
            .map_err(|e| DbError::Storage(format!("failed to scan range: {}", e)))?;

        Ok(Box::new(range.map(|result| {
            let (_key, value): (AccessGuard<'_, (u32, u64)>, AccessGuard<'_, &[u8]>) = result
                .map_err(|e| DbError::Storage(format!("error reading row: {}", e)))?;
            serde_json::from_slice(value.value())
                .map_err(|e| DbError::Storage(format!("failed to deserialize row: {}", e)))
        })))
    }

    fn insert_row(&mut self, table_id: u32, row: StoredRow) -> Result<(), DbError> {
        let db = self.db()?;
        let write_txn = db.begin_write()
            .map_err(|e| DbError::Storage(format!("failed to begin write txn: {}", e)))?;
        {
            let mut table = write_txn.open_table(ROWS_TABLE)
                .map_err(|e| DbError::Storage(format!("failed to open table: {}", e)))?;

            let next_idx = {
                let range = if table_id == u32::MAX {
                    table.range((table_id, 0)..)
                } else {
                    table.range((table_id, 0)..(table_id + 1, 0))
                }
                    .map_err(|e| DbError::Storage(format!("failed to scan range: {}", e)))?
                    .rev()
                    .next();

                match range {
                    Some(Ok((key, _val))) => key.value().1 + 1,
                    _ => 0,
                }
            };

            let row_bytes = serde_json::to_vec(&row)
                .map_err(|e| DbError::Storage(format!("failed to serialize row: {}", e)))?;

            table.insert((table_id, next_idx), row_bytes.as_slice())
                .map_err(|e| DbError::Storage(format!("failed to insert row: {}", e)))?;
        }
        write_txn.commit()
            .map_err(|e| DbError::Storage(format!("failed to commit txn: {}", e)))?;
        Ok(())
    }

    fn update_row(&mut self, table_id: u32, index: usize, row: StoredRow) -> Result<(), DbError> {
        let db = self.db()?;
        let write_txn = db.begin_write()
            .map_err(|e| DbError::Storage(format!("failed to begin write txn: {}", e)))?;
        {
            let mut table = write_txn.open_table(ROWS_TABLE)
                .map_err(|e| DbError::Storage(format!("failed to open table: {}", e)))?;

            let row_bytes = serde_json::to_vec(&row)
                .map_err(|e| DbError::Storage(format!("failed to serialize row: {}", e)))?;

            table.insert((table_id, index as u64), row_bytes.as_slice())
                .map_err(|e| DbError::Storage(format!("failed to update row: {}", e)))?;
        }
        write_txn.commit()
            .map_err(|e| DbError::Storage(format!("failed to commit txn: {}", e)))?;
        Ok(())
    }

    fn delete_row(&mut self, table_id: u32, index: usize) -> Result<(), DbError> {
        let db = self.db()?;
        let write_txn = db.begin_write()
            .map_err(|e| DbError::Storage(format!("failed to begin write txn: {}", e)))?;
        {
            let mut table = write_txn.open_table(ROWS_TABLE)
                .map_err(|e| DbError::Storage(format!("failed to open table: {}", e)))?;

            let row_opt = {
                let current_val = table.get((table_id, index as u64))
                    .map_err(|e| DbError::Storage(format!("failed to get row: {}", e)))?;

                if let Some(val) = current_val {
                    let mut row: StoredRow = serde_json::from_slice(val.value())
                        .map_err(|e| DbError::Storage(format!("failed to deserialize row: {}", e)))?;
                    row.deleted = true;
                    Some(row)
                } else {
                    None
                }
            };

            if let Some(row) = row_opt {
                let row_bytes = serde_json::to_vec(&row)
                    .map_err(|e| DbError::Storage(format!("failed to serialize row: {}", e)))?;
                table.insert((table_id, index as u64), row_bytes.as_slice())
                    .map_err(|e| DbError::Storage(format!("failed to delete row: {}", e)))?;
            }
        }
        write_txn.commit()
            .map_err(|e| DbError::Storage(format!("failed to commit txn: {}", e)))?;
        Ok(())
    }

    fn replace_table(&mut self, table_id: u32, rows: Vec<StoredRow>) -> Result<(), DbError> {
        let db = self.db()?;
        let write_txn = db.begin_write()
            .map_err(|e| DbError::Storage(format!("failed to begin write txn: {}", e)))?;
        {
            let mut table = write_txn.open_table(ROWS_TABLE)
                .map_err(|e| DbError::Storage(format!("failed to open table: {}", e)))?;

            let keys_to_delete: Vec<(u32, u64)> = {
                let range = if table_id == u32::MAX {
                    table.range((table_id, 0)..)
                } else {
                    table.range((table_id, 0)..(table_id + 1, 0))
                }
                    .map_err(|e| DbError::Storage(format!("failed to scan range: {}", e)))?;
                range.flatten().map(|(k, _v): (AccessGuard<'_, (u32, u64)>, AccessGuard<'_, &[u8]>)| k.value()).collect()
            };

            for key in keys_to_delete {
                table.remove(key)
                    .map_err(|e| DbError::Storage(format!("failed to remove row: {}", e)))?;
            }

            for (idx, row) in rows.into_iter().enumerate() {
                let row_bytes = serde_json::to_vec(&row)
                    .map_err(|e| DbError::Storage(format!("failed to serialize row: {}", e)))?;
                table.insert((table_id, idx as u64), row_bytes.as_slice())
                    .map_err(|e| DbError::Storage(format!("failed to insert row: {}", e)))?;
            }
        }
        write_txn.commit()
            .map_err(|e| DbError::Storage(format!("failed to commit txn: {}", e)))?;
        Ok(())
    }

    fn clear_table(&mut self, table_id: u32) -> Result<(), DbError> {
        let db = self.db()?;
        let write_txn = db.begin_write()
            .map_err(|e| DbError::Storage(format!("failed to begin write txn: {}", e)))?;
        {
            let mut table = write_txn.open_table(ROWS_TABLE)
                .map_err(|e| DbError::Storage(format!("failed to open table: {}", e)))?;

            let keys_to_delete: Vec<(u32, u64)> = {
                let range = if table_id == u32::MAX {
                    table.range((table_id, 0)..)
                } else {
                    table.range((table_id, 0)..(table_id + 1, 0))
                }
                    .map_err(|e| DbError::Storage(format!("failed to scan range: {}", e)))?;
                range.flatten().map(|(k, _v): (AccessGuard<'_, (u32, u64)>, AccessGuard<'_, &[u8]>)| k.value()).collect()
            };

            for key in keys_to_delete {
                table.remove(key)
                    .map_err(|e| DbError::Storage(format!("failed to remove row: {}", e)))?;
            }
        }
        write_txn.commit()
            .map_err(|e| DbError::Storage(format!("failed to commit txn: {}", e)))?;
        Ok(())
    }

    fn remove_table(&mut self, table_id: u32) {
        let _ = self.clear_table(table_id);
    }

    fn ensure_table(&mut self, _table_id: u32) {
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
