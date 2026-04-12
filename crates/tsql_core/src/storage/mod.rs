pub mod btree_index;
pub mod redb_storage;
pub use btree_index::{BTreeIndex, IndexStorage};
pub use redb_storage::RedbStorage;

use std::collections::HashMap;

use crate::error::DbError;
use crate::types::Value;
use serde::{Deserialize, Serialize};

pub type StorageRowStream<'a> = Box<dyn Iterator<Item = Result<StoredRow, DbError>> + 'a>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRow {
    pub values: Vec<Value>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageCheckpointData {
    InMemory(HashMap<u32, Vec<StoredRow>>),
    Persistent,
}

pub trait Storage: std::fmt::Debug + Send + Sync {
    fn scan_rows<'a>(&'a self, table_id: u32) -> Result<StorageRowStream<'a>, DbError>;

    fn get_row(&self, table_id: u32, index: usize) -> Result<Option<StoredRow>, DbError>;

    fn get_rows(&self, table_id: u32) -> Result<Vec<StoredRow>, DbError> {
        self.scan_rows(table_id)?.collect()
    }

    fn insert_row(&mut self, table_id: u32, row: StoredRow) -> Result<(), DbError>;
    fn update_row(&mut self, table_id: u32, index: usize, row: StoredRow) -> Result<(), DbError>;
    fn delete_row(&mut self, table_id: u32, index: usize) -> Result<(), DbError>;
    fn replace_table(&mut self, table_id: u32, rows: Vec<StoredRow>) -> Result<(), DbError>;
    fn clear_table(&mut self, table_id: u32) -> Result<(), DbError>;
    fn remove_table(&mut self, table_id: u32) -> Result<(), DbError>;
    fn ensure_table(&mut self, table_id: u32) -> Result<(), DbError>;

    fn clone_boxed(&self) -> Box<dyn Storage>;

    fn as_index_storage(&self) -> Option<&dyn IndexStorage> {
        None
    }
    fn as_index_storage_mut(&mut self) -> Option<&mut dyn IndexStorage> {
        None
    }
}

pub trait CheckpointableStorage: Storage {
    fn get_checkpoint_data(&self) -> StorageCheckpointData;
    fn restore_from_checkpoint(&mut self, data: StorageCheckpointData) -> Result<(), DbError>;
    fn clone_checkpointable(&self) -> Box<dyn CheckpointableStorage>;
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InMemoryStorage {
    pub tables: HashMap<u32, Vec<StoredRow>>,
}

impl Storage for InMemoryStorage {
    fn scan_rows<'a>(&'a self, table_id: u32) -> Result<StorageRowStream<'a>, DbError> {
        let rows = self
            .tables
            .get(&table_id)
            .ok_or_else(|| DbError::Storage(format!("table {} not found in storage", table_id)))?;

        Ok(Box::new(rows.iter().cloned().map(Ok)))
    }

    fn get_row(&self, table_id: u32, index: usize) -> Result<Option<StoredRow>, DbError> {
        let rows = self
            .tables
            .get(&table_id)
            .ok_or_else(|| DbError::Storage(format!("table {} not found", table_id)))?;
        Ok(rows.get(index).cloned())
    }

    fn insert_row(&mut self, table_id: u32, row: StoredRow) -> Result<(), DbError> {
        self.tables
            .get_mut(&table_id)
            .ok_or_else(|| DbError::Storage(format!("table {} not found in storage", table_id)))?
            .push(row);
        Ok(())
    }

    fn update_row(&mut self, table_id: u32, index: usize, row: StoredRow) -> Result<(), DbError> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| DbError::Storage(format!("table {} not found", table_id)))?;
        if index >= table.len() {
            return Err(DbError::Storage(format!(
                "index {} out of bounds for table {}",
                index, table_id
            )));
        }
        table[index] = row;
        Ok(())
    }

    fn delete_row(&mut self, table_id: u32, index: usize) -> Result<(), DbError> {
        let table = self
            .tables
            .get_mut(&table_id)
            .ok_or_else(|| DbError::Storage(format!("table {} not found", table_id)))?;
        if index >= table.len() {
            return Err(DbError::Storage(format!(
                "index {} out of bounds for table {}",
                index, table_id
            )));
        }
        table[index].deleted = true;
        Ok(())
    }

    fn replace_table(&mut self, table_id: u32, rows: Vec<StoredRow>) -> Result<(), DbError> {
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.tables.entry(table_id) {
            e.insert(rows);
            Ok(())
        } else {
            Err(DbError::Storage(format!(
                "table {} not found in storage",
                table_id
            )))
        }
    }

    fn clear_table(&mut self, table_id: u32) -> Result<(), DbError> {
        if let Some(rows) = self.tables.get_mut(&table_id) {
            rows.clear();
            Ok(())
        } else {
            Err(DbError::Storage(format!("table {} not found", table_id)))
        }
    }

    fn ensure_table(&mut self, table_id: u32) -> Result<(), DbError> {
        self.tables.entry(table_id).or_default();
        Ok(())
    }

    fn remove_table(&mut self, table_id: u32) -> Result<(), DbError> {
        self.tables.remove(&table_id);
        Ok(())
    }

    fn clone_boxed(&self) -> Box<dyn Storage> {
        Box::new(self.clone())
    }

    fn as_index_storage(&self) -> Option<&dyn IndexStorage> {
        None
    }
    fn as_index_storage_mut(&mut self) -> Option<&mut dyn IndexStorage> {
        None
    }
}

impl CheckpointableStorage for InMemoryStorage {
    fn get_checkpoint_data(&self) -> StorageCheckpointData {
        StorageCheckpointData::InMemory(self.tables.clone())
    }

    fn restore_from_checkpoint(&mut self, data: StorageCheckpointData) -> Result<(), DbError> {
        if let StorageCheckpointData::InMemory(tables) = data {
            self.tables = tables;
            Ok(())
        } else {
            Err(DbError::Storage(
                "invalid checkpoint data for InMemoryStorage".into(),
            ))
        }
    }

    fn clone_checkpointable(&self) -> Box<dyn CheckpointableStorage> {
        Box::new(self.clone())
    }
}
