pub mod redb_storage;
pub use redb_storage::RedbStorage;

use std::collections::HashMap;

use crate::error::DbError;
use crate::types::Value;
use serde::{Deserialize, Serialize};

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
    fn get_rows(&self, table_id: u32) -> Result<Vec<StoredRow>, DbError>;
    fn insert_row(&mut self, table_id: u32, row: StoredRow) -> Result<(), DbError>;
    fn update_row(&mut self, table_id: u32, index: usize, row: StoredRow) -> Result<(), DbError>;
    fn delete_row(&mut self, table_id: u32, index: usize) -> Result<(), DbError>;
    fn update_rows(&mut self, table_id: u32, rows: Vec<StoredRow>) -> Result<(), DbError>;
    fn clear_table(&mut self, table_id: u32) -> Result<(), DbError>;
    fn remove_table(&mut self, table_id: u32);
    fn ensure_table(&mut self, table_id: u32);

    fn get_checkpoint_data(&self) -> StorageCheckpointData;
    fn restore_from_checkpoint(&mut self, data: StorageCheckpointData) -> Result<(), DbError>;
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InMemoryStorage {
    pub tables: HashMap<u32, Vec<StoredRow>>,
}

impl Storage for InMemoryStorage {
    fn get_rows(&self, table_id: u32) -> Result<Vec<StoredRow>, DbError> {
        self.tables
            .get(&table_id)
            .cloned()
            .ok_or_else(|| DbError::Storage(format!("table {} not found in storage", table_id)))
    }

    fn insert_row(&mut self, table_id: u32, row: StoredRow) -> Result<(), DbError> {
        self.tables
            .get_mut(&table_id)
            .ok_or_else(|| DbError::Storage(format!("table {} not found in storage", table_id)))?
            .push(row);
        Ok(())
    }

    fn update_row(&mut self, table_id: u32, index: usize, row: StoredRow) -> Result<(), DbError> {
        let table = self.tables.get_mut(&table_id)
            .ok_or_else(|| DbError::Storage(format!("table {} not found", table_id)))?;
        if index >= table.len() {
            return Err(DbError::Storage(format!("index {} out of bounds for table {}", index, table_id)));
        }
        table[index] = row;
        Ok(())
    }

    fn delete_row(&mut self, table_id: u32, index: usize) -> Result<(), DbError> {
        let table = self.tables.get_mut(&table_id)
            .ok_or_else(|| DbError::Storage(format!("table {} not found", table_id)))?;
        if index >= table.len() {
            return Err(DbError::Storage(format!("index {} out of bounds for table {}", index, table_id)));
        }
        table[index].deleted = true;
        Ok(())
    }

    fn update_rows(&mut self, table_id: u32, rows: Vec<StoredRow>) -> Result<(), DbError> {
        self.tables
            .insert(table_id, rows)
            .ok_or_else(|| DbError::Storage(format!("table {} not found in storage", table_id)))?;
        Ok(())
    }

    fn clear_table(&mut self, table_id: u32) -> Result<(), DbError> {
        if let Some(rows) = self.tables.get_mut(&table_id) {
            rows.clear();
            Ok(())
        } else {
            Err(DbError::Storage(format!("table {} not found", table_id)))
        }
    }

    fn remove_table(&mut self, table_id: u32) {
        self.tables.remove(&table_id);
    }

    fn ensure_table(&mut self, table_id: u32) {
        self.tables.entry(table_id).or_default();
    }

    fn get_checkpoint_data(&self) -> StorageCheckpointData {
        StorageCheckpointData::InMemory(self.tables.clone())
    }

    fn restore_from_checkpoint(&mut self, data: StorageCheckpointData) -> Result<(), DbError> {
        if let StorageCheckpointData::InMemory(tables) = data {
            self.tables = tables;
            Ok(())
        } else {
            Err(DbError::Storage("invalid checkpoint data for InMemoryStorage".into()))
        }
    }
}
