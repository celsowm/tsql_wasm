use std::collections::HashMap;

use crate::types::Value;
use crate::error::DbError;

#[derive(Debug, Clone)]
pub struct StoredRow {
    pub values: Vec<Value>,
    pub deleted: bool,
}

pub trait Storage: std::fmt::Debug + Send + Sync {
    fn get_rows(&self, table_id: u32) -> Result<Vec<StoredRow>, DbError>;
    fn insert_row(&mut self, table_id: u32, row: StoredRow) -> Result<(), DbError>;
    fn update_rows(&mut self, table_id: u32, rows: Vec<StoredRow>) -> Result<(), DbError>;
    fn clear_table(&mut self, table_id: u32) -> Result<(), DbError>;
    fn remove_table(&mut self, table_id: u32);
    fn ensure_table(&mut self, table_id: u32);
}

#[derive(Debug, Default)]
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
}
