use crate::error::DbError;
use crate::storage::{BTreeIndex, IndexStorage};
use crate::types::Value;

use super::redb_storage::RedbStorage;

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

        let result = index.seek(key).cloned().unwrap_or_default();
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
