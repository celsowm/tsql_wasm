use std::collections::BTreeMap;

use crate::error::DbError;
use crate::types::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexKey {
    values: Vec<Value>,
}

impl IndexKey {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn from_row(values: &[Value], column_ids: &[u32]) -> Option<Self> {
        let key_values: Vec<Value> = column_ids
            .iter()
            .filter_map(|&cid| values.get(cid as usize).cloned())
            .collect();
        if key_values.len() != column_ids.len() {
            return None;
        }
        Some(Self::new(key_values))
    }

    pub fn as_values(&self) -> &[Value] {
        &self.values
    }
}

#[derive(Debug, Clone)]
pub struct BTreeIndex {
    pub column_ids: Vec<u32>,
    pub is_unique: bool,
    pub is_clustered: bool,
    tree: BTreeMap<IndexKey, Vec<usize>>,
}

impl BTreeIndex {
    pub fn new(column_ids: Vec<u32>, is_unique: bool, is_clustered: bool) -> Self {
        Self {
            column_ids,
            is_unique,
            is_clustered,
            tree: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, row_index: usize, row_values: &[Value]) -> Result<(), DbError> {
        let key = IndexKey::from_row(row_values, &self.column_ids)
            .ok_or_else(|| DbError::Storage("failed to extract index key from row".into()))?;

        if self.is_unique {
            if self.tree.contains_key(&key) {
                return Err(DbError::Execution(
                    "Cannot insert duplicate key in unique index".into(),
                ));
            }
        }

        self.tree.entry(key).or_default().push(row_index);
        Ok(())
    }

    pub fn delete(&mut self, row_index: usize, row_values: &[Value]) -> Result<(), DbError> {
        let key = IndexKey::from_row(row_values, &self.column_ids)
            .ok_or_else(|| DbError::Storage("failed to extract index key from row".into()))?;

        if let Some(indices) = self.tree.get_mut(&key) {
            if let Some(pos) = indices.iter().position(|&i| i == row_index) {
                indices.remove(pos);
                if indices.is_empty() {
                    self.tree.remove(&key);
                }
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn update(
        &mut self,
        old_row_index: usize,
        old_values: &[Value],
        new_row_index: usize,
        new_values: &[Value],
    ) -> Result<(), DbError> {
        self.delete(old_row_index, old_values)?;
        self.insert(new_row_index, new_values)
    }

    pub fn seek(&self, key: &Value) -> Option<&Vec<usize>> {
        let search_key = IndexKey::new(vec![key.clone()]);
        self.tree.get(&search_key)
    }

    pub fn seek_range(
        &self,
        lower: Option<&Value>,
        upper: Option<&Value>,
    ) -> Vec<(IndexKey, Vec<usize>)> {
        let mut result = Vec::new();

        if let Some(lower) = lower {
            if let Some(upper) = upper {
                for (k, v) in self.tree.iter() {
                    if let Some(first) = k.as_values().first() {
                        if first >= lower && first < upper {
                            result.push((k.clone(), v.clone()));
                        }
                    }
                }
            } else {
                for (k, v) in self.tree.iter() {
                    if let Some(first) = k.as_values().first() {
                        if first >= lower {
                            result.push((k.clone(), v.clone()));
                        }
                    }
                }
            }
        } else if let Some(upper) = upper {
            for (k, v) in self.tree.iter() {
                if let Some(first) = k.as_values().first() {
                    if first < upper {
                        result.push((k.clone(), v.clone()));
                    }
                }
            }
        } else {
            for (k, v) in self.tree.iter() {
                result.push((k.clone(), v.clone()));
            }
        }

        result
    }

    pub fn all_entries(&self) -> Vec<(IndexKey, Vec<usize>)> {
        self.tree
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    pub fn clear(&mut self) {
        self.tree.clear();
    }

    pub fn rebuild_from_rows(&mut self, rows: &[(usize, &[Value])]) -> Result<(), DbError> {
        self.clear();
        for (idx, values) in rows {
            self.insert(*idx, values)?;
        }
        Ok(())
    }
}

pub trait IndexStorage: Send + Sync {
    fn register_index(
        &mut self,
        index_id: u32,
        column_ids: Vec<u32>,
        is_unique: bool,
        is_clustered: bool,
    );
    fn get_index(&self, index_id: u32) -> Option<&BTreeIndex>;
    fn get_index_mut(&mut self, index_id: u32) -> Option<&mut BTreeIndex>;
    fn seek_index(&self, index_id: u32, key: &Value) -> Result<Vec<usize>, DbError>;
    fn seek_index_range(
        &self,
        index_id: u32,
        lower: Option<&Value>,
        upper: Option<&Value>,
    ) -> Result<Vec<(Value, Vec<usize>)>, DbError>;
    fn insert_index_entry(
        &mut self,
        index_id: u32,
        key: Value,
        row_index: usize,
    ) -> Result<(), DbError>;
    fn delete_index_entry(
        &mut self,
        index_id: u32,
        key: &Value,
        row_index: usize,
    ) -> Result<(), DbError>;
    fn rebuild_index(&mut self, index_id: u32, entries: Vec<(Value, usize)>)
        -> Result<(), DbError>;
}
