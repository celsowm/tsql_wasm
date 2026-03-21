use std::collections::HashMap;

use crate::types::Value;

#[derive(Debug, Clone)]
pub struct StoredRow {
    pub values: Vec<Value>,
    pub deleted: bool,
}

#[derive(Debug, Default)]
pub struct InMemoryStorage {
    pub tables: HashMap<u32, Vec<StoredRow>>,
}
