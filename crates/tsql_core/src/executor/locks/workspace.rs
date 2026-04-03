use std::collections::{HashMap, HashSet};

use super::types::AcquiredLock;

pub struct TxWorkspace<C, S> {
    pub catalog: C,
    pub storage: S,
    pub base_table_versions: HashMap<String, u64>,
    pub read_tables: HashSet<String>,
    pub write_tables: HashSet<String>,
    pub acquired_locks: Vec<AcquiredLock>,
}

impl<C: std::fmt::Debug, S: std::fmt::Debug> std::fmt::Debug for TxWorkspace<C, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxWorkspace")
            .field("base_table_versions", &self.base_table_versions)
            .field("read_tables", &self.read_tables)
            .field("write_tables", &self.write_tables)
            .field("acquired_locks", &self.acquired_locks)
            .finish()
    }
}

impl<C: Clone, S: Clone> Clone for TxWorkspace<C, S> {
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            storage: self.storage.clone(),
            base_table_versions: self.base_table_versions.clone(),
            read_tables: self.read_tables.clone(),
            write_tables: self.write_tables.clone(),
            acquired_locks: self.acquired_locks.clone(),
        }
    }
}
