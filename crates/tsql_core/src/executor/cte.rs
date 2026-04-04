use crate::catalog::TableDef;
use crate::storage::StoredRow;
use std::collections::HashMap;

use super::string_norm::normalize_identifier;

#[derive(Debug, Clone)]
pub struct CteTable {
    pub table_def: TableDef,
    pub rows: Vec<StoredRow>,
}

#[derive(Debug, Clone, Default)]
pub struct CteStorage {
    pub tables: HashMap<String, CteTable>,
}

impl CteStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: &str, table_def: TableDef, rows: Vec<StoredRow>) {
        self.tables
            .insert(normalize_identifier(name), CteTable { table_def, rows });
    }

    pub fn get(&self, name: &str) -> Option<&CteTable> {
        self.tables.get(&normalize_identifier(name))
    }
}

pub fn resolve_cte_table<'a>(
    ctes: &'a CteStorage,
    schema: &str,
    name: &str,
) -> Option<&'a CteTable> {
    if schema != "dbo" {
        return None;
    }
    ctes.get(name)
}

pub fn cte_to_context_rows(cte: &CteTable, alias: &str) -> Vec<crate::executor::model::JoinedRow> {
    cte.rows
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.deleted)
        .map(|(i, row)| {
            vec![crate::executor::model::ContextTable {
                table: cte.table_def.clone(),
                alias: alias.to_string(),
                row: Some(row.clone()),
                storage_index: Some(i),
            }]
        })
        .collect()
}
