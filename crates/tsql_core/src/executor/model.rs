use crate::catalog::TableDef;
use crate::storage::StoredRow;
use crate::types::Value;

#[derive(Debug, Clone)]
pub struct Group {
    pub key: Vec<Value>,
    pub rows: Vec<JoinedRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct BoundTable {
    pub(crate) table: TableDef,
    pub(crate) alias: String,
    pub(crate) virtual_rows: Option<Vec<StoredRow>>,
}

#[derive(Debug, Clone)]
pub struct ContextTable {
    pub table: TableDef,
    pub alias: String,
    pub row: Option<StoredRow>,
    pub storage_index: Option<usize>,
    pub source_aliases: Vec<String>,
}

impl ContextTable {
    pub fn null_row(&self) -> Self {
        Self {
            table: self.table.clone(),
            alias: self.alias.clone(),
            row: None,
            storage_index: None,
            source_aliases: self.source_aliases.clone(),
        }
    }
}

pub type JoinedRow = Vec<ContextTable>;

pub(crate) fn single_row_context(table: &TableDef, row: StoredRow) -> JoinedRow {
    vec![ContextTable {
        table: table.clone(),
        alias: table.name.clone(),
        row: Some(row),
        storage_index: None,
        source_aliases: Vec::new(),
    }]
}

#[derive(Debug, Clone)]
pub struct Cursor {
    pub query: Option<crate::ast::SelectStmt>,
    pub query_result: super::result::QueryResult,
    pub current_row: i64,
}
