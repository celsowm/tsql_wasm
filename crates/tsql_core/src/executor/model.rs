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
}

pub type JoinedRow = Vec<ContextTable>;

pub(crate) fn single_row_context(table: &TableDef, row: StoredRow) -> JoinedRow {
    vec![ContextTable {
        table: table.clone(),
        alias: table.name.clone(),
        row: Some(row),
    }]
}
