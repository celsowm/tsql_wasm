use crate::catalog::TableDef;
use crate::storage::StoredRow;

#[derive(Debug, Clone)]
pub(crate) struct BoundTable {
    pub(crate) table: TableDef,
    pub(crate) alias: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ContextTable {
    pub(crate) table: TableDef,
    pub(crate) alias: String,
    pub(crate) row: Option<StoredRow>,
}

pub(crate) type JoinedRow = Vec<ContextTable>;

#[derive(Debug, Clone, Default)]
pub(crate) struct Group {
    pub(crate) rows: Vec<JoinedRow>,
}

pub(crate) fn single_row_context(table: &TableDef, row: StoredRow) -> JoinedRow {
    vec![ContextTable {
        table: table.clone(),
        alias: table.name.clone(),
        row: Some(row),
    }]
}
