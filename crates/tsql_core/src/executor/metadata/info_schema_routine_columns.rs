use super::VirtualTable;
use super::{
    virtual_table_def,
};
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType};

pub(super) struct RoutineColumns;

impl VirtualTable for RoutineColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "ROUTINE_COLUMNS",
            vec![
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
                ("ORDINAL_POSITION", DataType::Int, false),
                ("COLUMN_DEFAULT", DataType::VarChar { max_len: 128 }, true),
                ("IS_NULLABLE", DataType::VarChar { max_len: 3 }, false),
                ("DATA_TYPE", DataType::VarChar { max_len: 128 }, false),
                ("CHARACTER_MAXIMUM_LENGTH", DataType::Int, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        // Currently we don't store the result schema of TVFs in the catalog in a way that's easy to retrieve here
        // as SelectStmt doesn't have an easily accessible output schema without analysis.
        // Returning empty for now but satisfying the view existence.
        vec![]
    }
}
