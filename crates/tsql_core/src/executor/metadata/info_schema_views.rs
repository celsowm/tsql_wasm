use super::VirtualTable;
use super::{
    virtual_table_def,
};
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType};

pub(super) struct ViewTableUsage;
pub(super) struct ViewColumnUsage;

impl VirtualTable for ViewTableUsage {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "VIEW_TABLE_USAGE",
            vec![
                ("VIEW_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("VIEW_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("VIEW_NAME", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}

impl VirtualTable for ViewColumnUsage {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "VIEW_COLUMN_USAGE",
            vec![
                ("VIEW_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("VIEW_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("VIEW_NAME", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}
