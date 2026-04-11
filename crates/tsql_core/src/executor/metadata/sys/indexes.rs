use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysIndexes;

impl VirtualTable for SysIndexes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "indexes",
            vec![
                ("object_id", DataType::Int, false),
                ("index_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::TinyInt, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_unique", DataType::Bit, false),
                ("data_space_id", DataType::Int, false),
                ("is_primary_key", DataType::Bit, false),
                ("is_unique_constraint", DataType::Bit, false),
                ("is_hypothetical", DataType::Bit, false),
                ("is_disabled", DataType::Bit, false),
                ("has_filter", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog
            .get_indexes()
            .iter()
            .map(|idx| StoredRow {
                values: vec![
                    Value::Int(idx.table_id as i32),
                    Value::Int(idx.id as i32),
                    Value::VarChar(idx.name.clone()),
                    Value::TinyInt(if idx.is_clustered { 1 } else { 2 }),
                    Value::VarChar(if idx.is_clustered {
                        "CLUSTERED".to_string()
                    } else {
                        "NONCLUSTERED".to_string()
                    }),
                    Value::Bit(idx.is_unique),
                    Value::Int(1),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                ],
                deleted: false,
            })
            .collect()
    }
}
