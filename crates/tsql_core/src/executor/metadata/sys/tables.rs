use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};
use super::super::VirtualTable;
use super::super::{builtin_types_rows, virtual_table_def, system_type_id, type_max_length};

pub(crate) struct SysSchemas;
pub(crate) struct SysTables;
pub(crate) struct SysColumns;
pub(crate) struct SysTypes;

impl VirtualTable for SysSchemas {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "schemas",
            vec![
                ("schema_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog
            .get_schemas()
            .iter()
            .map(|s| StoredRow {
                values: vec![Value::Int(s.id as i32), Value::VarChar(s.name.clone())],
                deleted: false,
            })
            .collect()
    }
}

impl VirtualTable for SysTables {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "tables",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("schema_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog
            .get_tables()
            .iter()
            .map(|t| StoredRow {
                values: vec![
                    Value::Int(t.id as i32),
                    Value::VarChar(t.name.clone()),
                    Value::Int(t.schema_id as i32),
                ],
                deleted: false,
            })
            .collect()
    }
}

impl VirtualTable for SysColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "columns",
            vec![
                ("object_id", DataType::Int, false),
                ("column_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("user_type_id", DataType::Int, false),
                ("max_length", DataType::SmallInt, false),
                ("is_nullable", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            for c in &t.columns {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(t.id as i32),
                        Value::Int(c.id as i32),
                        Value::VarChar(c.name.clone()),
                        Value::Int(system_type_id(&c.data_type)),
                        Value::SmallInt(type_max_length(&c.data_type)),
                        Value::Bit(c.nullable),
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}

impl VirtualTable for SysTypes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "types",
            vec![
                ("user_type_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("max_length", DataType::SmallInt, false),
                ("precision", DataType::TinyInt, false),
                ("scale", DataType::TinyInt, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        builtin_types_rows()
    }
}
