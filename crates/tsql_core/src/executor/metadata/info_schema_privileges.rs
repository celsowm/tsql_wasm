use super::VirtualTable;
use super::{
    virtual_table_def,
    DB_CATALOG, schema_name_by_id
};
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(super) struct TablePrivileges;
pub(super) struct ColumnPrivileges;

impl VirtualTable for TablePrivileges {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "TABLE_PRIVILEGES",
            vec![
                ("GRANTOR", DataType::VarChar { max_len: 128 }, false),
                ("GRANTEE", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("PRIVILEGE_TYPE", DataType::VarChar { max_len: 10 }, false),
                ("IS_GRANTABLE", DataType::VarChar { max_len: 3 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            rows.push(StoredRow {
                values: vec![
                    Value::VarChar("sa".to_string()),
                    Value::VarChar("public".to_string()),
                    Value::VarChar(DB_CATALOG.to_string()),
                    Value::VarChar(schema_name_by_id(catalog, t.schema_id)),
                    Value::VarChar(t.name.clone()),
                    Value::VarChar("SELECT".to_string()),
                    Value::VarChar("YES".to_string()),
                ],
                deleted: false,
            });
        }
        rows
    }
}

impl VirtualTable for ColumnPrivileges {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "COLUMN_PRIVILEGES",
            vec![
                ("GRANTOR", DataType::VarChar { max_len: 128 }, false),
                ("GRANTEE", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
                ("PRIVILEGE_TYPE", DataType::VarChar { max_len: 10 }, false),
                ("IS_GRANTABLE", DataType::VarChar { max_len: 3 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            for c in &t.columns {
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar("sa".to_string()),
                        Value::VarChar("public".to_string()),
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema_name_by_id(catalog, t.schema_id)),
                        Value::VarChar(t.name.clone()),
                        Value::VarChar(c.name.clone()),
                        Value::VarChar("SELECT".to_string()),
                        Value::VarChar("YES".to_string()),
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}
