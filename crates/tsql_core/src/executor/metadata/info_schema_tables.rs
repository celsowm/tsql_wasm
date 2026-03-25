use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};
use super::VirtualTable;
use super::{schema_name_by_id, virtual_table_def, DB_CATALOG};

pub(super) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    match name {
        n if n.eq_ignore_ascii_case("SCHEMATA") => Some(Box::new(Schemata)),
        n if n.eq_ignore_ascii_case("TABLES") => Some(Box::new(Tables)),
        n if n.eq_ignore_ascii_case("VIEWS") => Some(Box::new(Views)),
        _ => None,
    }
}

struct Schemata;
struct Tables;
struct Views;

impl VirtualTable for Schemata {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def("SCHEMATA", vec![
            ("CATALOG_NAME", DataType::VarChar { max_len: 128 }, false),
            ("SCHEMA_NAME", DataType::VarChar { max_len: 128 }, false),
            ("SCHEMA_OWNER", DataType::VarChar { max_len: 128 }, true),
            ("DEFAULT_CHARACTER_SET_CATALOG", DataType::VarChar { max_len: 6 }, true),
            ("DEFAULT_CHARACTER_SET_SCHEMA", DataType::VarChar { max_len: 3 }, true),
            ("DEFAULT_CHARACTER_SET_NAME", DataType::VarChar { max_len: 128 }, true),
        ])
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog.get_schemas().iter().map(|s| StoredRow {
            values: vec![
                Value::VarChar(DB_CATALOG.to_string()),
                Value::VarChar(s.name.clone()),
                Value::VarChar("dbo".to_string()),
                Value::Null,
                Value::Null,
                Value::VarChar("iso_1".to_string()),
            ],
            deleted: false,
        }).collect()
    }
}

impl VirtualTable for Tables {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def("TABLES", vec![
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_TYPE", DataType::VarChar { max_len: 10 }, false),
        ])
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows: Vec<StoredRow> = catalog.get_tables().iter().map(|t| StoredRow {
            values: vec![
                Value::VarChar(DB_CATALOG.to_string()),
                Value::VarChar(schema_name_by_id(catalog, t.schema_id)),
                Value::VarChar(t.name.clone()),
                Value::VarChar("BASE TABLE".to_string()),
            ],
            deleted: false,
        }).collect();
        for v in catalog.get_views() {
            rows.push(StoredRow {
                values: vec![
                    Value::VarChar(DB_CATALOG.to_string()),
                    Value::VarChar(v.schema.clone()),
                    Value::VarChar(v.name.clone()),
                    Value::VarChar("VIEW".to_string()),
                ],
                deleted: false,
            });
        }
        rows
    }
}

impl VirtualTable for Views {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def("VIEWS", vec![
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("VIEW_DEFINITION", DataType::VarChar { max_len: 128 }, true),
            ("CHECK_OPTION", DataType::VarChar { max_len: 7 }, false),
            ("IS_UPDATABLE", DataType::VarChar { max_len: 2 }, false),
        ])
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog.get_views().iter().map(|v| StoredRow {
            values: vec![
                Value::VarChar(DB_CATALOG.to_string()),
                Value::VarChar(v.schema.clone()),
                Value::VarChar(v.name.clone()),
                Value::Null,
                Value::VarChar("NONE".to_string()),
                Value::VarChar("NO".to_string()),
            ],
            deleted: false,
        }).collect()
    }
}
