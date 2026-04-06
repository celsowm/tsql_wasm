use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};
use super::super::VirtualTable;
use super::super::{builtin_types_rows, virtual_table_def, system_type_id, type_max_length};

pub(crate) struct SysSchemas;
pub(crate) struct SysDatabases;
pub(crate) struct SysSysDatabases;
pub(crate) struct SysConfigurations;
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

impl VirtualTable for SysDatabases {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "databases",
            vec![
                ("database_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("source_database_id", DataType::Int, true),
                ("owner_sid", DataType::VarBinary { max_len: 85 }, true),
                ("create_date", DataType::DateTime, false),
                ("compatibility_level", DataType::TinyInt, false),
                ("collation_name", DataType::VarChar { max_len: 128 }, false),
                ("state", DataType::TinyInt, false),
                ("state_desc", DataType::VarChar { max_len: 60 }, false),
                ("user_access", DataType::TinyInt, false),
                ("user_access_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_read_only", DataType::Bit, false),
                ("recovery_model", DataType::TinyInt, false),
                ("recovery_model_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_auto_close_on", DataType::Bit, false),
                ("is_auto_shrink_on", DataType::Bit, false),
                ("is_in_standby", DataType::Bit, false),
                ("is_cleanly_shutdown", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![StoredRow {
            values: vec![
                Value::Int(1),
                Value::VarChar("master".to_string()),
                Value::Null,
                Value::VarBinary(vec![0x01]),
                Value::DateTime(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                ),
                Value::TinyInt(160),
                Value::VarChar("SQL_Latin1_General_CP1_CI_AS".to_string()),
                Value::TinyInt(0),
                Value::VarChar("ONLINE".to_string()),
                Value::TinyInt(0),
                Value::VarChar("MULTI_USER".to_string()),
                Value::Bit(false),
                Value::TinyInt(1),
                Value::VarChar("FULL".to_string()),
                Value::Bit(false),
                Value::Bit(false),
                Value::Bit(false),
                Value::Bit(true),
            ],
            deleted: false,
        }]
    }
}

impl VirtualTable for SysConfigurations {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "configurations",
            vec![
                ("configuration_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("value", DataType::Int, false),
                ("value_in_use", DataType::Int, false),
                ("minimum", DataType::Int, false),
                ("maximum", DataType::Int, false),
                ("is_dynamic", DataType::Bit, false),
                ("is_advanced", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![StoredRow {
            // configuration_id=16384 used by SSMS probe (contained db auth).
            values: vec![
                Value::Int(16384),
                Value::VarChar("contained database authentication".to_string()),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(1),
                Value::Bit(true),
                Value::Bit(true),
            ],
            deleted: false,
        }]
    }
}

impl VirtualTable for SysSysDatabases {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "sysdatabases",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("dbid", DataType::SmallInt, false),
                ("sid", DataType::VarBinary { max_len: 85 }, true),
                ("mode", DataType::SmallInt, false),
                ("status", DataType::Int, false),
                ("status2", DataType::Int, false),
                ("crdate", DataType::DateTime, false),
                ("cmptlevel", DataType::TinyInt, false),
                ("filename", DataType::VarChar { max_len: 260 }, true),
                ("version", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![StoredRow {
            values: vec![
                Value::VarChar("master".to_string()),
                Value::SmallInt(1),
                Value::VarBinary(vec![0x01]),
                Value::SmallInt(0),
                Value::Int(0),
                Value::Int(0),
                Value::DateTime(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                ),
                Value::TinyInt(160),
                Value::Null,
                Value::Int(0),
            ],
            deleted: false,
        }]
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
