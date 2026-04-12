use super::super::super::virtual_table_def;
use super::super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysDatabases;
pub(crate) struct SysSysDatabases;
pub(crate) struct SysConfigurations;

#[derive(Clone, Copy)]
struct DatabaseRow {
    id: i32,
    name: &'static str,
    compatibility_level: u8,
    recovery_model: &'static str,
}

const CATALOG_DATABASES: &[DatabaseRow] = &[
    DatabaseRow {
        id: 1,
        name: "master",
        compatibility_level: 160,
        recovery_model: "FULL",
    },
    DatabaseRow {
        id: 2,
        name: "tempdb",
        compatibility_level: 160,
        recovery_model: "SIMPLE",
    },
    DatabaseRow {
        id: 3,
        name: "model",
        compatibility_level: 160,
        recovery_model: "FULL",
    },
    DatabaseRow {
        id: 4,
        name: "msdb",
        compatibility_level: 160,
        recovery_model: "FULL",
    },
];

const USER_DATABASES: &[DatabaseRow] = &[
    // The playground database is a user database, not a system database.
    DatabaseRow {
        id: 5,
        name: "tsql_wasm",
        compatibility_level: 160,
        recovery_model: "FULL",
    },
];

fn catalog_databases() -> impl Iterator<Item = &'static DatabaseRow> {
    CATALOG_DATABASES.iter().chain(USER_DATABASES.iter())
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
                ("is_fulltext_enabled", DataType::Bit, false),
                ("recovery_model", DataType::TinyInt, false),
                (
                    "recovery_model_desc",
                    DataType::VarChar { max_len: 60 },
                    false,
                ),
                ("is_auto_close_on", DataType::Bit, false),
                ("is_auto_shrink_on", DataType::Bit, false),
                ("is_in_standby", DataType::Bit, false),
                ("is_distributor", DataType::Bit, false),
                ("is_cleanly_shutdown", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        catalog_databases()
            .map(|db| StoredRow {
                values: vec![
                    Value::Int(db.id),
                    Value::VarChar(db.name.to_string()),
                    Value::Null,
                    Value::VarBinary(vec![0x01]),
                    created.clone(),
                    Value::TinyInt(db.compatibility_level),
                    Value::VarChar("SQL_Latin1_General_CP1_CI_AS".to_string()),
                    Value::TinyInt(0),
                    Value::VarChar("ONLINE".to_string()),
                    Value::TinyInt(0),
                    Value::VarChar("MULTI_USER".to_string()),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::TinyInt(match db.recovery_model {
                        "SIMPLE" => 3,
                        "BULK_LOGGED" => 2,
                        _ => 1,
                    }),
                    Value::VarChar(db.recovery_model.to_string()),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(db.name.eq_ignore_ascii_case("master")),
                ],
                deleted: false,
            })
            .collect()
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
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        catalog_databases()
            .map(|db| StoredRow {
                values: vec![
                    Value::VarChar(db.name.to_string()),
                    Value::SmallInt(db.id as i16),
                    Value::VarBinary(vec![0x01]),
                    Value::SmallInt(0),
                    Value::Int(0),
                    Value::Int(0),
                    created.clone(),
                    Value::TinyInt(db.compatibility_level),
                    Value::Null,
                    Value::Int(0),
                ],
                deleted: false,
            })
            .collect()
    }
}
