use super::super::super::virtual_table_def;
use super::super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysTables;

impl VirtualTable for SysTables {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "tables",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("schema_id", DataType::Int, false),
                ("principal_id", DataType::Int, true),
                ("is_replicated", DataType::Bit, false),
                (
                    "lock_escalation_desc",
                    DataType::VarChar { max_len: 60 },
                    false,
                ),
                ("lob_data_space_id", DataType::Int, true),
                ("filestream_data_space_id", DataType::Int, true),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_memory_optimized", DataType::Bit, false),
                ("is_ms_shipped", DataType::Bit, false),
                ("is_filetable", DataType::Bit, false),
                ("temporal_type", DataType::TinyInt, false),
                ("is_external", DataType::Bit, false),
                ("is_node", DataType::Bit, false),
                ("is_edge", DataType::Bit, false),
                ("ledger_type", DataType::Int, true),
                ("is_dropped_ledger_table", DataType::Bit, false),
                ("durability", DataType::TinyInt, false),
                ("durability_desc", DataType::VarChar { max_len: 60 }, false),
                ("history_table_id", DataType::Int, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        catalog
            .get_tables()
            .iter()
            .map(|t| StoredRow {
                values: vec![
                    Value::Int(t.id as i32),
                    Value::VarChar(t.name.clone()),
                    Value::Int(t.schema_id as i32),
                    Value::Null,
                    Value::Bit(false),
                    Value::VarChar("TABLE".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Char("U ".to_string()),
                    Value::VarChar("USER_TABLE".to_string()),
                    created.clone(),
                    created.clone(),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::TinyInt(0),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Int(0),
                    Value::Bit(false),
                    Value::TinyInt(0), // durability (SCHEMA_AND_DATA)
                    Value::VarChar("SCHEMA_AND_DATA".to_string()), // durability_desc
                    Value::Null,       // history_table_id
                ],
                deleted: false,
            })
            .collect()
    }
}
