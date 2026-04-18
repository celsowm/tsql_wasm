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
                ("parent_object_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_memory_optimized", DataType::Bit, false),
                ("is_ms_shipped", DataType::Bit, false),
                ("is_filetable", DataType::Bit, false),
                ("temporal_type", DataType::TinyInt, false),
                ("temporal_type_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_external", DataType::Bit, false),
                ("is_node", DataType::Bit, false),
                ("is_edge", DataType::Bit, false),
                ("ledger_type", DataType::Int, true),
                ("is_dropped_ledger_table", DataType::Bit, false),
                ("durability", DataType::TinyInt, false),
                ("durability_desc", DataType::VarChar { max_len: 60 }, false),
                ("history_table_id", DataType::Int, true),
                ("is_replicated", DataType::Bit, false),
                ("lock_escalation", DataType::TinyInt, false),
                ("lock_escalation_desc", DataType::VarChar { max_len: 60 }, false),
                ("lob_data_space_id", DataType::Int, true),
                ("filestream_data_space_id", DataType::Int, true),
                ("max_column_id_used", DataType::Int, false),
                ("lock_on_bulk_load", DataType::Bit, false),
                ("uses_ansi_null_defaults", DataType::Bit, false),
                ("is_tracked_by_cdc", DataType::Bit, false),
                ("is_merge_published", DataType::Bit, false),
                ("is_filetable", DataType::Bit, false),
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
                    Value::Int(t.id as i32),          // object_id
                    Value::VarChar(t.name.clone()),   // name
                    Value::Int(t.schema_id as i32),   // schema_id
                    Value::Null,                      // principal_id
                    Value::Int(0),                    // parent_object_id
                    Value::Char("U ".to_string()),    // type
                    Value::VarChar("USER_TABLE".to_string()), // type_desc
                    created.clone(),                  // create_date
                    created.clone(),                  // modify_date
                    Value::Bit(false),                // is_memory_optimized
                    Value::Bit(false),                // is_ms_shipped
                    Value::Bit(false),                // is_filetable
                    Value::TinyInt(0),                // temporal_type
                    Value::VarChar("0".to_string()),  // temporal_type_desc
                    Value::Bit(false),                // is_external
                    Value::Bit(false),                // is_node
                    Value::Bit(false),                // is_edge
                    Value::Int(0),                    // ledger_type
                    Value::Bit(false),                // is_dropped_ledger_table
                    Value::TinyInt(0),                // durability
                    Value::VarChar("SCHEMA_AND_DATA".to_string()), // durability_desc
                    Value::Null,                      // history_table_id
                    Value::Bit(false),                // is_replicated
                    Value::TinyInt(0),                // lock_escalation (TABLE)
                    Value::VarChar("TABLE".to_string()), // lock_escalation_desc
                    Value::Int(0),                    // lob_data_space_id (default filegroup)
                    Value::Null,                      // filestream_data_space_id
                    Value::Int(0),                    // max_column_id_used
                    Value::Bit(false),                // lock_on_bulk_load
                    Value::Bit(false),                // uses_ansi_null_defaults
                    Value::Bit(false),                // is_tracked_by_cdc
                    Value::Bit(false),                // is_merge_published
                    Value::Bit(false),                // is_filetable (duplicate for SSMS compat)
                ],
                deleted: false,
            })
            .collect()
    }
}
