use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysPolicyConfiguration;
pub(crate) struct SysPolicySystemHealthState;

impl VirtualTable for SysPolicyConfiguration {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "syspolicy_configuration",
            vec![
                ("configuration_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("current_value", DataType::Int, false),
                ("minimum", DataType::Int, false),
                ("maximum", DataType::Int, false),
                ("is_dynamic", DataType::Bit, false),
                ("is_advanced", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![
            StoredRow {
                values: vec![
                    Value::Int(1),
                    Value::VarChar("Enabled".to_string()),
                    Value::Int(1),
                    Value::Int(0),
                    Value::Int(1),
                    Value::Bit(true),
                    Value::Bit(true),
                ],
                deleted: false,
            },
            StoredRow {
                values: vec![
                    Value::Int(2),
                    Value::VarChar("HistoryRetentionInDays".to_string()),
                    Value::Int(90),
                    Value::Int(0),
                    Value::Int(3650),
                    Value::Bit(true),
                    Value::Bit(true),
                ],
                deleted: false,
            },
            StoredRow {
                values: vec![
                    Value::Int(3),
                    Value::VarChar("LogOnSuccess".to_string()),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Int(1),
                    Value::Bit(true),
                    Value::Bit(true),
                ],
                deleted: false,
            },
        ]
    }
}

impl VirtualTable for SysPolicySystemHealthState {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "syspolicy_system_health_state",
            vec![
                ("target_query_expression_with_id", DataType::NVarChar { max_len: 4000 }, false),
                ("policy_id", DataType::Int, false),
                ("policy_health_state", DataType::TinyInt, false),
                ("last_run_date", DataType::DateTime, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![StoredRow {
            values: vec![
                Value::NVarChar("Server/Database".to_string()),
                Value::Int(1),
                Value::TinyInt(1),
                Value::DateTime(
                    chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                        .unwrap()
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                ),
            ],
            deleted: false,
        }]
    }
}
