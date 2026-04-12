use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysDmExecSessions;
pub(crate) struct SysDmExecConnections;
pub(crate) struct SysDmExecRequests;

impl VirtualTable for SysDmExecSessions {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_exec_sessions",
            vec![
                ("session_id", DataType::SmallInt, false),
                ("login_time", DataType::DateTime, false),
                ("host_name", DataType::NVarChar { max_len: 128 }, true),
                ("program_name", DataType::NVarChar { max_len: 128 }, true),
                ("login_name", DataType::NVarChar { max_len: 128 }, false),
                ("status", DataType::NVarChar { max_len: 30 }, false),
                ("cpu_time", DataType::Int, false),
                ("memory_usage", DataType::Int, false),
                ("total_scheduled_time", DataType::Int, false),
                ("total_elapsed_time", DataType::Int, false),
                ("last_request_start_time", DataType::DateTime, false),
                ("last_request_end_time", DataType::DateTime, true),
                ("reads", DataType::BigInt, false),
                ("writes", DataType::BigInt, false),
                ("logical_reads", DataType::BigInt, false),
                ("is_user_process", DataType::Bit, false),
                ("text_size", DataType::Int, false),
                ("language", DataType::NVarChar { max_len: 128 }, true),
                ("date_format", DataType::NVarChar { max_len: 3 }, true),
                ("date_first", DataType::SmallInt, false),
                ("quoted_identifier", DataType::Bit, false),
                ("arithabort", DataType::Bit, false),
                ("ansi_null_dflt_on", DataType::Bit, false),
                ("ansi_defaults", DataType::Bit, false),
                ("ansi_warnings", DataType::Bit, false),
                ("ansi_padding", DataType::Bit, false),
                ("ansi_nulls", DataType::Bit, false),
                ("concat_null_yields_null", DataType::Bit, false),
                ("transaction_isolation_level", DataType::SmallInt, false),
                ("lock_timeout", DataType::Int, false),
                ("deadlock_priority", DataType::Int, false),
                ("row_count", DataType::BigInt, false),
                ("prev_error", DataType::Int, false),
                ("original_login_name", DataType::NVarChar { max_len: 128 }, false),
                ("database_id", DataType::SmallInt, false),
                ("open_transaction_count", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        let now = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        // Return a single row representing the current session shim.
        // In a real implementation, this would be populated from the engine's session manager.
        vec![StoredRow {
            values: vec![
                Value::SmallInt(51), // session_id
                now.clone(),        // login_time
                Value::NVarChar("localhost".to_string()),
                Value::NVarChar("tsql-wasm".to_string()),
                Value::NVarChar("sa".to_string()),
                Value::NVarChar("running".to_string()),
                Value::Int(0),    // cpu_time
                Value::Int(1024), // memory_usage
                Value::Int(0),    // total_scheduled_time
                Value::Int(0),    // total_elapsed_time
                now.clone(),      // last_request_start_time
                Value::Null,      // last_request_end_time
                Value::BigInt(0), // reads
                Value::BigInt(0), // writes
                Value::BigInt(0), // logical_reads
                Value::Bit(true), // is_user_process
                Value::Int(4096), // text_size
                Value::NVarChar("us_english".to_string()),
                Value::NVarChar("mdy".to_string()),
                Value::SmallInt(7), // date_first
                Value::Bit(true),   // quoted_identifier
                Value::Bit(true),   // arithabort
                Value::Bit(true),   // ansi_null_dflt_on
                Value::Bit(false),  // ansi_defaults
                Value::Bit(true),   // ansi_warnings
                Value::Bit(true),   // ansi_padding
                Value::Bit(true),   // ansi_nulls
                Value::Bit(true),   // concat_null_yields_null
                Value::SmallInt(2), // transaction_isolation_level (ReadCommitted)
                Value::Int(-1),     // lock_timeout
                Value::Int(0),      // deadlock_priority
                Value::BigInt(0),   // row_count
                Value::Int(0),      // prev_error
                Value::NVarChar("sa".to_string()),
                Value::SmallInt(5), // database_id (tsql_wasm)
                Value::Int(0),      // open_transaction_count
            ],
            deleted: false,
        }]
    }
}

impl VirtualTable for SysDmExecConnections {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_exec_connections",
            vec![
                ("session_id", DataType::Int, true),
                ("most_recent_session_id", DataType::Int, true),
                ("connect_time", DataType::DateTime, false),
                ("net_transport", DataType::NVarChar { max_len: 40 }, false),
                ("protocol_type", DataType::NVarChar { max_len: 40 }, true),
                ("protocol_version", DataType::Int, true),
                ("endpoint_id", DataType::Int, true),
                ("encrypt_option", DataType::NVarChar { max_len: 40 }, false),
                ("auth_scheme", DataType::NVarChar { max_len: 40 }, false),
                ("node_affinity", DataType::SmallInt, false),
                ("num_reads", DataType::Int, true),
                ("num_writes", DataType::Int, true),
                ("last_read", DataType::DateTime, true),
                ("last_write", DataType::DateTime, true),
                ("net_packet_size", DataType::Int, true),
                ("client_net_address", DataType::VarChar { max_len: 48 }, true),
                ("client_tcp_port", DataType::Int, true),
                ("local_net_address", DataType::VarChar { max_len: 48 }, true),
                ("local_tcp_port", DataType::Int, true),
                ("connection_id", DataType::UniqueIdentifier, false),
                ("parent_connection_id", DataType::UniqueIdentifier, true),
                ("most_recent_sql_handle", DataType::VarBinary { max_len: 64 }, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        let now = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        vec![StoredRow {
            values: vec![
                Value::Int(51),
                Value::Int(51),
                now.clone(),
                Value::NVarChar("TCP".to_string()),
                Value::NVarChar("TSQL".to_string()),
                Value::Int(0),
                Value::Int(0),
                Value::NVarChar("FALSE".to_string()),
                Value::NVarChar("SQL".to_string()),
                Value::SmallInt(0),
                Value::Int(0),
                Value::Int(0),
                Value::Null,
                Value::Null,
                Value::Int(4096),
                Value::VarChar("127.0.0.1".to_string()),
                Value::Int(12345),
                Value::VarChar("127.0.0.1".to_string()),
                Value::Int(1433),
                Value::UniqueIdentifier(uuid::Uuid::nil()),
                Value::Null,
                Value::Null,
            ],
            deleted: false,
        }]
    }
}

impl VirtualTable for SysDmExecRequests {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_exec_requests",
            vec![
                ("session_id", DataType::SmallInt, false),
                ("request_id", DataType::Int, false),
                ("start_time", DataType::DateTime, false),
                ("status", DataType::NVarChar { max_len: 30 }, false),
                ("command", DataType::NVarChar { max_len: 32 }, false),
                ("sql_handle", DataType::VarBinary { max_len: 64 }, true),
                ("statement_start_offset", DataType::Int, true),
                ("statement_end_offset", DataType::Int, true),
                ("plan_handle", DataType::VarBinary { max_len: 64 }, true),
                ("database_id", DataType::SmallInt, false),
                ("user_id", DataType::Int, false),
                ("connection_id", DataType::UniqueIdentifier, true),
                ("blocking_session_id", DataType::SmallInt, true),
                ("wait_type", DataType::NVarChar { max_len: 60 }, true),
                ("wait_time", DataType::Int, false),
                ("last_wait_type", DataType::NVarChar { max_len: 60 }, true),
                ("wait_resource", DataType::NVarChar { max_len: 256 }, true),
                ("open_transaction_count", DataType::Int, false),
                ("open_resultset_count", DataType::Int, false),
                ("transaction_id", DataType::BigInt, false),
                ("percent_complete", DataType::Float, false),
                ("estimated_completion_time", DataType::BigInt, false),
                ("cpu_time", DataType::Int, false),
                ("total_elapsed_time", DataType::Int, false),
                ("scheduler_id", DataType::Int, true),
                ("reads", DataType::BigInt, false),
                ("writes", DataType::BigInt, false),
                ("logical_reads", DataType::BigInt, false),
                ("text_size", DataType::Int, false),
                ("language", DataType::NVarChar { max_len: 128 }, true),
                ("date_format", DataType::NVarChar { max_len: 3 }, true),
                ("date_first", DataType::SmallInt, false),
                ("quoted_identifier", DataType::Bit, false),
                ("arithabort", DataType::Bit, false),
                ("ansi_null_dflt_on", DataType::Bit, false),
                ("ansi_defaults", DataType::Bit, false),
                ("ansi_warnings", DataType::Bit, false),
                ("ansi_padding", DataType::Bit, false),
                ("ansi_nulls", DataType::Bit, false),
                ("concat_null_yields_null", DataType::Bit, false),
                ("transaction_isolation_level", DataType::SmallInt, false),
                ("lock_timeout", DataType::Int, false),
                ("deadlock_priority", DataType::Int, false),
                ("row_count", DataType::BigInt, false),
                ("prev_error", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        let now = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        vec![StoredRow {
            values: vec![
                Value::SmallInt(51),
                Value::Int(0),
                now.clone(),
                Value::NVarChar("running".to_string()),
                Value::NVarChar("SELECT".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::SmallInt(5),
                Value::Int(1),
                Value::UniqueIdentifier(uuid::Uuid::nil()),
                Value::Null,
                Value::Null,
                Value::Int(0),
                Value::Null,
                Value::Null,
                Value::Int(0),
                Value::Int(1),
                Value::BigInt(0),
                Value::Float(0f64.to_bits()),
                Value::BigInt(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(1),
                Value::BigInt(0),
                Value::BigInt(0),
                Value::BigInt(0),
                Value::Int(4096),
                Value::NVarChar("us_english".to_string()),
                Value::NVarChar("mdy".to_string()),
                Value::SmallInt(7),
                Value::Bit(true),
                Value::Bit(true),
                Value::Bit(true),
                Value::Bit(false),
                Value::Bit(true),
                Value::Bit(true),
                Value::Bit(true),
                Value::Bit(true),
                Value::SmallInt(2),
                Value::Int(-1),
                Value::Int(0),
                Value::BigInt(0),
                Value::Int(0),
            ],
            deleted: false,
        }]
    }
}
