use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
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
                ("host_process_id", DataType::Int, true),
                ("client_version", DataType::Int, true),
                ("client_interface_name", DataType::NVarChar { max_len: 32 }, true),
                ("security_id", DataType::VarBinary { max_len: 85 }, false),
                ("login_name", DataType::NVarChar { max_len: 128 }, false),
                ("nt_domain", DataType::NVarChar { max_len: 128 }, true),
                ("nt_user_name", DataType::NVarChar { max_len: 128 }, true),
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
                ("original_security_id", DataType::VarBinary { max_len: 85 }, false),
                ("original_login_name", DataType::NVarChar { max_len: 128 }, false),
                ("last_successful_logon", DataType::DateTime, true),
                ("last_unsuccessful_logon", DataType::DateTime, true),
                ("unsuccessful_logons", DataType::BigInt, true),
                ("group_id", DataType::Int, false),
                ("database_id", DataType::SmallInt, false),
                ("authenticating_database_id", DataType::Int, true),
                ("open_transaction_count", DataType::Int, false),
                ("context_info", DataType::VarBinary { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, ctx: &ExecutionContext) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        vec![StoredRow {
            values: vec![
                Value::SmallInt(ctx.metadata.id as i16),
                created.clone(),
                ctx.metadata
                    .host_name
                    .as_ref()
                    .map(|s| Value::NVarChar(s.clone()))
                    .unwrap_or(Value::Null),
                ctx.metadata
                    .app_name
                    .as_ref()
                    .map(|s| Value::NVarChar(s.clone()))
                    .unwrap_or(Value::NVarChar("iridium-sql".to_string())),
                Value::Int(1234), // host_process_id
                Value::Int(1),    // client_version
                Value::NVarChar("TDS".to_string()),
                Value::VarBinary(vec![0x01]), // security_id
                Value::NVarChar(ctx.metadata.user.clone().unwrap_or_else(|| "sa".to_string())),
                Value::Null, // nt_domain
                Value::Null, // nt_user_name
                Value::NVarChar("running".to_string()),
                Value::Int(0), // cpu_time
                Value::Int(0), // memory_usage
                Value::Int(0), // total_scheduled_time
                Value::Int(0), // total_elapsed_time
                created.clone(),
                Value::Null,
                Value::BigInt(0),
                Value::BigInt(0),
                Value::BigInt(0),
                Value::Bit(true),
                Value::Int(4096),
                Value::NVarChar("us_english".to_string()),
                Value::NVarChar("mdy".to_string()),
                Value::SmallInt(ctx.metadata.datefirst as i16),
                Value::Bit(true),
                Value::Bit(false),
                Value::Bit(true),
                Value::Bit(false),
                Value::Bit(true),
                Value::Bit(true),
                Value::Bit(ctx.metadata.ansi_nulls),
                Value::Bit(true),
                Value::SmallInt(2), // READ COMMITTED
                Value::Int(-1),
                Value::Int(0),
                Value::BigInt(0),
                Value::Int(0),
                Value::VarBinary(vec![0x01]),
                Value::NVarChar(ctx.metadata.user.clone().unwrap_or_else(|| "sa".to_string())),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Int(1),
                Value::SmallInt(5), // iridium_sql
                Value::Int(5),
                Value::Int(ctx.frame.trancount as i32),
                Value::VarBinary(ctx.session.context_info.clone()),
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
                ("nest_level", DataType::Int, false),
                ("granted_query_memory", DataType::Int, false),
                ("executing_managed_code", DataType::Bit, false),
                ("group_id", DataType::Int, false),
                ("query_hash", DataType::VarBinary { max_len: 8 }, true),
                ("query_plan_hash", DataType::VarBinary { max_len: 8 }, true),
                ("statement_sql_handle", DataType::VarBinary { max_len: 64 }, true),
                ("statement_context_id", DataType::BigInt, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, ctx: &ExecutionContext) -> Vec<StoredRow> {
        let now = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        vec![StoredRow {
            values: vec![
                Value::SmallInt(ctx.metadata.id as i16),
                Value::Int(0),        // request_id
                now,                  // start_time
                Value::NVarChar("running".to_string()),
                Value::NVarChar("SELECT".to_string()), // command
                Value::Null,          // sql_handle
                Value::Int(0),        // start_offset
                Value::Int(-1),       // end_offset
                Value::Null,          // plan_handle
                Value::SmallInt(5),   // database_id
                Value::Int(1),        // user_id
                Value::UniqueIdentifier(uuid::Uuid::nil()),
                Value::Null,          // blocking_session_id
                Value::Null,          // wait_type
                Value::Int(0),        // wait_time
                Value::Null,          // last_wait_type
                Value::Null,          // wait_resource
                Value::Int(ctx.frame.trancount as i32),
                Value::Int(1),        // open_resultset_count
                Value::BigInt(0),     // transaction_id
                Value::Float(0.0f64.to_bits()), // percent_complete
                Value::BigInt(0),     // estimated_completion_time
                Value::Int(0),        // cpu_time
                Value::Int(0),        // total_elapsed_time
                Value::Int(1),        // scheduler_id
                Value::BigInt(0),     // reads
                Value::BigInt(0),     // writes
                Value::BigInt(0),     // logical_reads
                Value::Int(4096),     // text_size
                Value::NVarChar("us_english".to_string()),
                Value::NVarChar("mdy".to_string()),
                Value::SmallInt(ctx.metadata.datefirst as i16),
                Value::Bit(true),     // quoted_identifier
                Value::Bit(false),    // arithabort
                Value::Bit(true),     // ansi_null_dflt_on
                Value::Bit(false),    // ansi_defaults
                Value::Bit(true),     // ansi_warnings
                Value::Bit(true),     // ansi_padding
                Value::Bit(ctx.metadata.ansi_nulls),
                Value::Bit(true),     // concat_null_yields_null
                Value::SmallInt(2),   // transaction_isolation_level
                Value::Int(-1),       // lock_timeout
                Value::Int(0),        // deadlock_priority
                Value::BigInt(0),     // row_count
                Value::Int(0),        // prev_error
                Value::Int(0),        // nest_level
                Value::Int(0),        // granted_query_memory
                Value::Bit(false),    // executing_managed_code
                Value::Int(1),        // group_id
                Value::Null,          // query_hash
                Value::Null,          // query_plan_hash
                Value::Null,          // statement_sql_handle
                Value::Null,          // statement_context_id
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

    fn rows(&self, _catalog: &dyn Catalog, ctx: &ExecutionContext) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        vec![StoredRow {
            values: vec![
                Value::Int(ctx.metadata.id as i32),
                Value::Int(ctx.metadata.id as i32),
                created.clone(),
                Value::NVarChar("TCP".to_string()),
                Value::NVarChar("TSQL".to_string()),
                Value::Int(0),
                Value::Int(1),
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
