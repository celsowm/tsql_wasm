use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use crate::executor::script::ScriptExecutor;
use crate::types::{DataType, Value};
use crate::ast::statements::procedural::ExecProcedureStmt;
use crate::executor::evaluator::eval_expr;

pub(crate) fn execute_sp_who(ctx: &ExecutionContext<'_>) -> Result<QueryResult, DbError> {
    let rows = vec![vec![
        Value::Int(ctx.metadata.id as i32),
        Value::Int(0), // ecid
        Value::NVarChar("running".to_string()),
        Value::NVarChar(
            ctx.metadata
                .user
                .clone()
                .unwrap_or_else(|| "sa".to_string()),
        ),
        Value::NVarChar(
            ctx.metadata
                .host_name
                .clone()
                .unwrap_or_else(|| "localhost".to_string()),
        ),
        Value::Char("0".to_string()), // blk
        Value::NVarChar(ctx.metadata.database.clone().unwrap_or_default()),
        Value::NVarChar("SELECT".to_string()), // cmd
        Value::Int(0),                         // request_id
    ]];

    Ok(QueryResult {
        columns: vec![
            "spid".into(),
            "ecid".into(),
            "status".into(),
            "loginame".into(),
            "hostname".into(),
            "blk".into(),
            "dbname".into(),
            "cmd".into(),
            "request_id".into(),
        ],
        column_types: vec![
            DataType::Int,
            DataType::Int,
            DataType::NVarChar { max_len: 30 },
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 128 },
            DataType::Char { len: 5 },
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 16 },
            DataType::Int,
        ],
        column_nullabilities: vec![
            false, false, false, false, false, false, false, false, false,
        ],
        rows,
        ..Default::default()
    })
}

pub(crate) fn execute_sp_databases() -> Result<QueryResult, DbError> {
    let mut rows = Vec::new();
    for db in crate::executor::database_catalog::builtin_databases() {
        rows.push(vec![
            Value::VarChar(db.name.to_string()),
            Value::Int(0), // DATABASE_SIZE
            Value::Null,   // REMARKS
        ]);
    }
    Ok(QueryResult {
        columns: vec!["DATABASE_NAME".into(), "DATABASE_SIZE".into(), "REMARKS".into()],
        column_types: vec![
            DataType::VarChar { max_len: 128 },
            DataType::Int,
            DataType::VarChar { max_len: 254 },
        ],
        column_nullabilities: vec![false, false, true],
        rows,
        ..Default::default()
    })
}

pub(crate) fn execute_sp_server_info() -> Result<QueryResult, DbError> {
    let rows = vec![
        vec![
            Value::Int(1),
            Value::VarChar("DBMS_NAME".into()),
            Value::VarChar("SQL Server".into()),
        ],
        vec![
            Value::Int(2),
            Value::VarChar("DBMS_VER".into()),
            Value::VarChar("Microsoft SQL Server 2025 - 17.0.1000.0".into()),
        ],
        vec![
            Value::Int(10),
            Value::VarChar("OWNER_TERM".into()),
            Value::VarChar("owner".into()),
        ],
        vec![
            Value::Int(11),
            Value::VarChar("TABLE_TERM".into()),
            Value::VarChar("table".into()),
        ],
        vec![
            Value::Int(12),
            Value::VarChar("MAX_OWNER_NAME_LENGTH".into()),
            Value::VarChar("128".into()),
        ],
        vec![
            Value::Int(13),
            Value::VarChar("TABLE_LENGTH".into()),
            Value::VarChar("128".into()),
        ],
    ];
    Ok(QueryResult {
        columns: vec![
            "ATTRIBUTE_ID".into(),
            "ATTRIBUTE_NAME".into(),
            "ATTRIBUTE_VALUE".into(),
        ],
        column_types: vec![
            DataType::Int,
            DataType::VarChar { max_len: 60 },
            DataType::VarChar { max_len: 255 },
        ],
        column_nullabilities: vec![false, false, false],
        rows,
        ..Default::default()
    })
}

pub(crate) fn execute_sp_monitor(exec: &ScriptExecutor<'_>) -> Result<QueryResult, DbError> {
    let now = Value::DateTime(exec.clock.now_datetime_literal());
    let rows = vec![vec![
        now.clone(),   // last_run
        now.clone(),   // current_run
        Value::Int(0), // seconds
        Value::Int(0), // cpu_busy
        Value::Int(0), // io_busy
        Value::Int(0), // idle
        Value::Int(0), // packets_received
        Value::Int(0), // packets_sent
        Value::Int(0), // packet_errors
        Value::Int(0), // total_read
        Value::Int(0), // total_write
        Value::Int(0), // total_errors
        Value::Int(0), // connections
    ]];
    Ok(QueryResult {
        columns: vec![
            "last_run".into(),
            "current_run".into(),
            "seconds".into(),
            "cpu_busy".into(),
            "io_busy".into(),
            "idle".into(),
            "packets_received".into(),
            "packets_sent".into(),
            "packet_errors".into(),
            "total_read".into(),
            "total_write".into(),
            "total_errors".into(),
            "connections".into(),
        ],
        column_types: vec![
            DataType::DateTime,
            DataType::DateTime,
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
        ],
        column_nullabilities: vec![
            false, false, false, false, false, false, false, false, false, false, false, false,
            false,
        ],
        rows,
        ..Default::default()
    })
}

pub(crate) fn execute_sp_set_session_context(
    stmt: &ExecProcedureStmt,
    ctx: &mut ExecutionContext<'_>,
    exec: &mut ScriptExecutor<'_>,
) -> Result<QueryResult, DbError> {
    let mut key = String::new();
    let mut value = Value::Null;
    let mut read_only = false;

    for arg in &stmt.args {
        let val = eval_expr(&arg.expr, &[], ctx, exec.catalog, exec.storage, exec.clock)?;
        match arg.name.as_ref().map(|s| s.to_ascii_lowercase()) {
            Some(ref n) if n == "@key" => key = val.to_string_value(),
            Some(ref n) if n == "@value" => value = val,
            Some(ref n) if n == "@read_only" => read_only = val.to_bool().unwrap_or(false),
            _ => {
                // Positional arguments fallback if needed, but MSSQL usually uses named for this
            }
        }
    }

    if key.is_empty() {
        return Err(DbError::Execution(
            "sp_set_session_context: @key is required".into(),
        ));
    }

    if let Some((_, is_ro)) = ctx.session.session_context.get(&key) {
        if *is_ro {
            return Err(DbError::Execution(format!(
                "Cannot set value for read-only session context key '{}'",
                key
            )));
        }
    }

    ctx.session.session_context.insert(key, (value, read_only));

    Ok(QueryResult::default())
}
