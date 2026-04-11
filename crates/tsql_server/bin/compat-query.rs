//! Minimal CLI that runs a SQL query against tsql_core with playground data.
//! Outputs a structured JSON envelope so the compatibility runner can compare
//! row values, column metadata, and error shapes.
//!
//! Usage: compat-query "SELECT 1 as n"

use serde::Serialize;
use tsql_core::types::{DataType, Value};
use tsql_core::{Database, DbError, QueryResult, StatementExecutor};
use tsql_server::playground;
use std::io::Write;

fn format_compat_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        // SqlClient materializes SQL DATE as DateTime at midnight in the C# runner.
        Value::Date(v) => format!("{} 00:00:00", v.format("%Y-%m-%d")),
        Value::DateTime(v) | Value::DateTime2(v) => v.format("%Y-%m-%d %H:%M:%S").to_string(),
        other => other.to_string_value(),
    }
}

fn format_compat_type(value: &DataType) -> String {
    match value {
        DataType::Bit => "bit".to_string(),
        DataType::TinyInt => "tinyint".to_string(),
        DataType::SmallInt => "smallint".to_string(),
        DataType::Int => "int".to_string(),
        DataType::BigInt => "bigint".to_string(),
        DataType::Float => "float".to_string(),
        DataType::Decimal { .. } => "decimal".to_string(),
        DataType::Money => "money".to_string(),
        DataType::SmallMoney => "smallmoney".to_string(),
        DataType::Char { .. } => "char".to_string(),
        DataType::VarChar { .. } => "varchar".to_string(),
        DataType::NChar { .. } => "nchar".to_string(),
        DataType::NVarChar { .. } => "nvarchar".to_string(),
        DataType::Binary { .. } => "binary".to_string(),
        DataType::VarBinary { .. } => "varbinary".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time => "time".to_string(),
        DataType::DateTime => "datetime".to_string(),
        DataType::DateTime2 => "datetime2".to_string(),
        DataType::UniqueIdentifier => "uniqueidentifier".to_string(),
        DataType::SqlVariant => "sql_variant".to_string(),
        DataType::Xml => "xml".to_string(),
    }
}

fn normalize_error_message(message: &str) -> String {
    message.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn execute_query(sql: &str) -> QueryEnvelope {
    let db = Database::new();
    if let Err(e) = playground::seed_playground(&db) {
        return QueryEnvelope::error_from_db_error(DbError::Execution(format!(
            "seed failed: {}",
            e
        )));
    }

    let session_id = db.create_session();
    let result = db
        .executor()
        .execute_session_batch_sql_multi(session_id, sql);
    let _ = db.close_session(session_id);

    match result {
        Ok(result_sets) => {
            let mut output_sets = Vec::new();
            for result in result_sets.into_iter().flatten() {
                output_sets.push(to_envelope_result_set(&result));
            }
            QueryEnvelope::ok(output_sets)
        }
        Err(e) => QueryEnvelope::error_from_db_error(e),
    }
}

fn to_envelope_result_set(result: &QueryResult) -> ResultSetEnvelope {
    let columns = result.columns.clone();
    let column_types = result
        .column_types
        .iter()
        .map(format_compat_type)
        .collect::<Vec<_>>();
    let mut rows = result
        .rows
        .iter()
        .map(|row| row.iter().map(format_compat_value).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.cmp(right));

    ResultSetEnvelope {
        columns,
        column_types,
        rows,
        row_count: result.rows.len(),
    }
}

fn main() {
    let sql = match std::env::args().nth(1) {
        Some(s) => s,
        None => {
            eprintln!("Usage: compat-query \"SQL\"");
            std::process::exit(1);
        }
    };

    let envelope = execute_query(&sql);
    match serde_json::to_string(&envelope) {
        Ok(json) => {
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            writeln!(handle, "{}", json).expect("failed to write compatibility response");
            handle.flush().expect("failed to flush compatibility response");
        }
        Err(e) => {
            eprintln!("ERROR:0:0:0:failed to serialize compatibility response: {}", e);
            std::process::exit(1);
        }
    }

    std::process::exit(if envelope.ok { 0 } else { 1 });
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QueryEnvelope {
    ok: bool,
    error: Option<ErrorEnvelope>,
    result_sets: Vec<ResultSetEnvelope>,
}

impl QueryEnvelope {
    fn ok(result_sets: Vec<ResultSetEnvelope>) -> Self {
        Self {
            ok: true,
            error: None,
            result_sets,
        }
    }

    fn error_from_db_error(error: DbError) -> Self {
        Self {
            ok: false,
            error: Some(ErrorEnvelope {
                number: error.number(),
                class: error.class_severity(),
                state: 1,
                code: error.code().to_string(),
                message: normalize_error_message(&error.to_string()),
            }),
            result_sets: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorEnvelope {
    number: i32,
    class: u8,
    state: u8,
    code: String,
    message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ResultSetEnvelope {
    columns: Vec<String>,
    column_types: Vec<String>,
    rows: Vec<Vec<String>>,
    row_count: usize,
}
