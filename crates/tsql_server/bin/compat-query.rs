//! Minimal CLI that runs a SQL query against tsql_core with playground data.
//! Outputs rows as pipe-delimited text, one row per line, sorted.
//! Used by the compatibility test runner to compare against Azure SQL Edge.
//!
//! Usage: compat-query "SELECT 1 as n"

use tsql_core::types::Value;
use tsql_core::{Database, StatementExecutor};
use tsql_server::playground;

fn format_compat_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        // SqlClient materializes SQL DATE as DateTime at midnight in the C# runner.
        Value::Date(v) => format!("{} 00:00:00", v.format("%Y-%m-%d")),
        Value::DateTime(v) | Value::DateTime2(v) => v.format("%Y-%m-%d %H:%M:%S").to_string(),
        other => other.to_string_value(),
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

    let db = Database::new();
    if let Err(e) = playground::seed_playground(&db) {
        eprintln!("ERROR: seed failed: {}", e);
        std::process::exit(1);
    }

    let session_id = db.create_session();

    match db
        .executor()
        .execute_session_batch_sql_multi(session_id, &sql)
    {
        Ok(results) => {
            // Collect all result-set rows across all statements
            let mut lines: Vec<String> = Vec::new();
            for result in results.into_iter().flatten() {
                if result.columns.is_empty() {
                    continue;
                }
                for row in &result.rows {
                    let cols: Vec<String> = row.iter().map(format_compat_value).collect();
                    lines.push(cols.join("|"));
                }
            }
            lines.sort();
            for line in &lines {
                println!("{}", line);
            }
        }
        Err(e) => {
            eprintln!("ERROR:{}:{}:{}:{}", e.number(), e.class_severity(), 1, e);
            std::process::exit(1);
        }
    }

    let _ = db.close_session(session_id);
}
