use tsql_core::{Database, StatementExecutor};

fn main() {
    let db = Database::new();
    let session_id = db.create_session();

    // Seed a table
    db.executor()
        .execute_session_batch_sql(session_id, "CREATE TABLE t1 (id INT)")
        .unwrap();

    // Switch to tempdb
    db.executor()
        .set_session_database(session_id, "tempdb".to_string())
        .unwrap();

    let sql = "SELECT name FROM sys.tables WHERE is_ms_shipped = 0";

    match db.executor().execute_session_batch_sql(session_id, sql) {
        Ok(Some(result)) => {
            println!("Database: {:?}", db.session_options(session_id).unwrap()); // I want to see current db
            println!("Rows: {}", result.rows.len());
            for row in result.rows {
                println!("Row: {:?}", row);
            }
        }
        Ok(None) => println!("No result set"),
        Err(e) => println!("Error: {}", e),
    }
}
