use tsql_core::{Database, StatementExecutor};

fn main() {
    let db = Database::new();
    let session_id = db.create_session();

    let sql = "SELECT name FROM master.sys.databases";
    match db.executor().execute_session_batch_sql(session_id, sql) {
        Ok(Some(result)) => {
            println!("Rows: {}", result.rows.len());
            for row in result.rows {
                println!("Database: {:?}", row[0]);
            }
        }
        Ok(None) => println!("No result set"),
        Err(e) => println!("Error: {}", e),
    }
}
