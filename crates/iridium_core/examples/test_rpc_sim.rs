use iridium_core::{Database, StatementExecutor};

fn main() {
    let db = Database::new();
    let session_id = db.create_session();

    // Simulate sp_executesql call for database list
    let sql = "SELECT dtb.name AS [Database_Name] FROM master.sys.databases AS dtb";

    // SSMS often uses sp_executesql with parameters even if not needed
    // iridium_server prepends DECLARE for parameters.
    let full_sql = format!("DECLARE @_msparam_0 nvarchar(4000) = N'master'; {}", sql);

    match db
        .executor()
        .execute_session_batch_sql(session_id, &full_sql)
    {
        Ok(Some(result)) => {
            println!("Rows: {}", result.rows.len());
            for row in result.rows {
                println!("Row: {:?}", row);
            }
        }
        Ok(None) => println!("No result set"),
        Err(e) => println!("Error: {}", e),
    }
}
