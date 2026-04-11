use tsql_core::{Database, StatementExecutor};

fn main() {
    let db = Database::new();
    let session_id = db.create_session();
    
    // Create a simple table manually
    println!("=== Creating test table ===");
    match db.executor().execute_session_batch_sql(session_id, "CREATE TABLE test_tbl (id INT PRIMARY KEY, name VARCHAR(50))") {
        Ok(_) => println!("Table created"),
        Err(e) => println!("Error creating table: {}", e),
    }

    // Test 1: Simple sys.tables query
    println!("=== Test 1: SELECT name FROM sys.tables ===");
    match db.executor().execute_session_batch_sql_multi(session_id, "SELECT name FROM sys.tables") {
        Ok(results) => {
            for (i, result_opt) in results.iter().enumerate() {
                match result_opt {
                    Some(result) => {
                        println!("Result set {}:", i);
                        println!("  Columns: {:?}", result.columns);
                        println!("  Rows: {}", result.rows.len());
                        for row in &result.rows {
                            println!("  Row: {:?}", row);
                        }
                    }
                    None => println!("Result set {}: (no result)", i),
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Test 2: sys.tables with JOIN sys.indexes (simplified SSMS query)
    println!("\n=== Test 2: sys.tables JOIN sys.indexes ===");
    let sql = r#"
SELECT tbl.name AS [Name], SCHEMA_NAME(tbl.schema_id) AS [Schema]
FROM sys.tables AS tbl
INNER JOIN sys.indexes AS idx ON idx.object_id = tbl.object_id
WHERE idx.index_id < 2
"#;
    match db.executor().execute_session_batch_sql_multi(session_id, sql) {
        Ok(results) => {
            for (i, result_opt) in results.iter().enumerate() {
                match result_opt {
                    Some(result) => {
                        println!("Result set {}:", i);
                        println!("  Columns: {:?}", result.columns);
                        println!("  Rows: {}", result.rows.len());
                        for row in &result.rows {
                            println!("  Row: {:?}", row);
                        }
                    }
                    None => println!("Result set {}: (no result)", i),
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Test 3: Check if sys.indexes has data
    println!("\n=== Test 3: sys.indexes ===");
    match db.executor().execute_session_batch_sql_multi(session_id, "SELECT object_id, index_id, type, name FROM sys.indexes") {
        Ok(results) => {
            for (i, result_opt) in results.iter().enumerate() {
                match result_opt {
                    Some(result) => {
                        println!("Result set {}:", i);
                        println!("  Columns: {:?}", result.columns);
                        println!("  Rows: {}", result.rows.len());
                        for row in &result.rows {
                            println!("  Row: {:?}", row);
                        }
                    }
                    None => println!("Result set {}: (no result)", i),
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
    
    // Test 4: Check table IDs
    println!("\n=== Test 4: sys.tables with object_id ===");
    match db.executor().execute_session_batch_sql_multi(session_id, "SELECT object_id, name, schema_id FROM sys.tables") {
        Ok(results) => {
            for (i, result_opt) in results.iter().enumerate() {
                match result_opt {
                    Some(result) => {
                        println!("Result set {}:", i);
                        println!("  Columns: {:?}", result.columns);
                        println!("  Rows: {}", result.rows.len());
                        for row in &result.rows {
                            println!("  Row: {:?}", row);
                        }
                    }
                    None => println!("Result set {}: (no result)", i),
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
}
