use tsql_core::{Database, StatementExecutor};

fn main() {
    let db = Database::new();
    let session_id = db.create_session();

    // Full SSMS database list query as seen in proxy logs (reconstructed)
    let sql = r#"
SELECT dtb.name AS [Database_Name], 
       'Server[@Name=' + quotename(CAST(serverproperty(N'Servername') AS sysname),'''') + ']' + '/Database[@Name=' + quotename(dtb.name,'''') + ']' AS [Database_Urn], 
       case when dtb.collation_name is null then 0x200 else 0 end | 
       case when 1 = dtb.is_in_standby then 0x40 else 0 end | 
       case dtb.state when 1 then 0x2 when 2 then 0x8 when 3 then 0x4 when 4 then 0x10 when 5 then 0x100 when 6 then 0x20 else 1 end AS [Database_Status], 
       dtb.compatibility_level AS [Database_CompatibilityLevel]
FROM sys.databases AS dtb
WHERE (CAST(case when dtb.name in ('master','model','msdb','tempdb') then 1 else 0 end AS bit)=0 OR 
       CAST(ISNULL(HAS_PERMS_BY_NAME(dtb.name, 'DATABASE', 'VIEW DATABASE STATE'), HAS_PERMS_BY_NAME(null, null, 'VIEW SERVER STATE')) AS bit)=1)
ORDER BY [Database_Name] ASC
"#;

    println!("=== Test 1: Direct execution ===");
    println!("Executing query:\n{}\n", sql);
    
    match db.executor().execute_session_batch_sql_multi(session_id, sql) {
        Ok(results) => {
            println!("Number of result sets: {}", results.len());
            for (i, result_opt) in results.iter().enumerate() {
                match result_opt {
                    Some(result) => {
                        println!("\nResult set {}:", i);
                        println!("  Columns: {:?}", result.columns);
                        println!("  Rows: {}", result.rows.len());
                        for row in &result.rows {
                            println!("  Row: {:?}", row);
                        }
                    }
                    None => println!("\nResult set {}: (no result - statement)", i),
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    // Now test with DECLARE preamble (simulating sp_executesql)
    println!("\n=== Test 2: With DECLARE preamble (simulating RPC) ===");
    
    let full_sql = format!(r#"
{}
"#, sql);

    match db.executor().execute_session_batch_sql_multi(session_id, &full_sql) {
        Ok(results) => {
            println!("Number of result sets: {}", results.len());
            for (i, result_opt) in results.iter().enumerate() {
                match result_opt {
                    Some(result) => {
                        println!("\nResult set {}:", i);
                        println!("  Columns: {:?}", result.columns);
                        println!("  Rows: {}", result.rows.len());
                    }
                    None => println!("\nResult set {}: (no result - statement)", i),
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
}
