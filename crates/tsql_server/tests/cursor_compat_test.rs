//! Compatibility tests for cursor operations against Azure SQL Edge.
//!
//! These tests verify that cursor operations work correctly when run
//! against a real SQL Server-compatible instance (Azure SQL Edge via Podman).
//!
//! Run with: cargo test -p tsql_server --test cursor_compat_test -- --ignored

use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// Read Azure SQL Edge connection info from environment or defaults.
fn azure_config() -> Config {
    let host = std::env::var("TSQL_AZURE_HOST").unwrap_or_else(|_| "[::1]".to_string());
    let port: u16 = std::env::var("TSQL_AZURE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(11433);
    let user = std::env::var("TSQL_AZURE_USER").unwrap_or_else(|_| "sa".to_string());
    let password = std::env::var("TSQL_AZURE_PASSWORD").unwrap_or_else(|_| "Tsql12345!".to_string());

    let mut config = Config::new();
    config.host(&host);
    config.port(port);
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::NotSupported);
    config.authentication(tiberius::AuthMethod::sql_server(&user, &password));
    config
}

async fn connect_azure() -> Client<tokio_util::compat::Compat<TcpStream>> {
    let config = azure_config();
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("Failed to connect to Azure SQL Edge");
    tcp.set_nodelay(true).unwrap();

    Client::connect(config, tcp.compat_write())
        .await
        .expect("Failed TDS handshake with Azure SQL Edge")
}

async fn query_azure(
    client: &mut Client<tokio_util::compat::Compat<TcpStream>>,
    sql: &str,
) -> Vec<Vec<String>> {
    let stream = client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("Query failed: {} - {}", sql, e));
    let rows = stream
        .into_first_result()
        .await
        .expect("Failed to read result");

    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    if let Ok(Some(v)) = row.try_get::<&str, _>(i) {
                        return v.to_string();
                    }
                    if let Ok(Some(v)) = row.try_get::<i32, _>(i) {
                        return v.to_string();
                    }
                    if let Ok(Some(v)) = row.try_get::<i64, _>(i) {
                        return v.to_string();
                    }
                    if let Ok(Some(v)) = row.try_get::<bool, _>(i) {
                        return if v { "1".to_string() } else { "0".to_string() };
                    }
                    "NULL".to_string()
                })
                .collect()
        })
        .collect()
}

/// Test cursor via T-SQL (DECLARE CURSOR / OPEN / FETCH / CLOSE)
/// This validates the expected behavior that our engine should match.
#[tokio::test]
#[ignore]
async fn test_cursor_tsql_basic() {
    let mut client = connect_azure().await;

    // Create test table
    client
        .execute(
            "IF OBJECT_ID('dbo.cursor_test', 'U') IS NOT NULL DROP TABLE dbo.cursor_test",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.cursor_test (id INT PRIMARY KEY, val VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO dbo.cursor_test VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            &[],
        )
        .await
        .unwrap();

    // Test cursor via T-SQL
    let rows = query_azure(
        &mut client,
        r#"
        DECLARE @id INT, @val VARCHAR(50);
        DECLARE cur CURSOR FOR SELECT id, val FROM dbo.cursor_test ORDER BY id;
        OPEN cur;
        FETCH NEXT FROM cur INTO @id, @val;
        SELECT @id as id, @val as val;
        CLOSE cur;
        DEALLOCATE cur;
        "#,
    )
    .await;

    // First row should be (1, 'one')
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "one");

    // Cleanup
    client
        .execute("DROP TABLE dbo.cursor_test", &[])
        .await
        .unwrap();
}

/// Test cursor with WHILE loop
#[tokio::test]
#[ignore]
async fn test_cursor_tsql_while_loop() {
    let mut client = connect_azure().await;

    // Create test table
    client
        .execute(
            "IF OBJECT_ID('dbo.cursor_loop_test', 'U') IS NOT NULL DROP TABLE dbo.cursor_loop_test",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.cursor_loop_test (id INT PRIMARY KEY, val INT)",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO dbo.cursor_loop_test VALUES (1, 10), (2, 20), (3, 30)",
            &[],
        )
        .await
        .unwrap();

    // Test cursor with WHILE loop
    let rows = query_azure(
        &mut client,
        r#"
        DECLARE @id INT, @val INT, @total INT = 0;
        DECLARE cur CURSOR FOR SELECT id, val FROM dbo.cursor_loop_test ORDER BY id;
        OPEN cur;
        FETCH NEXT FROM cur INTO @id, @val;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @total = @total + @val;
            FETCH NEXT FROM cur INTO @id, @val;
        END
        CLOSE cur;
        DEALLOCATE cur;
        SELECT @total as total;
        "#,
    )
    .await;

    // Total should be 10 + 20 + 30 = 60
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "60");

    // Cleanup
    client
        .execute("DROP TABLE dbo.cursor_loop_test", &[])
        .await
        .unwrap();
}

/// Test cursor options (FORWARD_ONLY, SCROLL)
#[tokio::test]
#[ignore]
async fn test_cursor_options() {
    let mut client = connect_azure().await;

    // Create test table
    client
        .execute(
            "IF OBJECT_ID('dbo.cursor_options_test', 'U') IS NOT NULL DROP TABLE dbo.cursor_options_test",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.cursor_options_test (id INT PRIMARY KEY, val VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO dbo.cursor_options_test VALUES (1, 'first'), (2, 'second'), (3, 'third')",
            &[],
        )
        .await
        .unwrap();

    // Test STATIC cursor
    let rows = query_azure(
        &mut client,
        r#"
        DECLARE @id INT, @val VARCHAR(50);
        DECLARE cur CURSOR STATIC FOR SELECT id, val FROM dbo.cursor_options_test ORDER BY id;
        OPEN cur;
        FETCH FIRST FROM cur INTO @id, @val;
        SELECT @id as id, @val as val;
        CLOSE cur;
        DEALLOCATE cur;
        "#,
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "first");

    // Cleanup
    client
        .execute("DROP TABLE dbo.cursor_options_test", &[])
        .await
        .unwrap();
}

/// Test @@FETCH_STATUS behavior
#[tokio::test]
#[ignore]
async fn test_fetch_status() {
    let mut client = connect_azure().await;

    // Create test table
    client
        .execute(
            "IF OBJECT_ID('dbo.fetch_status_test', 'U') IS NOT NULL DROP TABLE dbo.fetch_status_test",
            &[],
        )
        .await
        .unwrap();
    client
        .execute("CREATE TABLE dbo.fetch_status_test (id INT PRIMARY KEY)", &[])
        .await
        .unwrap();
    client
        .execute("INSERT INTO dbo.fetch_status_test VALUES (1), (2)", &[])
        .await
        .unwrap();

    // Test @@FETCH_STATUS
    let rows = query_azure(
        &mut client,
        r#"
        DECLARE @id INT;
        DECLARE cur CURSOR FOR SELECT id FROM fetch_status_test ORDER BY id;
        OPEN cur;
        DECLARE @status1 INT, @status2 INT, @status3 INT;
        FETCH NEXT FROM cur INTO @id;
        SET @status1 = @@FETCH_STATUS;
        FETCH NEXT FROM cur INTO @id;
        SET @status2 = @@FETCH_STATUS;
        FETCH NEXT FROM cur INTO @id;
        SET @status3 = @@FETCH_STATUS;
        CLOSE cur;
        DEALLOCATE cur;
        SELECT @status1 as s1, @status2 as s2, @status3 as s3;
        "#,
    )
    .await;

    // After 2 rows, third fetch should return -1 (no more rows)
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "0"); // First fetch OK
    assert_eq!(rows[0][1], "0"); // Second fetch OK
    assert_eq!(rows[0][2], "-1"); // Third fetch no more rows

    // Cleanup
    client
        .execute("DROP TABLE dbo.fetch_status_test", &[])
        .await
        .unwrap();
}

/// Test cursor DEALLOCATE
#[tokio::test]
#[ignore]
async fn test_cursor_deallocate() {
    let mut client = connect_azure().await;

    // Create test table
    client
        .execute(
            "IF OBJECT_ID('dbo.dealloc_test', 'U') IS NOT NULL DROP TABLE dbo.dealloc_test",
            &[],
        )
        .await
        .unwrap();
    client
        .execute("CREATE TABLE dbo.dealloc_test (id INT PRIMARY KEY)", &[])
        .await
        .unwrap();
    client
        .execute("INSERT INTO dbo.dealloc_test VALUES (1), (2)", &[])
        .await
        .unwrap();

    // Test that DEALLOCATE works
    let rows = query_azure(
        &mut client,
        r#"
        DECLARE cur CURSOR FOR SELECT id FROM dealloc_test;
        OPEN cur;
        DEALLOCATE cur;
        SELECT 1 as done;
        "#,
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");

    // Cleanup
    client
        .execute("DROP TABLE dbo.dealloc_test", &[])
        .await
        .unwrap();
}

/// Test cursor with multiple columns
#[tokio::test]
#[ignore]
async fn test_cursor_multi_columns() {
    let mut client = connect_azure().await;

    // Create test table
    client
        .execute(
            "IF OBJECT_ID('dbo.multi_col_test', 'U') IS NOT NULL DROP TABLE dbo.multi_col_test",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.multi_col_test (id INT PRIMARY KEY, val VARCHAR(50), num INT)",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO dbo.multi_col_test VALUES (1, 'one', 10), (2, 'two', 20)",
            &[],
        )
        .await
        .unwrap();

    // Test cursor with multiple columns
    let rows = query_azure(
        &mut client,
        r#"
        DECLARE @id INT, @val VARCHAR(50), @num INT;
        DECLARE cur CURSOR FOR SELECT id, val, num FROM multi_col_test ORDER BY id;
        OPEN cur;
        FETCH NEXT FROM cur INTO @id, @val, @num;
        SELECT @id as id, @val as val, @num as num;
        CLOSE cur;
        DEALLOCATE cur;
        "#,
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "one");
    assert_eq!(rows[0][2], "10");

    // Cleanup
    client
        .execute("DROP TABLE dbo.multi_col_test", &[])
        .await
        .unwrap();
}

/// Test error handling: cursor not declared
#[tokio::test]
#[ignore]
async fn test_cursor_error_not_declared() {
    let mut client = connect_azure().await;

    // Test opening a cursor that doesn't exist
    let result = client
        .query(
            r#"
        DECLARE @id INT;
        OPEN nonexistent_cursor;
        FETCH NEXT FROM nonexistent_cursor INTO @id;
        CLOSE nonexistent_cursor;
        DEALLOCATE nonexistent_cursor;
        "#,
            &[],
        )
        .await;

    // Should fail with cursor not found error
    assert!(result.is_err(), "Should fail with cursor not found");

    let err = result.unwrap_err();
    let err_str = err.to_string();
    // Azure SQL Edge returns error 16916 or similar for cursor not found
    assert!(
        err_str.contains("cursor") || err_str.contains("Cursor"),
        "Error should mention cursor: {}",
        err_str
    );
}

/// Test CURSOR_SCOPE_GLOBAL vs LOCAL
#[tokio::test]
#[ignore]
async fn test_cursor_scope() {
    let mut client = connect_azure().await;

    // Create test table
    client
        .execute(
            "IF OBJECT_ID('dbo.scope_test', 'U') IS NOT NULL DROP TABLE dbo.scope_test",
            &[],
        )
        .await
        .unwrap();
    client
        .execute("CREATE TABLE dbo.scope_test (id INT PRIMARY KEY)", &[])
        .await
        .unwrap();
    client
        .execute("INSERT INTO dbo.scope_test VALUES (1)", &[])
        .await
        .unwrap();

    // Test LOCAL cursor (default)
    let rows = query_azure(
        &mut client,
        r#"
        DECLARE cur CURSOR LOCAL FOR SELECT id FROM scope_test;
        OPEN cur;
        DEALLOCATE cur;
        SELECT 1 as done;
        "#,
    )
    .await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");

    // Cleanup
    client
        .execute("DROP TABLE dbo.scope_test", &[])
        .await
        .unwrap();
}
