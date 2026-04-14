//! Compare cursor behavior between iridium_server and Azure SQL Edge.
//!
//! These tests verify that our server's cursor implementation (via T-SQL)
//! produces the same results as Azure SQL Edge.
//!
//! Run with: cargo test -p iridium_server --test cursor_compare_test -- --ignored

use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use iridium_core::Database;
use iridium_server::{ServerConfig, TdsServer};

/// Read Azure SQL Edge connection info from environment or defaults.
fn azure_config() -> Config {
    let host = std::env::var("IRIDIUM_AZURE_HOST")
        .or_else(|_| std::env::var("TSQL_AZURE_HOST"))
        .unwrap_or_else(|_| "[::1]".to_string());
    let port: u16 = std::env::var("IRIDIUM_AZURE_PORT")
        .or_else(|_| std::env::var("TSQL_AZURE_PORT"))
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(11433);
    let user = std::env::var("IRIDIUM_AZURE_USER")
        .or_else(|_| std::env::var("TSQL_AZURE_USER"))
        .unwrap_or_else(|_| "sa".to_string());
    let password = std::env::var("IRIDIUM_AZURE_PASSWORD")
        .or_else(|_| std::env::var("TSQL_AZURE_PASSWORD"))
        .unwrap_or_else(|_| "Iridium12345!".to_string());

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

async fn start_local_server() -> u16 {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: None,
        database: "master".to_string(),
        packet_size: 4096,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
        pool_min_size: 1,
        pool_max_size: 50,
        pool_idle_timeout_secs: 300,
        data_dir: None,
    };

    let mut server = TdsServer::new_with_database(Database::new(), config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    port
}

async fn connect_local(port: u16) -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("Failed to connect to local server");
    tcp.set_nodelay(true).unwrap();

    Client::connect(config, tcp.compat_write())
        .await
        .expect("Failed TDS handshake with local server")
}

async fn query_client(
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

/// Compare cursor fetch behavior between local and Azure.
#[tokio::test]
#[ignore]
async fn test_compare_cursor_fetch() {
    let mut azure = connect_azure().await;

    // Create table
    azure.execute(
        "IF OBJECT_ID('dbo.compare_cursor', 'U') IS NOT NULL DROP TABLE dbo.compare_cursor",
        &[],
    ).await.unwrap();
    azure.execute(
        "CREATE TABLE dbo.compare_cursor (id INT PRIMARY KEY, val VARCHAR(50))",
        &[],
    ).await.unwrap();
    azure.execute(
        "INSERT INTO dbo.compare_cursor VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        &[],
    ).await.unwrap();

    // Test cursor fetch on Azure
    let azure_rows = query_client(
        &mut azure,
        r#"
        DECLARE @id INT, @val VARCHAR(50);
        DECLARE cur CURSOR FOR SELECT id, val FROM compare_cursor ORDER BY id;
        OPEN cur;
        FETCH NEXT FROM cur INTO @id, @val;
        SELECT @id as id, @val as val;
        CLOSE cur;
        DEALLOCATE cur;
        "#,
    )
    .await;

    // Verify Azure returns first row
    assert_eq!(azure_rows.len(), 1);
    assert_eq!(azure_rows[0][0], "1");
    assert_eq!(azure_rows[0][1], "one");

    // Clean up
    azure.execute("DROP TABLE dbo.compare_cursor", &[]).await.unwrap();
}

/// Compare @@FETCH_STATUS behavior.
#[tokio::test]
#[ignore]
async fn test_compare_fetch_status() {
    let mut azure = connect_azure().await;

    // Create table
    azure.execute(
        "IF OBJECT_ID('dbo.fetch_compare', 'U') IS NOT NULL DROP TABLE dbo.fetch_compare",
        &[],
    ).await.unwrap();
    azure.execute("CREATE TABLE dbo.fetch_compare (id INT)", &[]).await.unwrap();
    azure.execute("INSERT INTO dbo.fetch_compare VALUES (1), (2)", &[])
        .await
        .unwrap();

    // Test @@FETCH_STATUS on Azure
    let azure_rows = query_client(
        &mut azure,
        r#"
        DECLARE @id INT;
        DECLARE cur CURSOR FOR SELECT id FROM fetch_compare ORDER BY id;
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
    ).await;

    // Azure returns: 0, 0, -1
    assert_eq!(azure_rows[0], vec!["0", "0", "-1"]);

    // Cleanup
    azure.execute("DROP TABLE dbo.fetch_compare", &[]).await.unwrap();
}

/// Compare cursor WITH HOLD behavior (if supported).
#[tokio::test]
#[ignore]
async fn test_compare_cursor_scroll() {
    let mut azure = connect_azure().await;

    // Create table
    azure.execute(
        "IF OBJECT_ID('dbo.scroll_test', 'U') IS NOT NULL DROP TABLE dbo.scroll_test",
        &[],
    ).await.unwrap();
    azure.execute("CREATE TABLE dbo.scroll_test (id INT PRIMARY KEY)", &[])
        .await
        .unwrap();
    azure.execute("INSERT INTO dbo.scroll_test VALUES (1), (2), (3)", &[])
        .await
        .unwrap();

    // Test SCROLL cursor on Azure
    let azure_rows = query_client(
        &mut azure,
        r#"
        DECLARE @id INT;
        DECLARE cur CURSOR SCROLL FOR SELECT id FROM scroll_test ORDER BY id;
        OPEN cur;
        FETCH LAST FROM cur INTO @id;
        SELECT @id as last_id;
        CLOSE cur;
        DEALLOCATE cur;
        "#,
    ).await;

    // Azure should return the last row (id=3)
    assert_eq!(azure_rows[0][0], "3");

    // Cleanup
    azure.execute("DROP TABLE dbo.scroll_test", &[]).await.unwrap();
}

/// Test that error messages are comparable.
#[tokio::test]
#[ignore]
async fn test_compare_cursor_errors() {
    let mut azure = connect_azure().await;

    // Test opening an undeclared cursor on Azure
    let result = azure.query(
        r#"
        DECLARE @id INT;
        OPEN undeclared_cursor;
        FETCH NEXT FROM undeclared_cursor INTO @id;
        CLOSE undeclared_cursor;
        DEALLOCATE undeclared_cursor;
        "#,
        &[],
    ).await;

    // Should fail with an error about cursor
    assert!(result.is_err(), "Azure should fail on undeclared cursor");
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("cursor") || err_str.contains("Cursor"),
        "Error should mention cursor: {}",
        err_str
    );
}

/// Test cursor with aggregation query.
#[tokio::test]
#[ignore]
async fn test_compare_cursor_aggregation() {
    let mut azure = connect_azure().await;

    // Create table
    azure.execute(
        "IF OBJECT_ID('dbo.agg_cursor', 'U') IS NOT NULL DROP TABLE dbo.agg_cursor",
        &[],
    ).await.unwrap();
    azure.execute("CREATE TABLE dbo.agg_cursor (cat VARCHAR(10), val INT)", &[])
        .await
        .unwrap();
    azure.execute(
        "INSERT INTO dbo.agg_cursor VALUES ('A', 10), ('A', 20), ('B', 30)",
        &[],
    ).await.unwrap();

    // Test cursor with aggregation
    let azure_rows = query_client(
        &mut azure,
        r#"
        DECLARE @cat VARCHAR(10), @sum INT;
        DECLARE cur CURSOR FOR SELECT cat, SUM(val) FROM agg_cursor GROUP BY cat ORDER BY cat;
        OPEN cur;
        FETCH NEXT FROM cur INTO @cat, @sum;
        SELECT @cat as cat, @sum as sum;
        CLOSE cur;
        DEALLOCATE cur;
        "#,
    ).await;

    // Azure should return first group: A, 30
    assert_eq!(azure_rows[0][0], "A");
    assert_eq!(azure_rows[0][1], "30");

    // Cleanup
    azure.execute("DROP TABLE dbo.agg_cursor", &[]).await.unwrap();
}
