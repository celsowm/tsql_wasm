//! Quick smoke test for cursor operations.
//!
//! Run with: cargo test -p iridium_server --test cursor_quick_test -- --ignored

use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

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

/// Quick test: verify cursor works end-to-end
#[tokio::test]
#[ignore]
async fn quick_cursor_smoke_test() {
    let mut client = connect_azure().await;

    // Clean up any existing test table
    client
        .execute(
            "IF OBJECT_ID('dbo.quick_cursor_test', 'U') IS NOT NULL DROP TABLE dbo.quick_cursor_test",
            &[],
        )
        .await
        .unwrap();

    // Create test data
    client
        .execute(
            "CREATE TABLE dbo.quick_cursor_test (id INT, name VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    client
        .execute(
            "INSERT INTO dbo.quick_cursor_test VALUES (1, 'Alice'), (2, 'Bob')",
            &[],
        )
        .await
        .unwrap();

    // Test basic cursor operations
    let stream = client
        .query(
            r#"
            DECLARE @id INT, @name VARCHAR(50);
            DECLARE cur CURSOR FOR SELECT id, name FROM quick_cursor_test ORDER BY id;
            OPEN cur;
            FETCH NEXT FROM cur INTO @id, @name;
            SELECT @id as id, @name as name;
            CLOSE cur;
            DEALLOCATE cur;
            "#,
            &[],
        )
        .await
        .expect("Cursor query should succeed");

    let rows = stream
        .into_first_result()
        .await
        .expect("Should read cursor result");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0].try_get::<i32, _>(0).unwrap(), Some(1));
    assert_eq!(rows[0].try_get::<&str, _>(1).unwrap(), Some("Alice"));

    // Clean up
    client
        .execute("DROP TABLE dbo.quick_cursor_test", &[])
        .await
        .unwrap();

    println!("✓ Cursor smoke test passed");
}

/// Quick test: verify @@FETCH_STATUS behavior
#[tokio::test]
#[ignore]
async fn quick_fetch_status_test() {
    let mut client = connect_azure().await;

    // Clean up
    client
        .execute(
            "IF OBJECT_ID('dbo.quick_fetch', 'U') IS NOT NULL DROP TABLE dbo.quick_fetch",
            &[],
        )
        .await
        .unwrap();

    // Create test data
    client
        .execute("CREATE TABLE dbo.quick_fetch (id INT)", &[])
        .await
        .unwrap();
    client
        .execute("INSERT INTO dbo.quick_fetch VALUES (1)", &[])
        .await
        .unwrap();

    // Test @@FETCH_STATUS
    let stream = client
        .query(
            r#"
            DECLARE @id INT, @status INT;
            DECLARE cur CURSOR FOR SELECT id FROM quick_fetch;
            OPEN cur;
            FETCH NEXT FROM cur INTO @id;
            SET @status = @@FETCH_STATUS;
            CLOSE cur;
            DEALLOCATE cur;
            SELECT @status as status;
            "#,
            &[],
        )
        .await
        .expect("Fetch status query should succeed");

    let rows = stream
        .into_first_result()
        .await
        .expect("Should read result");

    assert_eq!(rows.len(), 1);
    let status: i32 = rows[0].try_get(0).unwrap().unwrap();
    assert_eq!(status, 0, "FETCH_STATUS should be 0 after successful fetch");

    // Clean up
    client
        .execute("DROP TABLE dbo.quick_fetch", &[])
        .await
        .unwrap();

    println!("✓ Fetch status test passed");
}

