use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use tsql_server::{playground, ServerConfig, TdsServer};

fn row_to_strings(row: &Row) -> Vec<String> {
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
            if let Ok(Some(v)) = row.try_get::<i16, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<u8, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<f64, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<bool, _>(i) {
                return if v { "1".to_string() } else { "0".to_string() };
            }
            "NULL".to_string()
        })
        .collect()
}

async fn start_server() -> u16 {
    let _ = env_logger::builder().is_test(true).try_init();

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: None,
        database: "master".to_string(),
        packet_size: 4096,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
    };

    let mut server = TdsServer::new(config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    port
}

async fn connect(port: u16) -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::Off);

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("Failed to connect");
    tcp.set_nodelay(true).unwrap();

    Client::connect(config, tcp.compat_write())
        .await
        .expect("Failed TDS handshake")
}

async fn query_sql(
    client: &mut Client<tokio_util::compat::Compat<TcpStream>>,
    sql: &str,
) -> (Vec<String>, Vec<Vec<String>>) {
    let stream = client.query(sql, &[]).await.expect(&format!("Query failed: {}", sql));
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");

    let columns = if let Some(first) = rows.first() {
        let ncols: usize = first.len();
        (0..ncols)
            .map(|i| first.columns()[i].name().to_string())
            .collect()
    } else {
        vec![]
    };

    let data: Vec<Vec<String>> = rows.iter().map(row_to_strings).collect();
    (columns, data)
}

async fn exec_sql(
    client: &mut Client<tokio_util::compat::Compat<TcpStream>>,
    sql: &str,
) {
    client
        .execute(sql, &[])
        .await
        .expect(&format!("Execute failed: {}", sql));
}

#[tokio::test]
async fn test_prelogin_and_login() {
    let port = start_server().await;
    eprintln!("Server started on port {}", port);

    let mut client = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        connect(port),
    )
    .await
    .expect("Connection timed out");

    eprintln!("Connected, running query...");
    let (cols, rows) = query_sql(&mut client, "SELECT 1 as n").await;
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], "n");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");
}

#[tokio::test]
async fn test_select_string() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let (cols, rows) = query_sql(&mut client, "SELECT 'hello' as greeting").await;
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], "greeting");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "hello");
}

#[tokio::test]
async fn test_select_multiple_columns() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let (cols, rows) = query_sql(&mut client, "SELECT 42 as num, 'test' as str").await;
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], "num");
    assert_eq!(cols[1], "str");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "42");
    assert_eq!(rows[0][1], "test");
}

#[tokio::test]
async fn test_create_table_and_insert() {
    let port = start_server().await;
    let mut client = connect(port).await;

    // Create table
    exec_sql(
        &mut client,
        "CREATE TABLE test_users (id INT, name NVARCHAR(50), active BIT)",
    )
    .await;

    // Insert rows
    exec_sql(&mut client, "INSERT INTO test_users VALUES (1, N'Alice', 1)").await;
    exec_sql(&mut client, "INSERT INTO test_users VALUES (2, N'Bob', 0)").await;

    // Query back
    let (cols, rows) = query_sql(&mut client, "SELECT id, name FROM test_users ORDER BY id").await;
    assert_eq!(cols.len(), 2);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[1][1], "Bob");
}

#[tokio::test]
async fn test_join() {
    let port = start_server().await;
    let mut client = connect(port).await;

    exec_sql(
        &mut client,
        "CREATE TABLE t_orders (order_id INT, customer_id INT, amount INT)",
    )
    .await;
    exec_sql(
        &mut client,
        "CREATE TABLE t_customers (customer_id INT, name NVARCHAR(50))",
    )
    .await;

    exec_sql(&mut client, "INSERT INTO t_customers VALUES (1, N'Alice'), (2, N'Bob')").await;
    exec_sql(&mut client, "INSERT INTO t_orders VALUES (100, 1, 50), (101, 1, 75), (102, 2, 30)").await;

    let (_, rows) = query_sql(
        &mut client,
        "SELECT c.name, SUM(o.amount) as total \
         FROM t_customers c \
         INNER JOIN t_orders o ON c.customer_id = o.customer_id \
         GROUP BY c.name \
         ORDER BY total DESC",
    )
    .await;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "Alice");
    assert_eq!(rows[0][1], "125");
    assert_eq!(rows[1][0], "Bob");
    assert_eq!(rows[1][1], "30");
}

#[tokio::test]
async fn test_error_handling() {
    let port = start_server().await;
    let mut client = connect(port).await;

    // Invalid SQL should produce an error
    let result = client.query("SELECT * FROM nonexistent_table", &[]).await;
    match result {
        Ok(stream) => {
            let first_result = stream.into_first_result().await;
            assert!(first_result.is_err(), "Expected error from nonexistent table");
        }
        Err(_) => {
            // Also acceptable - error at query level
        }
    }
}

#[tokio::test]
async fn test_identity() {
    let port = start_server().await;
    let mut client = connect(port).await;

    exec_sql(
        &mut client,
        "CREATE TABLE t_id (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(20))",
    )
    .await;

    exec_sql(&mut client, "INSERT INTO t_id (val) VALUES (N'first')").await;
    exec_sql(&mut client, "INSERT INTO t_id (val) VALUES (N'second')").await;

    let (_, rows) = query_sql(&mut client, "SELECT id, val FROM t_id ORDER BY id").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "first");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[1][1], "second");
}

#[tokio::test]
async fn test_auth_reject() {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: Some(tsql_server::Credentials {
            user: "admin".to_string(),
            password: "secret".to_string(),
        }),
        database: "master".to_string(),
        packet_size: 4096,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
    };

    let mut server = TdsServer::new(config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Try to connect with wrong credentials
    let mut tds_config = Config::new();
    tds_config.host("127.0.0.1");
    tds_config.port(port);
    tds_config.trust_cert();
    tds_config.encryption(tiberius::EncryptionLevel::Off);
    tds_config.authentication(tiberius::AuthMethod::sql_server("wrong", "creds"));

    let tcp = TcpStream::connect(tds_config.get_addr())
        .await
        .expect("Failed to connect");
    tcp.set_nodelay(true).unwrap();

    let result = Client::connect(tds_config, tcp.compat_write()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_auth_accept() {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: Some(tsql_server::Credentials {
            user: "admin".to_string(),
            password: "secret".to_string(),
        }),
        database: "master".to_string(),
        packet_size: 4096,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
    };

    let mut server = TdsServer::new(config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect with correct credentials
    let mut tds_config = Config::new();
    tds_config.host("127.0.0.1");
    tds_config.port(port);
    tds_config.trust_cert();
    tds_config.encryption(tiberius::EncryptionLevel::Off);
    tds_config.authentication(tiberius::AuthMethod::sql_server("admin", "secret"));

    let tcp = TcpStream::connect(tds_config.get_addr())
        .await
        .expect("Failed to connect");
    tcp.set_nodelay(true).unwrap();

    let mut client = Client::connect(tds_config, tcp.compat_write())
        .await
        .expect("Should connect with correct creds");

    let (_, rows) = query_sql(&mut client, "SELECT 99 as val").await;
    assert_eq!(rows[0][0], "99");
}

#[tokio::test]
async fn test_playground_tables() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Start server with playground
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: None,
        database: "master".to_string(),
        packet_size: 4096,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
    };

    let db = tsql_core::Database::new();
    playground::seed_playground(&db).expect("Failed to seed playground");

    let mut server = TdsServer::new_with_database(db, config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client = connect(port).await;

    // Test Customers table
    let (cols, rows) = query_sql(&mut client, "SELECT COUNT(*) as cnt FROM dbo.Customers").await;
    assert_eq!(cols[0], "cnt");
    assert!(rows[0][0].parse::<i32>().unwrap() > 0);

    // Test Products table
    let (_, rows) = query_sql(&mut client, "SELECT TOP 3 Name FROM dbo.Products ORDER BY ProductId").await;
    assert_eq!(rows.len(), 3);

    // Test Orders table
    let (_, rows) = query_sql(&mut client, "SELECT COUNT(*) as cnt FROM dbo.Orders").await;
    assert!(rows[0][0].parse::<i32>().unwrap() > 0);

    // Test vCustomerOrders view
    let (cols, rows) = query_sql(&mut client, "SELECT TOP 2 FirstName, TotalOrders FROM dbo.vCustomerOrders ORDER BY CustomerId").await;
    assert_eq!(cols[0], "FirstName");
    assert_eq!(cols[1], "TotalOrders");
    assert_eq!(rows.len(), 2);

    // Test vProductSales view
    let (_, rows) = query_sql(&mut client, "SELECT ProductName, TotalSold FROM dbo.vProductSales ORDER BY TotalSold DESC").await;
    assert!(rows.len() > 0);

    // Test vEmployeeHierarchy view
    let (_, rows) = query_sql(&mut client, "SELECT FirstName, Department FROM dbo.vEmployeeHierarchy").await;
    assert!(rows.len() > 0);

    // Test vMonthlySales view
    let (_, rows) = query_sql(&mut client, "SELECT SaleYear, SaleMonth, TotalRevenue FROM dbo.vMonthlySales").await;
    assert!(rows.len() > 0);
}
