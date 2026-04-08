use tsql_server::playground;
use tsql_server::ServerConfig;
use tsql_server::TdsServer;
use tsql_server_test_support::*;

#[tokio::test]
async fn test_playground_tables() {
    init_test_logger();

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
    let (_, rows) = query_sql(
        &mut client,
        "SELECT TOP 3 Name FROM dbo.Products ORDER BY ProductId",
    )
    .await;
    assert_eq!(rows.len(), 3);

    // Test vCustomerOrders view
    let (cols, rows) = query_sql(
        &mut client,
        "SELECT TOP 2 FirstName, TotalOrders FROM dbo.vCustomerOrders ORDER BY CustomerId",
    )
    .await;
    assert_eq!(cols[0], "FirstName");
    assert_eq!(cols[1], "TotalOrders");
    assert_eq!(rows.len(), 2);
}
