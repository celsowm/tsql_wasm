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
            if let Ok(Some(v)) = row.try_get::<tiberius::numeric::Numeric, _>(i) {
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
    start_server_with_config(config).await
}

async fn start_server_with_config(config: ServerConfig) -> u16 {
    let _ = env_logger::builder().is_test(true).try_init();

    let mut server = TdsServer::new(config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    port
}

async fn connect(port: u16) -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::NotSupported);

    // Retry connection a few times to handle server startup timing
    let mut attempts = 0;
    let max_attempts = 5;

    loop {
        match TcpStream::connect(config.get_addr()).await {
            Ok(tcp) => {
                tcp.set_nodelay(true).unwrap();
                match Client::connect(config.clone(), tcp.compat_write()).await {
                    Ok(client) => return client,
                    Err(e) => {
                        attempts += 1;
                        if attempts >= max_attempts {
                            panic!(
                                "Failed TDS handshake after {} attempts: {}",
                                max_attempts, e
                            );
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    panic!("Failed to connect after {} attempts: {}", max_attempts, e);
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn query_sql(
    client: &mut Client<tokio_util::compat::Compat<TcpStream>>,
    sql: &str,
) -> (Vec<String>, Vec<Vec<String>>) {
    let stream = client
        .query(sql, &[])
        .await
        .expect(&format!("Query failed: {}", sql));
    let rows: Vec<Row> = stream
        .into_first_result()
        .await
        .expect("Failed to read result");

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

async fn exec_sql(client: &mut Client<tokio_util::compat::Compat<TcpStream>>, sql: &str) {
    client
        .execute(sql, &[])
        .await
        .expect(&format!("Execute failed: {}", sql));
}

#[tokio::test]
async fn test_prelogin_and_login() {
    let port = start_server().await;
    eprintln!("Server started on port {}", port);

    let mut client = tokio::time::timeout(std::time::Duration::from_secs(10), connect(port))
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
async fn test_sysdac_probe_returns_int() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let stream = client
        .query(
            "select case when object_id('dbo.sysdac_instances') is not null then 1 else 0 end",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let flag: i32 = rows[0].try_get(0).expect("int conversion failed").expect("missing value");
    assert_eq!(flag, 0);
}

#[tokio::test]
async fn test_cast_null_as_int_metadata() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let stream = client
        .query("SELECT CAST(NULL AS int) AS v", &[])
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let value: Option<i32> = rows[0]
        .try_get(0)
        .expect("int conversion failed for CAST(NULL AS int)");
    assert!(value.is_none());
}

#[tokio::test]
async fn test_object_explorer_server_probe_shape() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let stream = client
        .query(
            "SELECT \
            CAST(serverproperty(N'Servername') AS sysname) AS [Server_Name], \
            'Server[@Name=' + quotename(CAST(serverproperty(N'Servername') AS sysname),'''') + ']' AS [Server_Urn], \
            CAST(null AS int) AS [Server_ServerType], \
            CAST(0x0001 AS int) AS [Server_Status], \
            0 AS [Server_IsContainedAuthentication], \
            (@@microsoftversion / 0x1000000) & 0xff AS [VersionMajor], \
            (@@microsoftversion / 0x10000) & 0xff AS [VersionMinor], \
            @@microsoftversion & 0xffff AS [BuildNumber], \
            CAST(SERVERPROPERTY('IsSingleUser') AS bit) AS [IsSingleUser], \
            CAST(SERVERPROPERTY(N'Edition') AS sysname) AS [Edition], \
            CAST(SERVERPROPERTY('EngineEdition') AS int) AS [EngineEdition], \
            CAST(ISNULL(SERVERPROPERTY(N'IsXTPSupported'), 0) AS bit) AS [IsXTPSupported], \
            SERVERPROPERTY(N'ProductVersion') AS [VersionString], \
            CAST('Windows' AS nvarchar(512)) AS [HostPlatform], \
            CAST(FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') AS bit) AS [IsFullTextInstalled]",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);

    let _server_name: &str = rows[0]
        .try_get(0)
        .expect("Server_Name conversion failed")
        .expect("Server_Name missing");
    let _server_urn: &str = rows[0]
        .try_get(1)
        .expect("Server_Urn conversion failed")
        .expect("Server_Urn missing");
    let _server_type: Option<i32> = rows[0]
        .try_get(2)
        .expect("Server_ServerType conversion failed");
    let _server_status: i32 = rows[0]
        .try_get(3)
        .expect("Server_Status conversion failed")
        .expect("Server_Status missing");
    let _contained: i32 = rows[0]
        .try_get(4)
        .expect("Server_IsContainedAuthentication conversion failed")
        .expect("Server_IsContainedAuthentication missing");
    let _ver_major: i32 = rows[0]
        .try_get(5)
        .expect("VersionMajor conversion failed")
        .expect("VersionMajor missing");
    let _ver_minor: i32 = rows[0]
        .try_get(6)
        .expect("VersionMinor conversion failed")
        .expect("VersionMinor missing");
    let _build_number: i32 = rows[0]
        .try_get(7)
        .expect("BuildNumber conversion failed")
        .expect("BuildNumber missing");
    let is_single_user: Option<bool> = rows[0]
        .try_get(8)
        .expect("IsSingleUser conversion failed");
    let _edition: &str = rows[0]
        .try_get(9)
        .expect("Edition conversion failed")
        .expect("Edition missing");
    let _engine_edition: i32 = rows[0]
        .try_get(10)
        .expect("EngineEdition conversion failed")
        .expect("EngineEdition missing");
    let _is_xtp_supported: Option<bool> = rows[0]
        .try_get(11)
        .expect("IsXTPSupported conversion failed");
    let _version_string: &str = rows[0]
        .try_get(12)
        .expect("VersionString conversion failed")
        .expect("VersionString missing");
    let _host_platform: &str = rows[0]
        .try_get(13)
        .expect("HostPlatform conversion failed")
        .expect("HostPlatform missing");
    let is_fulltext_installed: Option<bool> = rows[0]
        .try_get(14)
        .expect("IsFullTextInstalled conversion failed");
    assert_eq!(is_single_user, Some(false));
    assert_eq!(is_fulltext_installed, Some(false));
}

#[tokio::test]
async fn test_object_explorer_database_list_probe() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let stream = client
        .query(
            "SELECT name, database_id, state_desc FROM sys.databases WHERE name = 'master'",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].try_get(0).expect("name conversion failed").expect("name missing");
    let database_id: i32 = rows[0]
        .try_get(1)
        .expect("database_id conversion failed")
        .expect("database_id missing");
    let state_desc: &str = rows[0]
        .try_get(2)
        .expect("state_desc conversion failed")
        .expect("state_desc missing");
    assert_eq!(name, "master");
    assert_eq!(database_id, 1);
    assert_eq!(state_desc, "ONLINE");
}

#[tokio::test]
async fn test_object_explorer_tables_list_probe() {
    let port = start_server().await;
    let mut client = connect(port).await;

    exec_sql(&mut client, "CREATE TABLE dbo.oe_probe_table (id INT)").await;

    let stream = client
        .query(
            "SELECT t.name, s.name AS schema_name \
             FROM sys.tables t \
             INNER JOIN sys.schemas s ON s.schema_id = t.schema_id \
             WHERE s.name = 'dbo' AND t.name = 'oe_probe_table'",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let table_name: &str = rows[0]
        .try_get(0)
        .expect("table name conversion failed")
        .expect("table name missing");
    let schema_name: &str = rows[0]
        .try_get(1)
        .expect("schema name conversion failed")
        .expect("schema name missing");
    assert_eq!(table_name, "oe_probe_table");
    assert_eq!(schema_name, "dbo");
}

#[tokio::test]
async fn test_object_explorer_configurations_probe() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let stream = client
        .query(
            "select value_in_use from sys.configurations where configuration_id = 16384",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let value_in_use: i32 = rows[0]
        .try_get(0)
        .expect("value_in_use conversion failed")
        .expect("value_in_use missing");
    assert_eq!(value_in_use, 0);
}

#[tokio::test]
async fn test_object_explorer_srvrolemember_probe() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let stream = client
        .query(
            "select is_srvrolemember('sysadmin') * 1 +is_srvrolemember('serveradmin') * 2 +is_srvrolemember('setupadmin') * 4 +is_srvrolemember('securityadmin') * 8 +is_srvrolemember('processadmin') * 16 +is_srvrolemember('dbcreator') * 32 +is_srvrolemember('diskadmin') * 64+ is_srvrolemember('bulkadmin') * 128",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let roles_mask: i32 = rows[0]
        .try_get(0)
        .expect("roles mask conversion failed")
        .expect("roles mask missing");
    assert_eq!(roles_mask, 1);
}

#[tokio::test]
async fn test_object_explorer_database_access_probe() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let stream = client
        .query(
            "SELECT name, database_id FROM sys.databases WHERE HAS_DBACCESS(name) = 1 ORDER BY name",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].try_get(0).expect("name conversion failed").expect("name missing");
    let dbid: i32 = rows[0].try_get(1).expect("id conversion failed").expect("id missing");
    assert_eq!(name, "master");
    assert_eq!(dbid, 1);
}

#[tokio::test]
async fn test_object_explorer_server_permissions_probe() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let stream = client
        .query(
            "SELECT HAS_PERMS_BY_NAME('SERVER', 'SERVER', 'VIEW ANY DATABASE') AS can_view",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream.into_first_result().await.expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let can_view: i32 = rows[0]
        .try_get(0)
        .expect("can_view conversion failed")
        .expect("can_view missing");
    assert_eq!(can_view, 1);
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
    exec_sql(
        &mut client,
        "INSERT INTO test_users VALUES (1, N'Alice', 1)",
    )
    .await;
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

    exec_sql(
        &mut client,
        "INSERT INTO t_customers VALUES (1, N'Alice'), (2, N'Bob')",
    )
    .await;
    exec_sql(
        &mut client,
        "INSERT INTO t_orders VALUES (100, 1, 50), (101, 1, 75), (102, 2, 30)",
    )
    .await;

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
            assert!(
                first_result.is_err(),
                "Expected error from nonexistent table"
            );
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
        pool_min_size: 1,
        pool_max_size: 50,
        pool_idle_timeout_secs: 300,
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
    tds_config.encryption(tiberius::EncryptionLevel::NotSupported);
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
        pool_min_size: 1,
        pool_max_size: 50,
        pool_idle_timeout_secs: 300,
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
    tds_config.encryption(tiberius::EncryptionLevel::NotSupported);
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

    // Test Orders table
    let (_, rows) = query_sql(&mut client, "SELECT COUNT(*) as cnt FROM dbo.Orders").await;
    assert!(rows[0][0].parse::<i32>().unwrap() > 0);

    // Test vCustomerOrders view
    let (cols, rows) = query_sql(
        &mut client,
        "SELECT TOP 2 FirstName, TotalOrders FROM dbo.vCustomerOrders ORDER BY CustomerId",
    )
    .await;
    assert_eq!(cols[0], "FirstName");
    assert_eq!(cols[1], "TotalOrders");
    assert_eq!(rows.len(), 2);

    // Test vProductSales view
    let (_, rows) = query_sql(
        &mut client,
        "SELECT ProductName, TotalSold FROM dbo.vProductSales ORDER BY TotalSold DESC",
    )
    .await;
    assert!(rows.len() > 0);

    // Test vEmployeeHierarchy view
    let (_, rows) = query_sql(
        &mut client,
        "SELECT FirstName, Department FROM dbo.vEmployeeHierarchy",
    )
    .await;
    assert!(rows.len() > 0);

    // Test vMonthlySales view
    let (_, rows) = query_sql(
        &mut client,
        "SELECT SaleYear, SaleMonth, TotalRevenue FROM dbo.vMonthlySales",
    )
    .await;
    assert!(rows.len() > 0);
}
#[tokio::test]
async fn test_decimal_select() {
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
        pool_min_size: 1,
        pool_max_size: 50,
        pool_idle_timeout_secs: 300,
    };
    let db = tsql_core::Database::new();
    let mut server = TdsServer::new_with_database(db, config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client = connect(port).await;
    let (_, rows) = query_sql(&mut client, "SELECT CAST(123.45 AS DECIMAL(10,2)) as val").await;
    assert_eq!(rows[0][0], "123.45");

    let (_, rows) = query_sql(&mut client, "SELECT CAST(123.45 AS DECIMAL(18,2)) as val").await;
    assert_eq!(rows[0][0], "123.45");
}

#[tokio::test]
async fn test_session_pool_reuse_resets_session_state() {
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
        pool_max_size: 1,
        pool_idle_timeout_secs: 300,
    };

    let port = start_server_with_config(config).await;

    let mut client1 = connect(port).await;
    exec_sql(
        &mut client1,
        "CREATE TABLE pool_reset_t (id INT PRIMARY KEY)",
    )
    .await;
    exec_sql(&mut client1, "INSERT INTO pool_reset_t VALUES (1)").await;
    exec_sql(&mut client1, "CREATE TABLE #pool_tmp (id INT)").await;
    exec_sql(&mut client1, "BEGIN TRANSACTION").await;
    exec_sql(&mut client1, "UPDATE pool_reset_t SET id = 1 WHERE id = 1").await;
    drop(client1);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client2 = connect(port).await;
    exec_sql(&mut client2, "CREATE TABLE #pool_tmp (id INT)").await;
    exec_sql(&mut client2, "UPDATE pool_reset_t SET id = 1 WHERE id = 1").await;
}

#[tokio::test]
async fn test_session_pool_max_size_enforced() {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: None,
        database: "master".to_string(),
        packet_size: 4096,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
        pool_min_size: 0,
        pool_max_size: 1,
        pool_idle_timeout_secs: 300,
    };

    let port = start_server_with_config(config).await;
    let _client1 = connect(port).await;

    let mut tds_config = Config::new();
    tds_config.host("127.0.0.1");
    tds_config.port(port);
    tds_config.trust_cert();
    tds_config.encryption(tiberius::EncryptionLevel::NotSupported);

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let tcp = TcpStream::connect(tds_config.get_addr()).await.unwrap();
        tcp.set_nodelay(true).unwrap();
        Client::connect(tds_config, tcp.compat_write()).await
    })
    .await
    .expect("second connection attempt timed out");

    assert!(result.is_err());
}

#[tokio::test]
async fn test_session_pool_idle_timeout_reap_still_allows_checkout() {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: None,
        database: "master".to_string(),
        packet_size: 4096,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
        pool_min_size: 0,
        pool_max_size: 1,
        pool_idle_timeout_secs: 1,
    };

    let port = start_server_with_config(config).await;

    let mut client1 = connect(port).await;
    let (_, rows1) = query_sql(&mut client1, "SELECT 1 as n").await;
    assert_eq!(rows1[0][0], "1");
    drop(client1);

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut client2 = connect(port).await;
    let (_, rows2) = query_sql(&mut client2, "SELECT 2 as n").await;
    assert_eq!(rows2[0][0], "2");
}
