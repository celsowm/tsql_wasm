use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use iridium_core::Database;
use iridium_server::{ServerConfig, TdsServer};

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
        pool_min_size: 1,
        pool_max_size: 50,
        pool_idle_timeout_secs: 300,
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

async fn connect(port: u16) -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(config.get_addr()).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    Client::connect(config, tcp.compat_write()).await.unwrap()
}

#[tokio::test]
async fn test_sys_tables_query() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.TestSpTables', 'U') IS NOT NULL DROP TABLE dbo.TestSpTables",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.TestSpTables (Id INT, Name NVARCHAR(100))",
            &[],
        )
        .await
        .unwrap();

    let stream = client
        .query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TestSpTables'", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert!(!rows.is_empty(), "Should find TestSpTables in INFORMATION_SCHEMA");
    let row = row_to_strings(&rows[0]);
    println!("TABLE_NAME: {}", row[0]);
    assert_eq!(row[0], "TestSpTables");

    client
        .execute("DROP TABLE dbo.TestSpTables", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sys_columns_query() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.TestSpCols', 'U') IS NOT NULL DROP TABLE dbo.TestSpCols",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.TestSpCols (Id INT, Name NVARCHAR(50), Price DECIMAL(10,2))",
            &[],
        )
        .await
        .unwrap();

    let stream = client
        .query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TestSpCols' ORDER BY ORDINAL_POSITION", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert_eq!(rows.len(), 3);

    let row0 = row_to_strings(&rows[0]);
    assert_eq!(row0[0], "Id");

    let row1 = row_to_strings(&rows[1]);
    assert_eq!(row1[0], "Name");

    let row2 = row_to_strings(&rows[2]);
    assert_eq!(row2[0], "Price");

    client
        .execute("DROP TABLE dbo.TestSpCols", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sys_key_column_usage() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.TestPkeys', 'U') IS NOT NULL DROP TABLE dbo.TestPkeys",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.TestPkeys (Id INT PRIMARY KEY, Name NVARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    let stream = client
        .query("SELECT COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = 'TestPkeys' ORDER BY ORDINAL_POSITION", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert!(!rows.is_empty(), "Should find primary key column");
    let row = row_to_strings(&rows[0]);
    assert_eq!(row[0], "Id");
    assert_eq!(row[1], "1");

    client
        .execute("DROP TABLE dbo.TestPkeys", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sys_parameters_query() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.TestProc', 'P') IS NOT NULL DROP PROCEDURE dbo.TestProc",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE PROCEDURE dbo.TestProc @Id INT, @Name NVARCHAR(100) AS SELECT @Id, @Name",
            &[],
        )
        .await
        .unwrap();

    let stream = client
        .query("SELECT PARAMETER_NAME FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_NAME = 'TestProc' ORDER BY ORDINAL_POSITION", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert!(!rows.is_empty(), "Should find procedure parameters");
    assert_eq!(rows.len(), 2);

    let row0 = row_to_strings(&rows[0]);
    assert!(row0[0].starts_with("@Id"));

    let row1 = row_to_strings(&rows[1]);
    assert!(row1[0].starts_with("@Name"));

    client
        .execute("DROP PROCEDURE dbo.TestProc", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sp_tables_via_query() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.TablesQueryTest', 'U') IS NOT NULL DROP TABLE dbo.TablesQueryTest",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.TablesQueryTest (Id INT)",
            &[],
        )
        .await
        .unwrap();

    let stream = client
        .query("SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TablesQueryTest' AND TABLE_SCHEMA = 'dbo'", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert!(!rows.is_empty());
    let row = row_to_strings(&rows[0]);
    assert_eq!(row[1], "TablesQueryTest");
    assert_eq!(row[2], "BASE TABLE");

    client
        .execute("DROP TABLE dbo.TablesQueryTest", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sp_columns_via_query() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.ColumnsQueryTest', 'U') IS NOT NULL DROP TABLE dbo.ColumnsQueryTest",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.ColumnsQueryTest (Id INT, Name VARCHAR(50))",
            &[],
        )
        .await
        .unwrap();

    let stream = client
        .query("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ColumnsQueryTest' ORDER BY ORDINAL_POSITION", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert_eq!(rows.len(), 2);

    let row0 = row_to_strings(&rows[0]);
    assert_eq!(row0[2], "Id");

    let row1 = row_to_strings(&rows[1]);
    assert_eq!(row1[2], "Name");

    client
        .execute("DROP TABLE dbo.ColumnsQueryTest", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sp_pkeys_via_query() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.PkeysQueryTest', 'U') IS NOT NULL DROP TABLE dbo.PkeysQueryTest",
            &[],
        )
        .await
        .unwrap();
    client
        .execute(
            "CREATE TABLE dbo.PkeysQueryTest (Id1 INT, Id2 INT, Name NVARCHAR(50), PRIMARY KEY (Id1, Id2))",
            &[],
        )
        .await
        .unwrap();

    let stream = client
        .query("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = 'PkeysQueryTest' AND CONSTRAINT_NAME LIKE 'PK%' ORDER BY ORDINAL_POSITION", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert_eq!(rows.len(), 2);

    let row0 = row_to_strings(&rows[0]);
    assert_eq!(row0[2], "Id1");
    assert_eq!(row0[3], "1");

    let row1 = row_to_strings(&rows[1]);
    assert_eq!(row1[2], "Id2");
    assert_eq!(row1[3], "2");

    client
        .execute("DROP TABLE dbo.PkeysQueryTest", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_table_without_pk() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.NoPkTest', 'U') IS NOT NULL DROP TABLE dbo.NoPkTest",
            &[],
        )
        .await
        .unwrap();
    client
        .execute("CREATE TABLE dbo.NoPkTest (Id INT, Name NVARCHAR(50))", &[])
        .await
        .unwrap();

    let stream = client
        .query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = 'NoPkTest'", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert!(rows.is_empty(), "Table without PK should return no key columns");

    client
        .execute("DROP TABLE dbo.NoPkTest", &[])
        .await
        .unwrap();
}

#[tokio::test]
async fn test_schema_filter() {
    let port = start_server().await;
    let mut client = connect(port).await;

    client
        .execute(
            "IF OBJECT_ID('dbo.SchemaTest', 'U') IS NOT NULL DROP TABLE dbo.SchemaTest",
            &[],
        )
        .await
        .unwrap();
    client
        .execute("CREATE TABLE dbo.SchemaTest (Id INT)", &[])
        .await
        .unwrap();

    let stream = client
        .query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'SchemaTest'", &[])
        .await
        .unwrap();
    let rows: Vec<Row> = stream.into_first_result().await.unwrap();

    assert!(!rows.is_empty());
    let row = row_to_strings(&rows[0]);
    assert_eq!(row[0], "SchemaTest");

    client
        .execute("DROP TABLE dbo.SchemaTest", &[])
        .await
        .unwrap();
}
