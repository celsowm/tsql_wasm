use tiberius::Row;
use iridium_server::ServerConfig;
use iridium_server_test_support::*;

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
    let rows: Vec<Row> = stream
        .into_first_result()
        .await
        .expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let flag: i32 = rows[0]
        .try_get(0)
        .expect("int conversion failed")
        .expect("missing value");
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
    let rows: Vec<Row> = stream
        .into_first_result()
        .await
        .expect("Failed to read result");
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
    let rows: Vec<Row> = stream
        .into_first_result()
        .await
        .expect("Failed to read result");
    assert_eq!(rows.len(), 1);

    let is_single_user: Option<bool> = rows[0].try_get(8).unwrap();
    let is_fulltext_installed: Option<bool> = rows[0].try_get(14).unwrap();
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
    let rows: Vec<Row> = stream
        .into_first_result()
        .await
        .expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].try_get(0).unwrap().unwrap();
    assert_eq!(name, "master");
}

#[tokio::test]
async fn test_database_context_tracks_login_database() {
    let port = start_server_with_config(ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: None,
        database: "msdb".to_string(),
        packet_size: 4096,
        tls_enabled: false,
        tls_cert_path: None,
        tls_key_path: None,
        pool_min_size: 1,
        pool_max_size: 50,
        pool_idle_timeout_secs: 300,
    })
    .await;
    let mut client = connect(port).await;

    let stream = client
        .query(
            "SELECT DB_NAME() AS current_db, ORIGINAL_DB_NAME() AS original_db",
            &[],
        )
        .await
        .expect("Query failed");
    let rows: Vec<Row> = stream
        .into_first_result()
        .await
        .expect("Failed to read result");
    assert_eq!(rows.len(), 1);

    let current_db: &str = rows[0].try_get(0).unwrap().unwrap();
    let original_db: &str = rows[0].try_get(1).unwrap().unwrap();
    assert_eq!(current_db, "msdb");
    assert_eq!(original_db, "msdb");
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
    let rows: Vec<Row> = stream
        .into_first_result()
        .await
        .expect("Failed to read result");
    assert_eq!(rows.len(), 1);
    let roles_mask: i32 = rows[0].try_get(0).unwrap().unwrap();
    assert_eq!(roles_mask, 1);
}

