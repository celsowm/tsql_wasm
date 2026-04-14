use tiberius::Client;
use tiberius::Config;
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use iridium_server::ServerConfig;
use iridium_server_test_support::*;

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
    // Should be able to create #pool_tmp again if session was reset
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

