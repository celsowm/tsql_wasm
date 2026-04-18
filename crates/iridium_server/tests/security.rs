use iridium_core::Database;
use iridium_server::{Credentials, ServerConfig, TdsServer};
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

mod common;
use common::*;

#[tokio::test]
async fn test_auth_reject() {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        auth: Some(Credentials {
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
        data_dir: None,
    };

    let mut server = TdsServer::new_with_database(Database::new(), config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

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
        auth: Some(Credentials {
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
        data_dir: None,
    };

    let mut server = TdsServer::new_with_database(Database::new(), config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

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
