#![allow(dead_code)]

use iridium_core::Database;
use iridium_server::{ServerConfig, TdsServer};
use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

pub fn init_test_logger() {
    let mut logger = env_logger::builder();
    logger.is_test(true);
    logger.filter_module("tiberius", log::LevelFilter::Error);
    let _ = logger.try_init();
}

pub fn row_to_strings(row: &Row) -> Vec<String> {
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

pub async fn start_server() -> u16 {
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
    start_server_with_config(config).await
}

pub async fn start_server_with_config(config: ServerConfig) -> u16 {
    let _ = env_logger::builder().is_test(true).try_init();

    let mut server = TdsServer::new_with_database(Database::new(), config);
    let addr = server.bind().await.unwrap();
    let port = addr.port();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    port
}

pub async fn connect(port: u16) -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::NotSupported);

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

pub async fn query_sql(
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

pub async fn exec_sql(client: &mut Client<tokio_util::compat::Compat<TcpStream>>, sql: &str) {
    client
        .execute(sql, &[])
        .await
        .expect(&format!("Execute failed: {}", sql));
}
