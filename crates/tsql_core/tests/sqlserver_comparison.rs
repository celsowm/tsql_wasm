use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tsql_core::{parse_sql, types::Value, Engine};

/// Helper to convert engine Value to a string representation that matches SQL Server's TDS output
fn engine_val_to_string(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bit(v) => (if *v { "1" } else { "0" }).to_string(),
        Value::Money(v) => {
            // SQL Server TDS doesn't include the $ prefix
            tsql_core::types::format_decimal(*v, 4)
        }
        Value::SmallMoney(v) => tsql_core::types::format_decimal(*v as i128, 4),
        _ => val.to_string_value(),
    }
}

/// Helper to convert Tiberius Row to a vector of strings
fn tiberius_row_to_strings(row: &Row) -> Vec<String> {
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
            if let Ok(Some(v)) = row.try_get::<f64, _>(i) {
                // Formatting float to match engine's format_float
                return tsql_core::types::format_float(v);
            }
            if let Ok(Some(v)) = row.try_get::<bool, _>(i) {
                return if v { "1".to_string() } else { "0".to_string() };
            }
            if let Ok(Some(v)) = row.try_get::<tiberius::numeric::Numeric, _>(i) {
                return v.to_string();
            }
            "NULL".to_string()
        })
        .collect()
}

async fn get_sqlserver_client() -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(11433);
    config.authentication(tiberius::AuthMethod::sql_server("sa", "Test@12345"));
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::Off);

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("Failed to connect to Podman SQL Server");
    tcp.set_nodelay(true).unwrap();

    Client::connect(config, tcp.compat_write())
        .await
        .expect("Failed to connect TDS")
}

async fn compare(sql: &str) {
    let engine = Engine::new();
    let mut client = get_sqlserver_client().await;

    // Run on tsql_core
    let stmt = parse_sql(sql).expect("Failed to parse SQL for engine");
    let engine_res = engine
        .execute(stmt)
        .expect("Engine execution failed")
        .expect("Expected result from engine");
    let engine_rows: Vec<Vec<String>> = engine_res
        .rows
        .iter()
        .map(|r| r.iter().map(engine_val_to_string).collect())
        .collect();

    // Run on SQL Server
    let stream = client
        .query(sql, &[])
        .await
        .expect("SQL Server query failed");
    let ss_rows_raw = stream
        .into_first_result()
        .await
        .expect("Failed to get results from SQL Server");
    let ss_rows: Vec<Vec<String>> = ss_rows_raw.iter().map(tiberius_row_to_strings).collect();

    assert_eq!(engine_rows, ss_rows, "Mismatch for SQL: {}", sql);
    println!("Success comparing: {}", sql);
}

#[tokio::test]
#[ignore] // Skip by default as it requires a running Podman container
async fn test_compare_basic_math() {
    compare("SELECT 1 + 1").await;
    compare("SELECT 10 * 3").await;
    compare("SELECT 100 / 4").await;
    compare("SELECT ABS(-42)").await;
}

#[tokio::test]
#[ignore]
async fn test_compare_strings() {
    compare("SELECT 'hello' + ' world'").await;
    compare("SELECT UPPER('rust')").await;
    compare("SELECT LOWER('SQL')").await;
    compare("SELECT LEN('test')").await;
}

#[tokio::test]
#[ignore]
async fn test_compare_logic() {
    compare("SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END").await;
    compare("SELECT CASE WHEN 1=0 THEN 'yes' ELSE 'no' END").await;
}
