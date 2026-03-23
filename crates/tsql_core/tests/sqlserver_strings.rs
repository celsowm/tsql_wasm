use tiberius::{Client, Config, AuthMethod, Row};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tsql_core::{parse_sql, Engine, types::Value};

const SQLSERVER_HOST: &str = "localhost";
const SQLSERVER_PORT: u16 = 11433;
const SQLSERVER_USER: &str = "sa";
const SQLSERVER_PASS: &str = "Test@12345";

async fn connect() -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host(SQLSERVER_HOST);
    config.port(SQLSERVER_PORT);
    config.authentication(AuthMethod::sql_server(SQLSERVER_USER, SQLSERVER_PASS));
    config.trust_cert();

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("Falha ao conectar no SQL Server");
    tcp.set_nodelay(true).unwrap();
    Client::connect(config, tcp.compat_write())
        .await
        .expect("Falha no handshake TDS")
}

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

fn values_to_strings(row: &[Value]) -> Vec<String> {
    row.iter()
        .map(|v| match v {
            Value::Null => "NULL".to_string(),
            Value::VarChar(s) | Value::NVarChar(s) | Value::Char(s) | Value::NChar(s) => s.clone(),
            Value::Int(i) => i.to_string(),
            Value::BigInt(i) => i.to_string(),
            Value::SmallInt(i) => i.to_string(),
            Value::TinyInt(i) => i.to_string(),
            Value::Bit(b) => if *b { "1" } else { "0" }.to_string(),
            _ => v.to_string_value(),
        })
        .collect()
}

async fn query_sql(
    client: &mut Client<tokio_util::compat::Compat<TcpStream>>,
    sql: &str,
) -> (Vec<String>, Vec<Vec<String>>) {
    let stream = client.query(sql, &[]).await.expect(&format!("Query falhou: {}", sql));
    let rows: Vec<Row> = stream.into_first_result().await.expect("Falha ao ler resultado");

    let columns = if let Some(first) = rows.first() {
        let ncols: usize = first.len();
        (0..ncols).map(|i| first.columns()[i].name().to_string()).collect()
    } else {
        vec![]
    };

    let data: Vec<Vec<String>> = rows.iter().map(row_to_strings).collect();
    (columns, data)
}

fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
}

// ─── LEN ────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_len_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT LEN('hello world')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── SUBSTRING ──────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_substring_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT SUBSTRING('hello', 2, 3)";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── UPPER ───────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_upper_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT UPPER('hello')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── LOWER ───────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_lower_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT LOWER('HELLO')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── LTRIM ───────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_ltrim_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT LTRIM('   hello')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── RTRIM ───────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_rtrim_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT RTRIM('hello   ')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── TRIM ────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_trim_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT TRIM('   hello   ')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── REPLACE ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_replace_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT REPLACE('hello world', 'world', 'there')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── CHARINDEX ───────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_charindex_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT CHARINDEX('world', 'hello world')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── LEFT/RIGHT string functions ───────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_left_string_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT LEFT('hello', 3)";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

#[tokio::test]
#[ignore]
async fn test_right_string_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT RIGHT('hello', 3)";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}