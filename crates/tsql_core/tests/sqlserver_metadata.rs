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

async fn exec_sql(client: &mut Client<tokio_util::compat::Compat<TcpStream>>, sql: &str) {
    client.execute(sql, &[]).await.expect(&format!("SQL Server falhou: {}", sql));
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

// ─── sys.tables ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_sys_tables_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_meta1").await;
    exec_sql(&mut client, "CREATE TABLE t_meta1 (id INT, name VARCHAR(20))").await;
    engine_exec(&mut engine, "CREATE TABLE t_meta1 (id INT, name VARCHAR(20))");

    let (_, sql_rows) = query_sql(&mut client, "SELECT name FROM sys.tables WHERE name = 't_meta1'").await;
    let engine_result = engine_exec(&mut engine, "SELECT name FROM sys.tables WHERE name = 't_meta1'").unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    if sql_rows.len() > 0 {
        assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
    }

    exec_sql(&mut client, "DROP TABLE t_meta1").await;
}

// ─── sys.columns ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_sys_columns_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_meta2").await;
    exec_sql(&mut client, "CREATE TABLE t_meta2 (id INT, name VARCHAR(20), age INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_meta2 (id INT, name VARCHAR(20), age INT)");

    let (_, sql_rows) = query_sql(&mut client, "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('t_meta2') ORDER BY column_id").await;
    let engine_result = engine_exec(&mut engine, "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('t_meta2') ORDER BY column_id").unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "sys.columns linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_meta2").await;
}

// ─── INFORMATION_SCHEMA.TABLES ─────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_info_schema_tables_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_is").await;
    exec_sql(&mut client, "CREATE TABLE t_is (id INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_is (id INT)");

    let (_, sql_rows) = query_sql(&mut client, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't_is'").await;
    let engine_result = engine_exec(&mut engine, "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 't_is'").unwrap();

    if sql_rows.len() > 0 {
        assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
    }

    exec_sql(&mut client, "DROP TABLE t_is").await;
}

// ─── INFORMATION_SCHEMA.COLUMNS ────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_info_schema_columns_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_iscol").await;
    exec_sql(&mut client, "CREATE TABLE t_iscol (id INT, name VARCHAR(10))").await;
    engine_exec(&mut engine, "CREATE TABLE t_iscol (id INT, name VARCHAR(10))");

    let (_, sql_rows) = query_sql(&mut client, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 't_iscol' ORDER BY ORDINAL_POSITION").await;
    let engine_result = engine_exec(&mut engine, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 't_iscol' ORDER BY ORDINAL_POSITION").unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "INFO_SCHEMA cols linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_iscol").await;
}

// ─── OBJECT_ID function ────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_object_id_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_objid").await;
    exec_sql(&mut client, "CREATE TABLE t_objid (id INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_objid (id INT)");

    let (_, sql_rows) = query_sql(&mut client, "SELECT OBJECT_ID('t_objid')").await;
    let engine_result = engine_exec(&mut engine, "SELECT OBJECT_ID('t_objid')").unwrap();

    let sql_val = &sql_rows[0][0];
    let eng_val = &values_to_strings(&engine_result.rows[0])[0];
    assert_eq!(sql_val, eng_val, "OBJECT_ID diverge");

    exec_sql(&mut client, "DROP TABLE t_objid").await;
}