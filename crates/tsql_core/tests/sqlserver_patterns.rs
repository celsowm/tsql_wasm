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

// ─── LIKE % ──────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_like_pattern_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_like").await;
    exec_sql(&mut client, "CREATE TABLE t_like (name VARCHAR(20))").await;
    engine_exec(&mut engine, "CREATE TABLE t_like (name VARCHAR(20))");

    exec_sql(&mut client, "INSERT INTO t_like VALUES ('Apple'), ('Banana'), ('Apricot')").await;
    engine_exec(&mut engine, "INSERT INTO t_like VALUES ('Apple'), ('Banana'), ('Apricot')");

    let sql = "SELECT name FROM t_like WHERE name LIKE 'A%' ORDER BY name";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "LIKE linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_like").await;
}

// ─── LIKE _ ──────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_like_underscore_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_und").await;
    exec_sql(&mut client, "CREATE TABLE t_und (code VARCHAR(10))").await;
    engine_exec(&mut engine, "CREATE TABLE t_und (code VARCHAR(10))");

    exec_sql(&mut client, "INSERT INTO t_und VALUES ('A1'), ('AB'), ('A12')").await;
    engine_exec(&mut engine, "INSERT INTO t_und VALUES ('A1'), ('AB'), ('A12')");

    let sql = "SELECT code FROM t_und WHERE code LIKE 'A_'";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "LIKE _ linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_und").await;
}

// ─── IN LIST ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_in_list_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_inlist").await;
    exec_sql(&mut client, "CREATE TABLE t_inlist (id INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_inlist (id INT)");

    exec_sql(&mut client, "INSERT INTO t_inlist VALUES (1), (2), (3), (4)").await;
    engine_exec(&mut engine, "INSERT INTO t_inlist VALUES (1), (2), (3), (4)");

    let sql = "SELECT id FROM t_inlist WHERE id IN (1, 3) ORDER BY id";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "IN list linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_inlist").await;
}

// ─── BETWEEN ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_between_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_btw").await;
    exec_sql(&mut client, "CREATE TABLE t_btw (val INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_btw (val INT)");

    exec_sql(&mut client, "INSERT INTO t_btw VALUES (1), (5), (10), (15)").await;
    engine_exec(&mut engine, "INSERT INTO t_btw VALUES (1), (5), (10), (15)");

    let sql = "SELECT val FROM t_btw WHERE val BETWEEN 5 AND 10 ORDER BY val";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "BETWEEN linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_btw").await;
}