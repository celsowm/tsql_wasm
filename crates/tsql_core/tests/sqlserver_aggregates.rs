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
            if let Ok(Some(v)) = row.try_get::<u8, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<f64, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<bool, _>(i) {
                return if v { "1".to_string() } else { "0".to_string() };
            }
            if let Ok(Some(v)) = row.try_get::<tiberius::numeric::Numeric, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<tiberius::time::chrono::NaiveDateTime, _>(i) {
                return v.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
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

// ─── COUNT(*) ───────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_count_star_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_agg").await;
    exec_sql(&mut client, "CREATE TABLE t_agg (id INT, val INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_agg (id INT, val INT)");

    exec_sql(&mut client, "INSERT INTO t_agg VALUES (1, 10), (2, 20), (3, 30)").await;
    engine_exec(&mut engine, "INSERT INTO t_agg VALUES (1, 10), (2, 20), (3, 30)");

    let sql = "SELECT COUNT(*) FROM t_agg";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── SUM ────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_sum_aggregate_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_sum").await;
    exec_sql(&mut client, "CREATE TABLE t_sum (val INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_sum (val INT)");

    exec_sql(&mut client, "INSERT INTO t_sum VALUES (100), (200), (300)").await;
    engine_exec(&mut engine, "INSERT INTO t_sum VALUES (100), (200), (300)");

    let sql = "SELECT SUM(val) FROM t_sum";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── AVG ────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_avg_aggregate_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_avg").await;
    exec_sql(&mut client, "CREATE TABLE t_avg (val INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_avg (val INT)");

    exec_sql(&mut client, "INSERT INTO t_avg VALUES (10), (20), (30)").await;
    engine_exec(&mut engine, "INSERT INTO t_avg VALUES (10), (20), (30)");

    let sql = "SELECT AVG(val) FROM t_avg";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── MIN/MAX ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_min_max_aggregate_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_minmax").await;
    exec_sql(&mut client, "CREATE TABLE t_minmax (val INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_minmax (val INT)");

    exec_sql(&mut client, "INSERT INTO t_minmax VALUES (5), (15), (25)").await;
    engine_exec(&mut engine, "INSERT INTO t_minmax VALUES (5), (15), (25)");

    let sql = "SELECT MIN(val), MAX(val) FROM t_minmax";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows[0].len(), engine_result.rows[0].len());
    for (i, (sql_cell, eng_cell)) in sql_rows[0].iter().zip(values_to_strings(&engine_result.rows[0]).iter()).enumerate() {
        assert_eq!(sql_cell, eng_cell, "MIN/MAX coluna {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_minmax").await;
}

// ─── GROUP BY ───────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_group_by_aggregate_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_grp").await;
    exec_sql(&mut client, "CREATE TABLE t_grp (cat VARCHAR(10), val INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_grp (cat VARCHAR(10), val INT)");

    exec_sql(&mut client, "INSERT INTO t_grp VALUES ('A', 10), ('A', 20), ('B', 30)").await;
    engine_exec(&mut engine, "INSERT INTO t_grp VALUES ('A', 10), ('A', 20), ('B', 30)");

    let sql = "SELECT cat, SUM(val) AS total FROM t_grp GROUP BY cat ORDER BY cat";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "GROUP BY linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_grp").await;
}