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

// ─── DISTINCT ───────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_distinct_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_dist").await;
    exec_sql(&mut client, "CREATE TABLE t_dist (val VARCHAR(10))").await;
    engine_exec(&mut engine, "CREATE TABLE t_dist (val VARCHAR(10))");

    exec_sql(&mut client, "INSERT INTO t_dist VALUES ('A'), ('B'), ('A'), ('C')").await;
    engine_exec(&mut engine, "INSERT INTO t_dist VALUES ('A'), ('B'), ('A'), ('C')");

    let (_, sql_rows) = query_sql(&mut client, "SELECT DISTINCT val FROM t_dist ORDER BY val").await;
    let engine_result = engine_exec(&mut engine, "SELECT DISTINCT val FROM t_dist ORDER BY val").unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "DISTINCT linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_dist").await;
}

// ─── UNION ALL ───────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_union_all_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_union_a", "t_union_b"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_union_a (id INT)").await;
    exec_sql(&mut client, "CREATE TABLE t_union_b (id INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_union_a (id INT)");
    engine_exec(&mut engine, "CREATE TABLE t_union_b (id INT)");

    exec_sql(&mut client, "INSERT INTO t_union_a VALUES (1), (2)").await;
    exec_sql(&mut client, "INSERT INTO t_union_b VALUES (2), (3)").await;
    engine_exec(&mut engine, "INSERT INTO t_union_a VALUES (1), (2)");
    engine_exec(&mut engine, "INSERT INTO t_union_b VALUES (2), (3)");

    let sql = "SELECT id FROM t_union_a UNION ALL SELECT id FROM t_union_b ORDER BY id";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "UNION ALL linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_union_a").await;
    exec_sql(&mut client, "DROP TABLE t_union_b").await;
}

// ─── UNION ────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_union_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_u1", "t_u2"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_u1 (val VARCHAR(10))").await;
    exec_sql(&mut client, "CREATE TABLE t_u2 (val VARCHAR(10))").await;
    engine_exec(&mut engine, "CREATE TABLE t_u1 (val VARCHAR(10))");
    engine_exec(&mut engine, "CREATE TABLE t_u2 (val VARCHAR(10))");

    exec_sql(&mut client, "INSERT INTO t_u1 VALUES ('A'), ('B')").await;
    exec_sql(&mut client, "INSERT INTO t_u2 VALUES ('B'), ('C')").await;
    engine_exec(&mut engine, "INSERT INTO t_u1 VALUES ('A'), ('B')");
    engine_exec(&mut engine, "INSERT INTO t_u2 VALUES ('B'), ('C')");

    let sql = "SELECT val FROM t_u1 UNION SELECT val FROM t_u2 ORDER BY val";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "UNION linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_u1").await;
    exec_sql(&mut client, "DROP TABLE t_u2").await;
}

// ─── EXCEPT ──────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_except_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_ex1", "t_ex2"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_ex1 (id INT)").await;
    exec_sql(&mut client, "CREATE TABLE t_ex2 (id INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_ex1 (id INT)");
    engine_exec(&mut engine, "CREATE TABLE t_ex2 (id INT)");

    exec_sql(&mut client, "INSERT INTO t_ex1 VALUES (1), (2), (3)").await;
    exec_sql(&mut client, "INSERT INTO t_ex2 VALUES (2), (3)").await;
    engine_exec(&mut engine, "INSERT INTO t_ex1 VALUES (1), (2), (3)");
    engine_exec(&mut engine, "INSERT INTO t_ex2 VALUES (2), (3)");

    let sql = "SELECT id FROM t_ex1 EXCEPT SELECT id FROM t_ex2 ORDER BY id";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "EXCEPT linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_ex1").await;
    exec_sql(&mut client, "DROP TABLE t_ex2").await;
}

// ─── INTERSECT ──────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_intersect_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_in1", "t_in2"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_in1 (id INT)").await;
    exec_sql(&mut client, "CREATE TABLE t_in2 (id INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_in1 (id INT)");
    engine_exec(&mut engine, "CREATE TABLE t_in2 (id INT)");

    exec_sql(&mut client, "INSERT INTO t_in1 VALUES (1), (2), (3)").await;
    exec_sql(&mut client, "INSERT INTO t_in2 VALUES (2), (3), (4)").await;
    engine_exec(&mut engine, "INSERT INTO t_in1 VALUES (1), (2), (3)");
    engine_exec(&mut engine, "INSERT INTO t_in2 VALUES (2), (3), (4)");

    let sql = "SELECT id FROM t_in1 INTERSECT SELECT id FROM t_in2 ORDER BY id";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "INTERSECT linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_in1").await;
    exec_sql(&mut client, "DROP TABLE t_in2").await;
}