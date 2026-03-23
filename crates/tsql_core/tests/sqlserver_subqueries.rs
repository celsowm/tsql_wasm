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

// ─── EXISTS ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_subquery_exists_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_sq_dept", "t_sq_emp"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_sq_dept (id INT PRIMARY KEY, name VARCHAR(20))").await;
    exec_sql(&mut client, "CREATE TABLE t_sq_emp (id INT, dept_id INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_sq_dept (id INT PRIMARY KEY, name VARCHAR(20))");
    engine_exec(&mut engine, "CREATE TABLE t_sq_emp (id INT, dept_id INT)");

    exec_sql(&mut client, "INSERT INTO t_sq_dept VALUES (10, 'Sales'), (20, 'IT')").await;
    exec_sql(&mut client, "INSERT INTO t_sq_emp VALUES (1, 10), (2, 20)").await;
    engine_exec(&mut engine, "INSERT INTO t_sq_dept VALUES (10, 'Sales'), (20, 'IT')");
    engine_exec(&mut engine, "INSERT INTO t_sq_emp VALUES (1, 10), (2, 20)");

    let sql = "SELECT name FROM t_sq_dept WHERE EXISTS (SELECT 1 FROM t_sq_emp WHERE dept_id = t_sq_dept.id)";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "EXISTS linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_sq_emp").await;
    exec_sql(&mut client, "DROP TABLE t_sq_dept").await;
}

// ─── IN ─────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_subquery_in_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_in1", "t_in2"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_in1 (id INT)").await;
    exec_sql(&mut client, "CREATE TABLE t_in2 (code INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_in1 (id INT)");
    engine_exec(&mut engine, "CREATE TABLE t_in2 (code INT)");

    exec_sql(&mut client, "INSERT INTO t_in1 VALUES (1), (2), (3)").await;
    exec_sql(&mut client, "INSERT INTO t_in2 VALUES (1), (2)").await;
    engine_exec(&mut engine, "INSERT INTO t_in1 VALUES (1), (2), (3)");
    engine_exec(&mut engine, "INSERT INTO t_in2 VALUES (1), (2)");

    let sql = "SELECT id FROM t_in1 WHERE id IN (SELECT code FROM t_in2)";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "IN subquery linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_in1").await;
    exec_sql(&mut client, "DROP TABLE t_in2").await;
}

// ─── SCALAR SUBQUERY ───────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_subquery_scalar_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_scalar").await;
    exec_sql(&mut client, "CREATE TABLE t_scalar (id INT, val INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_scalar (id INT, val INT)");

    exec_sql(&mut client, "INSERT INTO t_scalar VALUES (1, 10), (2, 20), (3, 30)").await;
    engine_exec(&mut engine, "INSERT INTO t_scalar VALUES (1, 10), (2, 20), (3, 30)");

    let sql = "SELECT id, (SELECT MAX(val) FROM t_scalar) AS max_val FROM t_scalar WHERE id = 1";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "scalar subquery linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_scalar").await;
}