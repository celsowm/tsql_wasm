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

// ─── LEFT OUTER JOIN ─────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_left_outer_join_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_orders", "t_customers"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_customers (id INT PRIMARY KEY, name VARCHAR(50))").await;
    exec_sql(&mut client, "CREATE TABLE t_orders (id INT PRIMARY KEY, customer_id INT, amount INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_customers (id INT PRIMARY KEY, name VARCHAR(50))");
    engine_exec(&mut engine, "CREATE TABLE t_orders (id INT PRIMARY KEY, customer_id INT, amount INT)");

    exec_sql(&mut client, "INSERT INTO t_customers VALUES (1, 'Alice'), (2, 'Bob')").await;
    exec_sql(&mut client, "INSERT INTO t_orders VALUES (100, 1, 500)").await;
    engine_exec(&mut engine, "INSERT INTO t_customers VALUES (1, 'Alice'), (2, 'Bob')");
    engine_exec(&mut engine, "INSERT INTO t_orders VALUES (100, 1, 500)");

    let join_sql = "SELECT c.name, o.amount FROM t_customers c LEFT JOIN t_orders o ON c.id = o.customer_id ORDER BY c.id";
    let (_, sql_rows) = query_sql(&mut client, join_sql).await;
    let engine_result = engine_exec(&mut engine, join_sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "LEFT JOIN linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_orders").await;
    exec_sql(&mut client, "DROP TABLE t_customers").await;
}

// ─── RIGHT OUTER JOIN ────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_right_outer_join_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_dept", "t_emp"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_dept (id INT PRIMARY KEY, name VARCHAR(50))").await;
    exec_sql(&mut client, "CREATE TABLE t_emp (id INT PRIMARY KEY, dept_id INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_dept (id INT PRIMARY KEY, name VARCHAR(50))");
    engine_exec(&mut engine, "CREATE TABLE t_emp (id INT PRIMARY KEY, dept_id INT)");

    exec_sql(&mut client, "INSERT INTO t_dept VALUES (10, 'Sales'), (20, 'IT')").await;
    exec_sql(&mut client, "INSERT INTO t_emp VALUES (1, 10), (2, 10), (3, 30)").await;
    engine_exec(&mut engine, "INSERT INTO t_dept VALUES (10, 'Sales'), (20, 'IT')");
    engine_exec(&mut engine, "INSERT INTO t_emp VALUES (1, 10), (2, 10), (3, 30)");

    let join_sql = "SELECT e.id, d.name FROM t_emp e RIGHT JOIN t_dept d ON e.dept_id = d.id ORDER BY d.id";
    let (_, sql_rows) = query_sql(&mut client, join_sql).await;
    let engine_result = engine_exec(&mut engine, join_sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "RIGHT JOIN linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_emp").await;
    exec_sql(&mut client, "DROP TABLE t_dept").await;
}

// ─── FULL OUTER JOIN ────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_full_outer_join_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    for &t in &["t_a", "t_b"] {
        exec_sql(&mut client, &format!("DROP TABLE IF EXISTS {}", t)).await;
    }

    exec_sql(&mut client, "CREATE TABLE t_a (id INT, val VARCHAR(10))").await;
    exec_sql(&mut client, "CREATE TABLE t_b (id INT, val VARCHAR(10))").await;
    engine_exec(&mut engine, "CREATE TABLE t_a (id INT, val VARCHAR(10))");
    engine_exec(&mut engine, "CREATE TABLE t_b (id INT, val VARCHAR(10))");

    exec_sql(&mut client, "INSERT INTO t_a VALUES (1, 'A1'), (2, 'A2')").await;
    exec_sql(&mut client, "INSERT INTO t_b VALUES (2, 'B2'), (3, 'B3')").await;
    engine_exec(&mut engine, "INSERT INTO t_a VALUES (1, 'A1'), (2, 'A2')");
    engine_exec(&mut engine, "INSERT INTO t_b VALUES (2, 'B2'), (3, 'B3')");

    let join_sql = "SELECT a.id AS aid, b.id AS bid FROM t_a a FULL OUTER JOIN t_b b ON a.id = b.id ORDER BY COALESCE(a.id, 999)";
    let (_, sql_rows) = query_sql(&mut client, join_sql).await;
    let engine_result = engine_exec(&mut engine, join_sql).unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "FULL OUTER JOIN linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_a").await;
    exec_sql(&mut client, "DROP TABLE t_b").await;
}