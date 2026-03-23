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

// ─── INSERT single row ─────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_insert_single_row_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_ins1").await;
    exec_sql(&mut client, "CREATE TABLE t_ins1 (id INT, name VARCHAR(20))").await;
    engine_exec(&mut engine, "CREATE TABLE t_ins1 (id INT, name VARCHAR(20))");

    exec_sql(&mut client, "INSERT INTO t_ins1 VALUES (1, 'Alice')").await;
    engine_exec(&mut engine, "INSERT INTO t_ins1 VALUES (1, 'Alice')");

    let (_, sql_rows) = query_sql(&mut client, "SELECT * FROM t_ins1").await;
    let engine_result = engine_exec(&mut engine, "SELECT * FROM t_ins1").unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "INSERT linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_ins1").await;
}

// ─── INSERT multi-row ──────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_insert_multi_row_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_insmulti").await;
    exec_sql(&mut client, "CREATE TABLE t_insmulti (id INT, val INT)").await;
    engine_exec(&mut engine, "CREATE TABLE t_insmulti (id INT, val INT)");

    exec_sql(&mut client, "INSERT INTO t_insmulti VALUES (1, 10), (2, 20), (3, 30)").await;
    engine_exec(&mut engine, "INSERT INTO t_insmulti VALUES (1, 10), (2, 20), (3, 30)");

    let (_, sql_rows) = query_sql(&mut client, "SELECT * FROM t_insmulti ORDER BY id").await;
    let engine_result = engine_exec(&mut engine, "SELECT * FROM t_insmulti ORDER BY id").unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "INSERT multi linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_insmulti").await;
}

// ─── INSERT with NULL ─────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_insert_null_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_insnull").await;
    exec_sql(&mut client, "CREATE TABLE t_insnull (id INT, val VARCHAR(10))").await;
    engine_exec(&mut engine, "CREATE TABLE t_insnull (id INT, val VARCHAR(10))");

    exec_sql(&mut client, "INSERT INTO t_insnull VALUES (1, NULL), (2, 'text')").await;
    engine_exec(&mut engine, "INSERT INTO t_insnull VALUES (1, NULL), (2, 'text')");

    let (_, sql_rows) = query_sql(&mut client, "SELECT * FROM t_insnull ORDER BY id").await;
    let engine_result = engine_exec(&mut engine, "SELECT * FROM t_insnull ORDER BY id").unwrap();

    assert_eq!(sql_rows.len(), engine_result.rows.len());
    for (i, (sql_row, eng_row)) in sql_rows.iter().zip(engine_result.rows.iter()).enumerate() {
        let eng_strings = values_to_strings(eng_row);
        assert_eq!(sql_row, &eng_strings, "INSERT NULL linha {} diverge", i);
    }

    exec_sql(&mut client, "DROP TABLE t_insnull").await;
}

// ─── UPDATE ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_update_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_upd").await;
    exec_sql(&mut client, "CREATE TABLE t_upd (id INT, val INT)").await;
    exec_sql(&mut client, "INSERT INTO t_upd VALUES (1, 10), (2, 20)").await;
    engine_exec(&mut engine, "CREATE TABLE t_upd (id INT, val INT)");
    engine_exec(&mut engine, "INSERT INTO t_upd VALUES (1, 10), (2, 20)");

    exec_sql(&mut client, "UPDATE t_upd SET val = 99 WHERE id = 1").await;
    engine_exec(&mut engine, "UPDATE t_upd SET val = 99 WHERE id = 1");

    let (_, sql_rows) = query_sql(&mut client, "SELECT val FROM t_upd WHERE id = 1").await;
    let engine_result = engine_exec(&mut engine, "SELECT val FROM t_upd WHERE id = 1").unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);

    exec_sql(&mut client, "DROP TABLE t_upd").await;
}

// ─── UPDATE multiple rows ─────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_update_multiple_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_updmulti").await;
    exec_sql(&mut client, "CREATE TABLE t_updmulti (id INT, val INT)").await;
    exec_sql(&mut client, "INSERT INTO t_updmulti VALUES (1, 10), (2, 20), (3, 30)").await;
    engine_exec(&mut engine, "CREATE TABLE t_updmulti (id INT, val INT)");
    engine_exec(&mut engine, "INSERT INTO t_updmulti VALUES (1, 10), (2, 20), (3, 30)");

    exec_sql(&mut client, "UPDATE t_updmulti SET val = val * 2").await;
    engine_exec(&mut engine, "UPDATE t_updmulti SET val = val * 2");

    let (_, sql_rows) = query_sql(&mut client, "SELECT SUM(val) FROM t_updmulti").await;
    let engine_result = engine_exec(&mut engine, "SELECT SUM(val) FROM t_updmulti").unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);

    exec_sql(&mut client, "DROP TABLE t_updmulti").await;
}

// ─── DELETE ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_delete_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_del").await;
    exec_sql(&mut client, "CREATE TABLE t_del (id INT)").await;
    exec_sql(&mut client, "INSERT INTO t_del VALUES (1), (2), (3)").await;
    engine_exec(&mut engine, "CREATE TABLE t_del (id INT)");
    engine_exec(&mut engine, "INSERT INTO t_del VALUES (1), (2), (3)");

    exec_sql(&mut client, "DELETE FROM t_del WHERE id = 2").await;
    engine_exec(&mut engine, "DELETE FROM t_del WHERE id = 2");

    let (_, sql_rows) = query_sql(&mut client, "SELECT COUNT(*) FROM t_del").await;
    let engine_result = engine_exec(&mut engine, "SELECT COUNT(*) FROM t_del").unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);

    exec_sql(&mut client, "DROP TABLE t_del").await;
}

// ─── DELETE all ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_delete_all_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    exec_sql(&mut client, "DROP TABLE IF EXISTS t_delall").await;
    exec_sql(&mut client, "CREATE TABLE t_delall (id INT)").await;
    exec_sql(&mut client, "INSERT INTO t_delall VALUES (1), (2)").await;
    engine_exec(&mut engine, "CREATE TABLE t_delall (id INT)");
    engine_exec(&mut engine, "INSERT INTO t_delall VALUES (1), (2)");

    exec_sql(&mut client, "DELETE FROM t_delall").await;
    engine_exec(&mut engine, "DELETE FROM t_delall");

    let (_, sql_rows) = query_sql(&mut client, "SELECT COUNT(*) FROM t_delall").await;
    let engine_result = engine_exec(&mut engine, "SELECT COUNT(*) FROM t_delall").unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);

    exec_sql(&mut client, "DROP TABLE t_delall").await;
}