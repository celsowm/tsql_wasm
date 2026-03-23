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

// ─── DATEADD ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_dateadd_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT DATEADD(day, 5, '2024-01-01')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── DATEDIFF ───────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_datediff_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT DATEDIFF(day, '2024-01-01', '2024-01-10')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    assert_eq!(&sql_rows[0][0], &values_to_strings(&engine_result.rows[0])[0]);
}

// ─── YEAR/MONTH/DAY ─────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_year_month_day_compare() {
    let mut client = connect().await;
    let mut engine = Engine::new();

    let sql = "SELECT YEAR('2024-06-15'), MONTH('2024-06-15'), DAY('2024-06-15')";
    let (_, sql_rows) = query_sql(&mut client, sql).await;
    let engine_result = engine_exec(&mut engine, sql).unwrap();

    for (i, (sql_cell, eng_cell)) in sql_rows[0].iter().zip(values_to_strings(&engine_result.rows[0]).iter()).enumerate() {
        assert_eq!(sql_cell, eng_cell, "YEAR/MONTH/DAY coluna {} diverge", i);
    }
}