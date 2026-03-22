use tsql_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

#[test]
fn test_tinyint_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (id TINYINT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (id) VALUES (0)");
    exec(&mut engine, "INSERT INTO dbo.t (id) VALUES (255)");
    let r = query(&mut engine, "SELECT id FROM dbo.t ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::TinyInt(0));
    assert_eq!(r.rows[1][0], Value::TinyInt(255));
}

#[test]
fn test_tinyint_overflow() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (id TINYINT NOT NULL)");
    let stmt = parse_sql("INSERT INTO dbo.t (id) VALUES (256)").unwrap();
    let result = engine.execute(stmt);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("overflow"));
}

#[test]
fn test_smallint_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (id SMALLINT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (id) VALUES (100)");
    exec(&mut engine, "INSERT INTO dbo.t (id) VALUES (32767)");
    let r = query(&mut engine, "SELECT id FROM dbo.t ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::SmallInt(100));
    assert_eq!(r.rows[1][0], Value::SmallInt(32767));
}

#[test]
fn test_decimal_basic() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (price DECIMAL(10,2) NOT NULL)",
    );
    exec(&mut engine, "INSERT INTO dbo.t (price) VALUES ('19.99')");
    exec(&mut engine, "INSERT INTO dbo.t (price) VALUES ('0.50')");
    let r = query(&mut engine, "SELECT price FROM dbo.t ORDER BY price");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Decimal(50, 2));
    assert_eq!(r.rows[1][0], Value::Decimal(1999, 2));
}

#[test]
fn test_decimal_cast() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CAST('123.45' AS DECIMAL(10,2)) AS val");
    assert_eq!(r.rows[0][0], Value::Decimal(12345, 2));
}

#[test]
fn test_char_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (code CHAR(5) NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (code) VALUES ('AB')");
    let r = query(&mut engine, "SELECT code FROM dbo.t");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Char("AB   ".to_string()));
}

#[test]
fn test_nchar_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (code NCHAR(3) NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (code) VALUES (N'AB')");
    let r = query(&mut engine, "SELECT code FROM dbo.t");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::NChar("AB ".to_string()));
}

#[test]
fn test_date_type() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (d DATE NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (d) VALUES ('2025-06-15')");
    let r = query(&mut engine, "SELECT d FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::Date("2025-06-15".to_string()));
}

#[test]
fn test_time_type() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (t TIME NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (t) VALUES ('14:30:00')");
    let r = query(&mut engine, "SELECT t FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::Time("14:30:00".to_string()));
}

#[test]
fn test_datetime2_type() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (dt DATETIME2 NOT NULL)");
    exec(
        &mut engine,
        "INSERT INTO dbo.t (dt) VALUES ('2025-06-15T14:30:00')",
    );
    let r = query(&mut engine, "SELECT dt FROM dbo.t");
    assert_eq!(
        r.rows[0][0],
        Value::DateTime2("2025-06-15T14:30:00".to_string())
    );
}

#[test]
fn test_uniqueidentifier_type() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (id UNIQUEIDENTIFIER NOT NULL)",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.t (id) VALUES ('550e8400-e29b-41d4-a716-446655440000')",
    );
    let r = query(&mut engine, "SELECT id FROM dbo.t");
    assert_eq!(
        r.rows[0][0],
        Value::UniqueIdentifier("550e8400-e29b-41d4-a716-446655440000".to_string())
    );
}

#[test]
fn test_string_length_enforcement() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (name VARCHAR(5) NOT NULL)");
    let stmt = parse_sql("INSERT INTO dbo.t (name) VALUES ('toolong')").unwrap();
    let result = engine.execute(stmt);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("truncated"));
}

#[test]
fn test_cast_to_new_types() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CAST(42 AS TINYINT) AS v");
    assert_eq!(r.rows[0][0], Value::TinyInt(42));

    let r = query(&mut engine, "SELECT CAST(42 AS SMALLINT) AS v");
    assert_eq!(r.rows[0][0], Value::SmallInt(42));

    let r = query(&mut engine, "SELECT CAST('hello' AS CHAR(10)) AS v");
    assert_eq!(r.rows[0][0], Value::Char("hello     ".to_string()));

    let r = query(&mut engine, "SELECT CAST('2025-01-01' AS DATE) AS v");
    assert_eq!(r.rows[0][0], Value::Date("2025-01-01".to_string()));
}

#[test]
fn test_convert_new_types() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONVERT(TINYINT, 100) AS v");
    assert_eq!(r.rows[0][0], Value::TinyInt(100));

    let r = query(&mut engine, "SELECT CONVERT(SMALLINT, 999) AS v");
    assert_eq!(r.rows[0][0], Value::SmallInt(999));
}
