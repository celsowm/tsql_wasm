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

// ========================
// FLOAT type tests
// ========================

#[test]
fn test_float_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val FLOAT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (3.14)");
    let r = query(&mut engine, "SELECT val FROM dbo.t");
    let v = f64::from_bits(match &r.rows[0][0] {
        Value::Float(b) => *b,
        _ => panic!("expected Float"),
    });
    assert!((v - 3.14).abs() < 1e-10);
}

#[test]
fn test_float_arithmetic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (a FLOAT NOT NULL, b FLOAT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (a, b) VALUES (10.0, 3.0)");
    let r = query(&mut engine, "SELECT a + b, a - b, a * b, a / b FROM dbo.t");
    assert_eq!(f64::from_bits(match &r.rows[0][0] { Value::Float(b) => *b, _ => panic!() }), 13.0);
    assert_eq!(f64::from_bits(match &r.rows[0][1] { Value::Float(b) => *b, _ => panic!() }), 7.0);
    assert_eq!(f64::from_bits(match &r.rows[0][2] { Value::Float(b) => *b, _ => panic!() }), 30.0);
    assert!((f64::from_bits(match &r.rows[0][3] { Value::Float(b) => *b, _ => panic!() }) - 10.0/3.0).abs() < 1e-10);
}

#[test]
fn test_float_cast() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CAST('3.14' AS FLOAT) AS v");
    let v = f64::from_bits(match &r.rows[0][0] {
        Value::Float(b) => *b,
        _ => panic!("expected Float"),
    });
    assert!((v - 3.14).abs() < 1e-10);
}

#[test]
fn test_float_negate() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT -CAST(3.14 AS FLOAT) AS v");
    let v = f64::from_bits(match &r.rows[0][0] {
        Value::Float(b) => *b,
        _ => panic!("expected Float"),
    });
    assert!((v - (-3.14)).abs() < 1e-10);
}

#[test]
fn test_float_comparison() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val FLOAT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (1.5)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (2.5)");
    let r = query(&mut engine, "SELECT val FROM dbo.t WHERE val > 2.0");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn test_float_sum_avg() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val FLOAT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (1.0)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (2.0)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (3.0)");
    let r = query(&mut engine, "SELECT SUM(val), AVG(val) FROM dbo.t");
    let sum = f64::from_bits(match &r.rows[0][0] { Value::Float(b) => *b, _ => panic!() });
    assert!((sum - 6.0).abs() < 1e-10);
}

#[test]
fn test_real_type() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val REAL NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (2.5)");
    let r = query(&mut engine, "SELECT val FROM dbo.t");
    let v = f64::from_bits(match &r.rows[0][0] {
        Value::Float(b) => *b,
        _ => panic!("expected Float"),
    });
    assert!((v - 2.5).abs() < 1e-10);
}

// ========================
// MONEY type tests
// ========================

#[test]
fn test_money_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (amount MONEY NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (amount) VALUES ('$123.45')");
    let r = query(&mut engine, "SELECT amount FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::Money(1234500));
}

#[test]
fn test_money_cast() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CAST('100.50' AS MONEY) AS v");
    assert_eq!(r.rows[0][0], Value::Money(1005000));
}

#[test]
fn test_money_arithmetic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (a MONEY NOT NULL, b MONEY NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (a, b) VALUES ('$10.00', '$5.00')");
    let r = query(&mut engine, "SELECT a + b, a - b FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::Money(150000));
    assert_eq!(r.rows[0][1], Value::Money(50000));
}

#[test]
fn test_money_comparison() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (amount MONEY NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (amount) VALUES ('$10.00')");
    exec(&mut engine, "INSERT INTO dbo.t (amount) VALUES ('$20.00')");
    let r = query(&mut engine, "SELECT amount FROM dbo.t WHERE amount > '$15.00'");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn test_money_to_string() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CAST('$123.45' AS MONEY) AS v");
    let s = r.rows[0][0].to_string_value();
    assert!(s.contains("$123.45"), "got: {}", s);
}

#[test]
fn test_smallmoney_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (amount SMALLMONEY NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (amount) VALUES ('$99.99')");
    let r = query(&mut engine, "SELECT amount FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::SmallMoney(999900));
}

// ========================
// BINARY type tests
// ========================

#[test]
fn test_binary_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (data BINARY(4) NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (data) VALUES (0xDEADBEEF)");
    let r = query(&mut engine, "SELECT data FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[test]
fn test_binary_zero_padded() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (data BINARY(4) NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (data) VALUES (0xAB)");
    let r = query(&mut engine, "SELECT data FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::Binary(vec![0xAB, 0x00, 0x00, 0x00]));
}

#[test]
fn test_binary_comparison() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (data BINARY(2) NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (data) VALUES (0x0102)");
    exec(&mut engine, "INSERT INTO dbo.t (data) VALUES (0x0304)");
    let r = query(&mut engine, "SELECT data FROM dbo.t ORDER BY data");
    assert_eq!(r.rows[0][0], Value::Binary(vec![0x01, 0x02]));
    assert_eq!(r.rows[1][0], Value::Binary(vec![0x03, 0x04]));
}

#[test]
fn test_varbinary_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (data VARBINARY(10) NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (data) VALUES (0xCAFEBABE)");
    let r = query(&mut engine, "SELECT data FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::VarBinary(vec![0xCA, 0xFE, 0xBA, 0xBE]));
}

#[test]
fn test_binary_to_string() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CAST(0xDEADBEEF AS VARBINARY(4)) AS v");
    let s = r.rows[0][0].to_string_value();
    assert_eq!(s, "0xDEADBEEF");
}

#[test]
fn test_binary_cast_from_string() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CAST('0xABCD' AS VARBINARY(2)) AS v");
    assert_eq!(r.rows[0][0], Value::VarBinary(vec![0xAB, 0xCD]));
}

#[test]
fn test_binary_declare() {
    let mut engine = Engine::new();
    exec(&mut engine, "DECLARE @b BINARY(4) = 0x11223344");
    exec(&mut engine, "DECLARE @vb VARBINARY(10) = 0xAABB");
}
