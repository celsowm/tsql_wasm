use tsql_core::{types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect(sql);
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine.query(sql).expect(sql)
}

#[test]
fn coercion_int_to_tinyint() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(42 AS TINYINT)");
    assert_eq!(r.rows[0][0], Value::TinyInt(42));
}

#[test]
fn coercion_int_overflow_to_tinyint() {
    let mut e = Engine::new();
    let result = e.exec("SELECT CAST(256 AS TINYINT)");
    assert!(result.is_err(), "256 exceeds TINYINT max (255)");
}

#[test]
fn coercion_int_to_smallint() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(42 AS SMALLINT)");
    assert_eq!(r.rows[0][0], Value::SmallInt(42));
}

#[test]
fn coercion_int_to_bigint() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(42 AS BIGINT)");
    assert_eq!(r.rows[0][0], Value::BigInt(42));
}

#[test]
fn coercion_bigint_to_int() {
    let mut e = Engine::new();
    let r = query(
        &mut e,
        "SELECT CAST(42 AS BIGINT) AS b, CAST(42 AS INT) AS i",
    );
    assert_eq!(r.rows[0][0], Value::BigInt(42));
    assert_eq!(r.rows[0][1], Value::Int(42));
}

#[test]
fn coercion_varchar_to_int() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST('123' AS INT)");
    assert_eq!(r.rows[0][0], Value::Int(123));
}

#[test]
fn coercion_varchar_invalid_to_int() {
    let mut e = Engine::new();
    let result = e.exec("SELECT CAST('abc' AS INT)");
    assert!(result.is_err(), "cannot parse 'abc' as INT");
}

#[test]
fn coercion_float_to_int() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(42.7 AS INT)");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn coercion_float_to_int_truncation() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(42.9 AS INT)");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn coercion_decimal_to_int() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(CAST(42 AS DECIMAL(10,2)) AS INT)");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn coercion_decimal_to_tinyint_overflow() {
    let mut e = Engine::new();
    let result = e.exec("SELECT CAST(300 AS DECIMAL(10,2)) AS TINYINT");
    assert!(result.is_err(), "300 exceeds TINYINT max (255)");
}

#[test]
fn coercion_date_to_varchar() {
    let mut e = Engine::new();
    let r = query(
        &mut e,
        "SELECT CAST(CAST('2025-01-15' AS DATE) AS VARCHAR(10))",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "2025-01-15");
}

#[test]
fn coercion_datetime_to_date() {
    let mut e = Engine::new();
    let r = query(
        &mut e,
        "SELECT CAST(CAST('2025-01-15 10:30:00' AS DATETIME) AS DATE)",
    );
    let expected = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    assert_eq!(r.rows[0][0], Value::Date(expected));
}

#[test]
fn coercion_null_to_int() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(NULL AS INT)");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn coercion_null_to_varchar() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(NULL AS VARCHAR(10))");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn coercion_string_to_date() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST('2025-01-15' AS DATE)");
    let expected = chrono::NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    assert_eq!(r.rows[0][0], Value::Date(expected));
}

#[test]
fn coercion_string_to_datetime() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST('2025-01-15 10:30:00' AS DATETIME)");
    assert!(matches!(r.rows[0][0], Value::DateTime(_)));
}

#[test]
fn coercion_int_to_bit() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(1 AS BIT)");
    assert_eq!(r.rows[0][0], Value::Bit(true));
}

#[test]
fn coercion_zero_to_bit() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(0 AS BIT)");
    assert_eq!(r.rows[0][0], Value::Bit(false));
}

#[test]
fn coercion_bit_to_int() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(CAST(1 AS BIT) AS INT)");
    assert_eq!(r.rows[0][0], Value::Int(1));
}

#[test]
fn coercion_uniqueidentifier_roundtrip() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST(CAST('550e8400-e29b-41d4-a716-446655440000' AS UNIQUEIDENTIFIER) AS VARCHAR(36))");
    assert_eq!(
        r.rows[0][0].to_string_value().to_uppercase(),
        "550E8400-E29B-41D4-A716-446655440000"
    );
}

#[test]
fn coercion_varchar_to_float() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CAST('3.14' AS FLOAT)");
    let v = f64::from_bits(match &r.rows[0][0] {
        Value::Float(b) => *b,
        _ => panic!("expected Float"),
    });
    assert!((v - 3.14).abs() < 0.001);
}

#[test]
fn coercion_explicit_convert_int_to_varchar() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CONVERT(VARCHAR(10), 42)");
    assert_eq!(r.rows[0][0].to_string_value(), "42");
}

#[test]
fn coercion_convert_with_style() {
    let mut e = Engine::new();
    let r = query(
        &mut e,
        "SELECT CONVERT(VARCHAR(10), CAST('2025-01-15' AS DATE), 23)",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "2025-01-15");
}

#[test]
fn coercion_nested_conversions() {
    let mut e = Engine::new();
    let r = query(
        &mut e,
        "SELECT CAST(CAST(CAST(42 AS INT) AS VARCHAR(10)) AS INT)",
    );
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn coercion_table_column_coercion() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.src (val VARCHAR(10))");
    exec(&mut e, "INSERT INTO dbo.src VALUES ('123')");
    let r = query(&mut e, "SELECT CAST(val AS INT) FROM dbo.src");
    assert_eq!(r.rows[0][0], Value::Int(123));
}

#[test]
fn coercion_numeric_plus_numeric() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT 10 + 5");
    assert_eq!(r.rows[0][0], Value::BigInt(15));
}

#[test]
fn coercion_try_cast_valid() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT TRY_CAST('123' AS INT)");
    assert_eq!(r.rows[0][0], Value::Int(123));
}

#[test]
fn coercion_try_cast_invalid() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT TRY_CAST('abc' AS INT)");
    assert!(
        r.rows[0][0].is_null(),
        "TRY_CAST should return NULL for invalid input"
    );
}

#[test]
fn coercion_try_convert_valid() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT TRY_CONVERT(INT, '456')");
    assert_eq!(r.rows[0][0], Value::Int(456));
}

#[test]
fn coercion_try_convert_overflow() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT TRY_CONVERT(TINYINT, 999)");
    assert!(
        r.rows[0][0].is_null(),
        "TRY_CONVERT should return NULL for overflow"
    );
}

#[test]
fn coercion_insert_implicit() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.coerce_test (val INT)");
    exec(&mut e, "INSERT INTO dbo.coerce_test VALUES ('42')");
    let r = query(&mut e, "SELECT val FROM dbo.coerce_test");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn coercion_update_implicit() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.coerce_test (val INT)");
    exec(&mut e, "INSERT INTO dbo.coerce_test VALUES (0)");
    exec(&mut e, "UPDATE dbo.coerce_test SET val = '123'");
    let r = query(&mut e, "SELECT val FROM dbo.coerce_test");
    assert_eq!(r.rows[0][0], Value::Int(123));
}
