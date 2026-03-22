use tsql_core::types::Value;
use tsql_core::{parse_sql, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_sql(sql).expect("parse")).expect("exec");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

fn query_value(engine: &mut Engine, sql: &str) -> Value {
    let result = query(engine, sql);
    assert_eq!(result.rows.len(), 1, "Expected 1 row for: {}", sql);
    assert_eq!(result.columns.len(), 1, "Expected 1 column for: {}", sql);
    result.rows[0][0].clone()
}

/// Test arithmetic expression consistency
#[test]
fn test_phase8_expression_arithmetic() {
    let mut engine = Engine::new();

    // Basic arithmetic - engine may return BigInt or Int depending on context
    // We just verify the values are correct
    let result = query_value(&mut engine, "SELECT 1 + 2");
    assert!(matches!(result, Value::Int(3) | Value::BigInt(3)));

    let result = query_value(&mut engine, "SELECT 10 - 3");
    assert!(matches!(result, Value::Int(7) | Value::BigInt(7)));

    let result = query_value(&mut engine, "SELECT 4 * 5");
    assert!(matches!(result, Value::Int(20) | Value::BigInt(20)));

    let result = query_value(&mut engine, "SELECT 20 / 4");
    assert!(matches!(result, Value::Int(5) | Value::BigInt(5)));

    let result = query_value(&mut engine, "SELECT 10 % 3");
    assert!(matches!(result, Value::Int(1) | Value::BigInt(1)));

    // Operator precedence
    let result = query_value(&mut engine, "SELECT 2 + 3 * 4");
    assert!(matches!(result, Value::Int(14) | Value::BigInt(14)));

    // NULL propagation
    assert_eq!(query_value(&mut engine, "SELECT NULL + 1"), Value::Null);
    assert_eq!(query_value(&mut engine, "SELECT NULL - 1"), Value::Null);
    assert_eq!(query_value(&mut engine, "SELECT NULL * 1"), Value::Null);
    assert_eq!(query_value(&mut engine, "SELECT NULL / 1"), Value::Null);
    assert_eq!(query_value(&mut engine, "SELECT 1 + NULL"), Value::Null);
}

/// Test comparison expression consistency
#[test]
fn test_phase8_expression_comparison() {
    let mut engine = Engine::new();

    // Equality
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN 1 = 1 THEN 1 ELSE 0 END"),
        Value::Int(1)
    );
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN 1 = 2 THEN 1 ELSE 0 END"),
        Value::Int(0)
    );

    // Inequality (only <> is supported, not !=)
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN 1 <> 2 THEN 1 ELSE 0 END"),
        Value::Int(1)
    );
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN 1 <> 1 THEN 1 ELSE 0 END"),
        Value::Int(0)
    );

    // Less than
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN 1 < 2 THEN 1 ELSE 0 END"),
        Value::Int(1)
    );
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN 2 < 1 THEN 1 ELSE 0 END"),
        Value::Int(0)
    );

    // Greater than
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN 2 > 1 THEN 1 ELSE 0 END"),
        Value::Int(1)
    );
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN 1 > 2 THEN 1 ELSE 0 END"),
        Value::Int(0)
    );

    // NULL comparisons (three-valued logic)
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN NULL = NULL THEN 1 ELSE 0 END"),
        Value::Int(0)
    );
    assert_eq!(
        query_value(&mut engine, "SELECT CASE WHEN NULL = 1 THEN 1 ELSE 0 END"),
        Value::Int(0)
    );
}

/// Test logical expression consistency
#[test]
fn test_phase8_expression_logical() {
    let mut engine = Engine::new();

    // AND truth table
    assert_eq!(
        query_value(
            &mut engine,
            "SELECT CASE WHEN 1 = 1 AND 2 = 2 THEN 1 ELSE 0 END"
        ),
        Value::Int(1)
    );
    assert_eq!(
        query_value(
            &mut engine,
            "SELECT CASE WHEN 1 = 1 AND 1 = 2 THEN 1 ELSE 0 END"
        ),
        Value::Int(0)
    );

    // OR truth table
    assert_eq!(
        query_value(
            &mut engine,
            "SELECT CASE WHEN 1 = 1 OR 2 = 2 THEN 1 ELSE 0 END"
        ),
        Value::Int(1)
    );
    assert_eq!(
        query_value(
            &mut engine,
            "SELECT CASE WHEN 1 = 2 OR 1 = 2 THEN 1 ELSE 0 END"
        ),
        Value::Int(0)
    );

    // NOT - use explicit parentheses for precedence
    assert_eq!(
        query_value(
            &mut engine,
            "SELECT CASE WHEN NOT (1 = 2) THEN 1 ELSE 0 END"
        ),
        Value::Int(1)
    );
    assert_eq!(
        query_value(
            &mut engine,
            "SELECT CASE WHEN NOT (1 = 1) THEN 1 ELSE 0 END"
        ),
        Value::Int(0)
    );
}

/// Test string function consistency
#[test]
fn test_phase8_expression_string_functions() {
    let mut engine = Engine::new();

    // LEN
    let result = query_value(&mut engine, "SELECT LEN('hello')");
    assert!(matches!(result, Value::Int(5) | Value::BigInt(5)));

    let result = query_value(&mut engine, "SELECT LEN('hello  ')");
    assert!(matches!(result, Value::Int(5) | Value::BigInt(5)));

    let result = query_value(&mut engine, "SELECT LEN('')");
    assert!(matches!(result, Value::Int(0) | Value::BigInt(0)));

    assert_eq!(query_value(&mut engine, "SELECT LEN(NULL)"), Value::Null);

    // UPPER/LOWER
    assert_eq!(
        query_value(&mut engine, "SELECT UPPER('hello')"),
        Value::VarChar("HELLO".to_string())
    );
    assert_eq!(
        query_value(&mut engine, "SELECT LOWER('HELLO')"),
        Value::VarChar("hello".to_string())
    );

    // SUBSTRING
    assert_eq!(
        query_value(&mut engine, "SELECT SUBSTRING('hello', 2, 3)"),
        Value::VarChar("ell".to_string())
    );
    assert_eq!(
        query_value(&mut engine, "SELECT SUBSTRING('hello', 1, 5)"),
        Value::VarChar("hello".to_string())
    );

    // REPLACE
    assert_eq!(
        query_value(&mut engine, "SELECT REPLACE('hello', 'l', 'x')"),
        Value::VarChar("hexxo".to_string())
    );

    // TRIM/LTRIM/RTRIM
    assert_eq!(
        query_value(&mut engine, "SELECT TRIM('  hello  ')"),
        Value::VarChar("hello".to_string())
    );
    assert_eq!(
        query_value(&mut engine, "SELECT LTRIM('  hello  ')"),
        Value::VarChar("hello  ".to_string())
    );
    assert_eq!(
        query_value(&mut engine, "SELECT RTRIM('  hello  ')"),
        Value::VarChar("  hello".to_string())
    );

    // CHARINDEX
    let result = query_value(&mut engine, "SELECT CHARINDEX('l', 'hello')");
    assert!(matches!(result, Value::Int(3) | Value::BigInt(3)));

    let result = query_value(&mut engine, "SELECT CHARINDEX('x', 'hello')");
    assert!(matches!(result, Value::Int(0) | Value::BigInt(0)));

    // String concatenation with +
    assert_eq!(
        query_value(&mut engine, "SELECT 'hello' + ' ' + 'world'"),
        Value::VarChar("hello world".to_string())
    );
}

/// Test date function consistency
#[test]
fn test_phase8_expression_date_functions() {
    let mut engine = Engine::new();

    // DATEADD
    let result = query_value(
        &mut engine,
        "SELECT DATEADD(day, 1, CAST('2024-01-01' AS DATE))",
    );
    assert!(matches!(result, Value::Date(_)));

    // DATEDIFF
    let result = query_value(
        &mut engine,
        "SELECT DATEDIFF(day, CAST('2024-01-01' AS DATE), CAST('2024-01-10' AS DATE))"
    );
    assert!(matches!(result, Value::Int(9) | Value::BigInt(9)));

    // DATEPART
    let result = query_value(
        &mut engine,
        "SELECT DATEPART(year, CAST('2024-06-15' AS DATE))"
    );
    assert!(matches!(result, Value::Int(2024) | Value::BigInt(2024)));

    let result = query_value(
        &mut engine,
        "SELECT DATEPART(month, CAST('2024-06-15' AS DATE))"
    );
    assert!(matches!(result, Value::Int(6) | Value::BigInt(6)));

    let result = query_value(
        &mut engine,
        "SELECT DATEPART(day, CAST('2024-06-15' AS DATE))"
    );
    assert!(matches!(result, Value::Int(15) | Value::BigInt(15)));
}

/// Test COALESCE/ISNULL consistency
#[test]
fn test_phase8_expression_null_handling() {
    let mut engine = Engine::new();

    // Simple COALESCE test
    assert_eq!(
        query_value(&mut engine, "SELECT COALESCE(NULL, 5)"),
        Value::Int(5)
    );
    assert_eq!(
        query_value(&mut engine, "SELECT COALESCE(10, 5)"),
        Value::Int(10)
    );

    // Simple ISNULL test
    assert_eq!(
        query_value(&mut engine, "SELECT ISNULL(NULL, 5)"),
        Value::Int(5)
    );
    assert_eq!(
        query_value(&mut engine, "SELECT ISNULL(10, 5)"),
        Value::Int(10)
    );
}

/// Test aggregate expression consistency
#[test]
fn test_phase8_expression_aggregates() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT, value INT)",
    );
    exec(
        &mut engine,
        "INSERT INTO t (id, value) VALUES (1, 10)",
    );
    exec(
        &mut engine,
        "INSERT INTO t (id, value) VALUES (2, 20)",
    );
    exec(
        &mut engine,
        "INSERT INTO t (id, value) VALUES (3, 30)",
    );
    exec(
        &mut engine,
        "INSERT INTO t (id, value) VALUES (4, NULL)",
    );

    // COUNT(*) - returns BigInt
    let result = query_value(&mut engine, "SELECT COUNT(*) FROM t");
    assert!(matches!(result, Value::BigInt(4)));

    // COUNT(column) ignores NULL
    let result = query_value(&mut engine, "SELECT COUNT(value) FROM t");
    assert!(matches!(result, Value::BigInt(3)));

    // SUM - may return BigInt or Decimal depending on implementation
    let result = query_value(&mut engine, "SELECT SUM(value) FROM t");
    match result {
        Value::BigInt(v) => assert_eq!(v, 60),
        Value::Decimal(raw, scale) => {
            let val = raw as f64 / 10_f64.powi(scale as i32);
            assert!((val - 60.0).abs() < 0.01);
        }
        Value::Int(v) => assert_eq!(v, 60),
        _ => panic!("Expected BigInt, Int, or Decimal for SUM, got {:?}", result),
    }

    // MIN/MAX
    let result = query_value(&mut engine, "SELECT MIN(value) FROM t");
    assert!(matches!(result, Value::Int(10) | Value::BigInt(10)));

    let result = query_value(&mut engine, "SELECT MAX(value) FROM t");
    assert!(matches!(result, Value::Int(30) | Value::BigInt(30)));
}

/// Test CAST/CONVERT consistency
#[test]
fn test_phase8_expression_type_conversion() {
    let mut engine = Engine::new();

    // INT to VARCHAR
    assert_eq!(
        query_value(&mut engine, "SELECT CAST(42 AS VARCHAR)"),
        Value::VarChar("42".to_string())
    );

    // VARCHAR to INT
    let result = query_value(&mut engine, "SELECT CAST('42' AS INT)");
    assert!(matches!(result, Value::Int(42) | Value::BigInt(42)));

    // DECIMAL to INT (truncation)
    let result = query_value(&mut engine, "SELECT CAST(42.9 AS INT)");
    assert!(matches!(result, Value::Int(42) | Value::BigInt(42)));

    // NULL conversion
    assert_eq!(
        query_value(&mut engine, "SELECT CAST(NULL AS INT)"),
        Value::Null
    );
}
