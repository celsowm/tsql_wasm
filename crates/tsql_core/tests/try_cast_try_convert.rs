include!("new_functions/helpers.rs");

// ═══════════════════════════════════════════════════════════════════════════
// TRY_CAST
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_try_cast_valid() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CAST('123' AS INT) AS v");
    assert_eq!(r.rows[0][0], Value::Int(123));
}

#[test]
fn test_try_cast_invalid_returns_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CAST('abc' AS INT) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_try_cast_null_input() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CAST(NULL AS INT) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_try_cast_datetime_valid() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CAST('2025-01-15' AS DATETIME) AS v");
    assert!(!r.rows[0][0].is_null());
}

#[test]
fn test_try_cast_datetime_from_string() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CAST('2025-01-15T10:30:00' AS DATETIME) AS v");
    assert!(!r.rows[0][0].is_null());
}

#[test]
fn test_try_cast_overflow_returns_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CAST(99999999999 AS TINYINT) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_try_cast_in_case_expression() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CASE WHEN TRY_CAST('abc' AS INT) IS NULL THEN 'failed' ELSE 'ok' END AS v",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("failed".to_string()));
}

#[test]
fn test_try_cast_in_where() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val VARCHAR(10))");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES ('123')");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES ('abc')");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES ('456')");
    let r = query(
        &mut engine,
        "SELECT val FROM dbo.t WHERE TRY_CAST(val AS INT) IS NOT NULL ORDER BY TRY_CAST(val AS INT)",
    );
    assert_eq!(r.rows.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// TRY_CONVERT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_try_convert_valid() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CONVERT(INT, '456') AS v");
    assert_eq!(r.rows[0][0], Value::Int(456));
}

#[test]
fn test_try_convert_invalid_returns_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CONVERT(INT, 'xyz') AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_try_convert_with_style() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRY_CONVERT(VARCHAR, 123) AS v");
    assert!(!r.rows[0][0].is_null());
}
