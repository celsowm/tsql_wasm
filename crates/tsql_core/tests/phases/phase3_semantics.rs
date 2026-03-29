use tsql_core::{parse_sql, Engine};

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
fn test_mixed_numeric_string_comparison() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec(&mut e, "INSERT INTO t VALUES (10)");
    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t WHERE v = '10'");
    assert_eq!(r.rows[0][0].to_string_value(), "1");
}

#[test]
fn test_mixed_date_datetime_comparison() {
    let mut e = Engine::new();
    let r = query(
        &mut e,
        "SELECT CASE WHEN CAST('2026-03-21' AS DATE) = CAST('2026-03-21' AS DATETIME) THEN 1 ELSE 0 END AS eq",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "1");
}

#[test]
fn test_string_truncation_error() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (name VARCHAR(3))");
    let err = e
        .execute(parse_sql("INSERT INTO t VALUES ('ABCDE')").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("truncated"));
}

#[test]
fn test_overflow_error_tinyint() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v TINYINT)");
    let err = e
        .execute(parse_sql("INSERT INTO t VALUES (300)").unwrap())
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("overflow"));
}

#[test]
fn test_invalid_numeric_conversion_error() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    let err = e
        .execute(parse_sql("INSERT INTO t VALUES ('abc')").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("cannot convert"));
}

#[test]
fn test_computed_column_insert_and_select() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (a INT, b INT, total AS (a + b))");
    exec(&mut e, "INSERT INTO t (a, b) VALUES (2, 3)");
    let r = query(&mut e, "SELECT total FROM t");
    assert_eq!(r.rows[0][0].to_string_value(), "5");
}

#[test]
fn test_computed_column_cannot_be_inserted_directly() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (a INT, b INT, total AS (a + b))");
    let err = e
        .execute(parse_sql("INSERT INTO t (a, b, total) VALUES (2, 3, 10)").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("computed column"));
}

#[test]
fn test_computed_column_recomputes_on_update() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (a INT, b INT, total AS (a + b))");
    exec(&mut e, "INSERT INTO t (a, b) VALUES (2, 3)");
    exec(&mut e, "UPDATE t SET a = 10");
    let r = query(&mut e, "SELECT total FROM t");
    assert_eq!(r.rows[0][0].to_string_value(), "13");
}
