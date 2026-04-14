use chrono::NaiveDateTime;
use iridium_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

#[test]
fn test_coalesce_returns_first_non_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT COALESCE(NULL, NULL, 42, 99) AS v");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn test_coalesce_all_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT COALESCE(NULL, NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_coalesce_with_column() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (a VARCHAR(10), b VARCHAR(10))",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.t (a, b) VALUES (NULL, 'fallback')",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.t (a, b) VALUES ('primary', 'fallback')",
    );
    let r = query(
        &mut engine,
        "SELECT COALESCE(a, b) AS v FROM dbo.t ORDER BY v",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("fallback".to_string()));
    assert_eq!(r.rows[1][0], Value::VarChar("primary".to_string()));
}

#[test]
fn test_dateadd_hour() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEADD(hour, 3, '2025-01-01T10:00:00') AS v",
    );
    assert_eq!(
        r.rows[0][0],
        Value::DateTime(
            NaiveDateTime::parse_from_str("2025-01-01T13:00:00", "%Y-%m-%dT%H:%M:%S").unwrap()
        )
    );
}

#[test]
fn test_len_trims_trailing_spaces() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LEN('hello   ') AS v");
    assert_eq!(r.rows[0][0], Value::Int(5));
}

#[test]
fn test_len_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LEN(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_substring_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SUBSTRING('hello world', 7, 5) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("world".to_string()));
}

#[test]
fn test_substring_from_start() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SUBSTRING('abcdef', 1, 3) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("abc".to_string()));
}

#[test]
fn test_dateadd_month() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEADD(month, 1, '2025-01-15T00:00:00') AS v",
    );
    assert_eq!(
        r.rows[0][0],
        Value::DateTime(
            NaiveDateTime::parse_from_str("2025-02-15T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap()
        )
    );
}

#[test]
fn test_dateadd_day() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEADD(day, 1, '2025-01-01T00:00:00') AS v",
    );
    assert_eq!(
        r.rows[0][0],
        Value::DateTime(
            NaiveDateTime::parse_from_str("2025-01-02T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap()
        )
    );
}

#[test]
fn test_dateadd_day_from_date_literal() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DATEADD(day, 5, '2025-01-01') AS v");
    assert_eq!(
        r.rows[0][0],
        Value::DateTime(
            NaiveDateTime::parse_from_str("2025-01-06T00:00:00", "%Y-%m-%dT%H:%M:%S").unwrap()
        )
    );
}

#[test]
fn test_dateadd_year() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEADD(year, 2, '2023-06-15T12:00:00') AS v",
    );
    assert_eq!(
        r.rows[0][0],
        Value::DateTime(
            NaiveDateTime::parse_from_str("2025-06-15T12:00:00", "%Y-%m-%dT%H:%M:%S").unwrap()
        )
    );
}

#[test]
fn test_datediff_day() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEDIFF(day, '2025-01-01T00:00:00', '2025-01-10T00:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], Value::Int(9));
}

#[test]
fn test_datediff_month() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEDIFF(month, '2024-01-15T00:00:00', '2025-06-15T00:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], Value::Int(17));
}

#[test]
fn test_datediff_year() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEDIFF(year, '2020-06-15T00:00:00', '2025-06-15T00:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], Value::Int(5));
}

#[test]
fn test_datediff_hour() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEDIFF(hour, '2025-01-01T08:00:00', '2025-01-01T14:30:00') AS v",
    );
    assert_eq!(r.rows[0][0], Value::Int(6));
}

#[test]
fn test_sum_aggregate() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val INT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (10)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (20)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (30)");
    let r = query(&mut engine, "SELECT SUM(val) AS total FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::BigInt(60));
}

#[test]
fn test_avg_aggregate() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val INT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (10)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (20)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (30)");
    let r = query(&mut engine, "SELECT AVG(val) AS avg_val FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::Int(20));
}

#[test]
fn test_avg_decimal_returns_scale_six() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (val DECIMAL(12,2) NOT NULL)",
    );
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (85000.00)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (88750.00)");
    let r = query(&mut engine, "SELECT AVG(val) AS avg_val FROM dbo.t");
    assert_eq!(r.rows[0][0], Value::Decimal(86875000000, 6));
}

#[test]
fn test_decimal_literal_division_preserves_sql_scale() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT 5.0 / 2.0 AS v");
    assert_eq!(r.rows[0][0], Value::Decimal(2500000, 6));
}

#[test]
fn test_round_decimal_literal_preserves_input_scale() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ROUND(123.456, 2) AS v");
    assert_eq!(r.rows[0][0], Value::Decimal(123460, 3));
}

#[test]
fn test_cast_datetime_string_to_date() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CAST('2025-01-06 00:00:00' AS DATE) AS v",
    );
    assert_eq!(
        r.rows[0][0],
        Value::Date(chrono::NaiveDate::from_ymd_opt(2025, 1, 6).unwrap())
    );
}

#[test]
fn test_min_max_aggregate() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val INT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (10)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (30)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (20)");
    let r = query(
        &mut engine,
        "SELECT MIN(val) AS mn, MAX(val) AS mx FROM dbo.t",
    );
    assert_eq!(r.rows[0][0], Value::Int(10));
    assert_eq!(r.rows[0][1], Value::Int(30));
}

#[test]
fn test_sum_group_by() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (grp VARCHAR(10), val INT NOT NULL)",
    );
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('A', 10)");
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('A', 20)");
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('B', 5)");
    let r = query(
        &mut engine,
        "SELECT grp, SUM(val) AS total FROM dbo.t GROUP BY grp ORDER BY grp",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("A".to_string()));
    assert_eq!(r.rows[0][1], Value::BigInt(30));
    assert_eq!(r.rows[1][0], Value::VarChar("B".to_string()));
    assert_eq!(r.rows[1][1], Value::BigInt(5));
}

#[test]
fn test_having_basic() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (grp VARCHAR(10), val INT NOT NULL)",
    );
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('A', 10)");
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('A', 20)");
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('B', 5)");
    let r = query(
        &mut engine,
        "SELECT grp FROM dbo.t GROUP BY grp HAVING SUM(val) > 10 ORDER BY grp",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("A".to_string()));
}

#[test]
fn test_count_group_by() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (grp VARCHAR(10), val INT NOT NULL)",
    );
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('A', 1)");
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('A', 2)");
    exec(&mut engine, "INSERT INTO dbo.t (grp, val) VALUES ('B', 3)");
    let r = query(
        &mut engine,
        "SELECT grp, COUNT(*) AS cnt FROM dbo.t GROUP BY grp ORDER BY grp",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Value::BigInt(2));
    assert_eq!(r.rows[1][1], Value::BigInt(1));
}

#[test]
fn test_three_valued_logic_and() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (a INT, b INT)");
    exec(&mut engine, "INSERT INTO dbo.t (a, b) VALUES (1, NULL)");
    // NULL AND true -> NULL -> filtered out in WHERE
    let r = query(&mut engine, "SELECT a FROM dbo.t WHERE b = 1 AND a = 1");
    assert_eq!(r.rows.len(), 0);

    // NULL AND false -> false -> filtered out
    let r = query(&mut engine, "SELECT a FROM dbo.t WHERE b = 1 AND a = 2");
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn test_three_valued_logic_or() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (a INT, b INT)");
    exec(&mut engine, "INSERT INTO dbo.t (a, b) VALUES (1, NULL)");
    // NULL OR false = NULL -> filtered out in WHERE
    let r = query(&mut engine, "SELECT a FROM dbo.t WHERE b = 1 OR a = 2");
    assert_eq!(r.rows.len(), 0);

    // NULL OR true = true -> row should appear
    let r = query(&mut engine, "SELECT a FROM dbo.t WHERE b = 1 OR a = 1");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn test_isnull_with_new_types() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT ISNULL(CAST(NULL AS TINYINT), CAST(42 AS TINYINT)) AS v",
    );
    assert_eq!(r.rows[0][0], Value::TinyInt(42));
}

