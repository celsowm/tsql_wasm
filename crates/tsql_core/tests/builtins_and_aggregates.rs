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
fn test_coalesce_returns_first_non_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT COALESCE(NULL, NULL, 42, 99) AS v");
    assert_eq!(r.rows[0][0], serde_json::json!(42));
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
    assert_eq!(r.rows[0][0], serde_json::json!("fallback"));
    assert_eq!(r.rows[1][0], serde_json::json!("primary"));
}

#[test]
fn test_dateadd_hour() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEADD(hour, 3, '2025-01-01T10:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("2025-01-01T13:00:00"));
}

#[test]
fn test_len_trims_trailing_spaces() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LEN('hello   ') AS v");
    assert_eq!(r.rows[0][0], serde_json::json!(5));
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
    assert_eq!(r.rows[0][0], serde_json::json!("world"));
}

#[test]
fn test_substring_from_start() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SUBSTRING('abcdef', 1, 3) AS v");
    assert_eq!(r.rows[0][0], serde_json::json!("abc"));
}

#[test]
fn test_dateadd_month() {
    let mut engine = Engine::new();
    // Adding month to Jan 15 should give Feb 15
    let r = query(
        &mut engine,
        "SELECT DATEADD(month, 1, '2025-01-15T00:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("2025-02-15T00:00:00"));
}

#[test]
fn test_dateadd_day() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEADD(day, 1, '2025-01-01T00:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("2025-01-02T00:00:00"));
}

#[test]
fn test_dateadd_year() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEADD(year, 2, '2023-06-15T12:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("2025-06-15T12:00:00"));
}

#[test]
fn test_datediff_day() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEDIFF(day, '2025-01-01T00:00:00', '2025-01-10T00:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(9));
}

#[test]
fn test_datediff_month() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEDIFF(month, '2024-01-15T00:00:00', '2025-06-15T00:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(17));
}

#[test]
fn test_datediff_year() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEDIFF(year, '2020-06-15T00:00:00', '2025-06-15T00:00:00') AS v",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(5));
}

#[test]
fn test_datediff_hour() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DATEDIFF(hour, '2025-01-01T08:00:00', '2025-01-01T14:30:00') AS v",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(6));
}

#[test]
fn test_sum_aggregate() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val INT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (10)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (20)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (30)");
    let r = query(&mut engine, "SELECT SUM(val) AS total FROM dbo.t");
    assert_eq!(r.rows[0][0], serde_json::json!(60));
}

#[test]
fn test_avg_aggregate() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val INT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (10)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (20)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (30)");
    let r = query(&mut engine, "SELECT AVG(val) AS avg_val FROM dbo.t");
    assert_eq!(r.rows[0][0], serde_json::json!("20"));
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
    assert_eq!(r.rows[0][0], serde_json::json!(10));
    assert_eq!(r.rows[0][1], serde_json::json!(30));
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
    assert_eq!(r.rows[0][0], serde_json::json!("A"));
    assert_eq!(r.rows[0][1], serde_json::json!(30));
    assert_eq!(r.rows[1][0], serde_json::json!("B"));
    assert_eq!(r.rows[1][1], serde_json::json!(5));
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
        "SELECT grp, SUM(val) AS total FROM dbo.t GROUP BY grp HAVING SUM(val) > 10 ORDER BY grp",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!("A"));
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
    assert_eq!(r.rows[0][1], serde_json::json!(2));
    assert_eq!(r.rows[1][1], serde_json::json!(1));
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
    assert_eq!(r.rows[0][0], serde_json::json!(42));
}
