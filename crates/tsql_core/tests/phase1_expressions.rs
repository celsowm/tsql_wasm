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

fn setup(engine: &mut Engine) {
    exec(
        engine,
        "CREATE TABLE t (id INT IDENTITY(1,1), name VARCHAR(50), val INT, price DECIMAL(10,2))",
    );
    exec(
        engine,
        "INSERT INTO t (name, val, price) VALUES ('Alice', 10, 1.50)",
    );
    exec(
        engine,
        "INSERT INTO t (name, val, price) VALUES ('Bob', 20, 2.75)",
    );
    exec(
        engine,
        "INSERT INTO t (name, val, price) VALUES ('Charlie', 30, 3.00)",
    );
    exec(
        engine,
        "INSERT INTO t (name, val, price) VALUES ('Dave', NULL, 4.25)",
    );
}

// ─── Arithmetic operators ──────────────────────────────────────────────

#[test]
fn test_arithmetic_add() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT val + 5 AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(15));
}

#[test]
fn test_arithmetic_subtract() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT val - 3 AS result FROM t WHERE name = 'Bob'");
    assert_eq!(r.rows[0][0], serde_json::json!(17));
}

#[test]
fn test_arithmetic_multiply() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT val * 2 AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(20));
}

#[test]
fn test_arithmetic_divide() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT val / 2 AS result FROM t WHERE name = 'Bob'");
    assert_eq!(r.rows[0][0], serde_json::json!(10));
}

#[test]
fn test_arithmetic_divide_by_zero_returns_null() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT val / 0 AS result FROM t WHERE name = 'Alice'",
    );
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_arithmetic_modulo() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT val % 3 AS result FROM t WHERE name = 'Bob'");
    assert_eq!(r.rows[0][0], serde_json::json!(2));
}

#[test]
fn test_in_with_arithmetic() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE x (v INT)");
    exec(&mut e, "INSERT INTO x VALUES (10)");
    exec(&mut e, "INSERT INTO x VALUES (20)");
    exec(&mut e, "INSERT INTO x VALUES (30)");

    // v+10 = 20, 30, 40
    // IN (20, 30) should match 20 and 30 only
    let r = query(
        &mut e,
        "SELECT v, v + 10 AS v10 FROM x WHERE v + 10 IN (20, 30) ORDER BY v",
    );
    for row in &r.rows {
        eprintln!("result: {:?}", row);
    }
    assert_eq!(
        r.rows.len(),
        2,
        "should match v=10 (v+10=20) and v=20 (v+10=30), not v=30 (v+10=40)"
    );
    assert_eq!(r.rows[0][0], serde_json::json!(10));
    assert_eq!(r.rows[1][0], serde_json::json!(20));
}

#[test]
fn test_arithmetic_precedence_paren() {
    let mut e = Engine::new();
    setup(&mut e);
    // (2 + 3) * 4 = 20
    let r = query(&mut e, "SELECT (2 + 3) * 4 AS result");
    assert_eq!(r.rows[0][0], serde_json::json!(20));
}

#[test]
fn test_arithmetic_null_propagation() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT val + 1 AS result FROM t WHERE name = 'Dave'",
    );
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_arithmetic_column_expression() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT val * val AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(100));
}

// ─── CASE expression ───────────────────────────────────────────────────

#[test]
fn test_case_simple() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT CASE val WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END AS result FROM t WHERE name = 'Alice'");
    assert_eq!(r.rows[0][0], serde_json::json!("ten"));
}

#[test]
fn test_case_searched() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT CASE WHEN val < 15 THEN 'low' WHEN val < 25 THEN 'mid' ELSE 'high' END AS result FROM t WHERE name = 'Bob'");
    assert_eq!(r.rows[0][0], serde_json::json!("mid"));
}

#[test]
fn test_case_else_null() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT CASE WHEN val = 999 THEN 'found' END AS result FROM t WHERE name = 'Alice'",
    );
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_case_in_where() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE CASE WHEN val > 15 THEN 1 ELSE 0 END = 1 ORDER BY name",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], serde_json::json!("Bob"));
    assert_eq!(r.rows[1][0], serde_json::json!("Charlie"));
}

// ─── IN / NOT IN ──────────────────────────────────────────────────────

#[test]
fn test_in_list() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE val IN (10, 30) ORDER BY name",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], serde_json::json!("Alice"));
    assert_eq!(r.rows[1][0], serde_json::json!("Charlie"));
}

#[test]
fn test_not_in_list() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE val NOT IN (10, 20) ORDER BY name",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!("Charlie"));
}

#[test]
fn test_in_with_strings() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE name IN ('Alice', 'Dave') ORDER BY name",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], serde_json::json!("Alice"));
    assert_eq!(r.rows[1][0], serde_json::json!("Dave"));
}

// ─── BETWEEN / NOT BETWEEN ────────────────────────────────────────────

#[test]
fn test_between() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE val BETWEEN 10 AND 20 ORDER BY name",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], serde_json::json!("Alice"));
    assert_eq!(r.rows[1][0], serde_json::json!("Bob"));
}

#[test]
fn test_not_between() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT name FROM t WHERE val NOT BETWEEN 10 AND 20");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!("Charlie"));
}

#[test]
fn test_between_with_expression() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE val BETWEEN 5 + 5 AND 10 * 2 ORDER BY name",
    );
    assert_eq!(r.rows.len(), 2);
}

// ─── LIKE ─────────────────────────────────────────────────────────────

#[test]
fn test_like_percent() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE name LIKE 'A%' ORDER BY name",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!("Alice"));
}

#[test]
fn test_like_underscore() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT name FROM t WHERE name LIKE 'B_b'");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!("Bob"));
}

#[test]
fn test_like_contains() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE name LIKE '%li%' ORDER BY name",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], serde_json::json!("Alice"));
    assert_eq!(r.rows[1][0], serde_json::json!("Charlie"));
}

#[test]
fn test_not_like() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name FROM t WHERE name NOT LIKE 'A%' ORDER BY name",
    );
    assert_eq!(r.rows.len(), 3);
}

// ─── Unary operators ──────────────────────────────────────────────────

#[test]
fn test_unary_negate() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT -val AS result FROM t WHERE name = 'Alice'");
    assert_eq!(r.rows[0][0], serde_json::json!(-10));
}

#[test]
fn test_unary_not() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t2 (flag BIT)");
    exec(&mut e, "INSERT INTO t2 VALUES (1)");
    exec(&mut e, "INSERT INTO t2 VALUES (0)");
    let r = query(&mut e, "SELECT flag FROM t2 WHERE NOT flag");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], serde_json::json!(false));
}

#[test]
fn test_double_negate() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT --val AS result FROM t WHERE name = 'Alice'");
    assert_eq!(r.rows[0][0], serde_json::json!(10));
}

// ─── String concatenation with + ──────────────────────────────────────

#[test]
fn test_string_concat() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name + '!' AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("Alice!"));
}

#[test]
fn test_string_concat_columns() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT name + ' has ' + CAST(val AS VARCHAR) AS result FROM t WHERE name = 'Bob'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("Bob has 20"));
}

// ─── New built-in functions ───────────────────────────────────────────

#[test]
fn test_upper() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT UPPER(name) AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("ALICE"));
}

#[test]
fn test_lower() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT LOWER(name) AS result FROM t WHERE name = 'Bob'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("bob"));
}

#[test]
fn test_ltrim_rtrim() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t3 (s VARCHAR(50))");
    exec(&mut e, "INSERT INTO t3 VALUES ('  hello  ')");
    let r = query(&mut e, "SELECT LTRIM(s) AS result FROM t3");
    assert_eq!(r.rows[0][0], serde_json::json!("hello  "));
    let r = query(&mut e, "SELECT RTRIM(s) AS result FROM t3");
    assert_eq!(r.rows[0][0], serde_json::json!("  hello"));
}

#[test]
fn test_trim() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t3 (s VARCHAR(50))");
    exec(&mut e, "INSERT INTO t3 VALUES ('  hello  ')");
    let r = query(&mut e, "SELECT TRIM(s) AS result FROM t3");
    assert_eq!(r.rows[0][0], serde_json::json!("hello"));
}

#[test]
fn test_replace() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT REPLACE(name, 'li', 'XX') AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!("AXXce"));
}

#[test]
fn test_abs() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT ABS(-val) AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(10));
}

#[test]
fn test_charindex() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT CHARINDEX('li', name) AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(2));
}

#[test]
fn test_charindex_not_found() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(
        &mut e,
        "SELECT CHARINDEX('xyz', name) AS result FROM t WHERE name = 'Alice'",
    );
    assert_eq!(r.rows[0][0], serde_json::json!(0));
}

#[test]
fn test_current_timestamp() {
    let mut e = Engine::new();
    let r = query(&mut e, "SELECT CURRENT_TIMESTAMP AS result");
    assert!(!r.rows[0][0].is_null());
}

// ─── Complex combinations ────────────────────────────────────────────

#[test]
fn test_between_in_case() {
    let mut e = Engine::new();
    setup(&mut e);
    let r = query(&mut e, "SELECT CASE WHEN val BETWEEN 10 AND 15 THEN 'range1' WHEN val BETWEEN 20 AND 25 THEN 'range2' ELSE 'other' END AS result FROM t WHERE name = 'Charlie'");
    assert_eq!(r.rows[0][0], serde_json::json!("other"));
}
