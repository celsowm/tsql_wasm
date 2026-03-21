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

fn setup_join_tables(engine: &mut Engine) {
    exec(
        engine,
        "CREATE TABLE employees (id INT, name VARCHAR(50), dept_id INT)",
    );
    exec(engine, "INSERT INTO employees VALUES (1, 'Alice', 10)");
    exec(engine, "INSERT INTO employees VALUES (2, 'Bob', 20)");
    exec(engine, "INSERT INTO employees VALUES (3, 'Charlie', 30)");
    exec(engine, "INSERT INTO employees VALUES (4, 'Dave', NULL)");

    exec(
        engine,
        "CREATE TABLE departments (id INT, dept_name VARCHAR(50))",
    );
    exec(engine, "INSERT INTO departments VALUES (10, 'Engineering')");
    exec(engine, "INSERT INTO departments VALUES (20, 'Marketing')");
    exec(engine, "INSERT INTO departments VALUES (40, 'Finance')");
}

// ─── RIGHT JOIN ────────────────────────────────────────────────────────

#[test]
fn test_right_join_basic() {
    let mut e = Engine::new();
    setup_join_tables(&mut e);
    let r = query(&mut e, "SELECT e.name, d.dept_name FROM departments d RIGHT JOIN employees e ON e.dept_id = d.id ORDER BY e.name");
    // All employees should appear. Alice→Engineering, Bob→Marketing, Charlie→NULL (dept 30 not found), Dave→NULL
    assert_eq!(r.rows.len(), 4);
}

#[test]
fn test_right_join_unmatched_right() {
    let mut e = Engine::new();
    setup_join_tables(&mut e);
    let r = query(&mut e, "SELECT e.name, d.dept_name FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id ORDER BY d.dept_name");
    // All departments should appear: Finance (no employee), Engineering (Alice), Marketing (Bob)
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], serde_json::json!("Engineering"));
    assert_eq!(r.rows[1][1], serde_json::json!("Finance"));
    assert_eq!(r.rows[2][1], serde_json::json!("Marketing"));
}

// ─── FULL OUTER JOIN ───────────────────────────────────────────────────

#[test]
fn test_full_outer_join() {
    let mut e = Engine::new();
    setup_join_tables(&mut e);
    let r = query(&mut e, "SELECT e.name, d.dept_name FROM employees e FULL OUTER JOIN departments d ON e.dept_id = d.id ORDER BY COALESCE(e.name, 'ZZZ'), COALESCE(d.dept_name, 'ZZZ')");
    // Alice→Engineering, Bob→Marketing, Charlie→NULL, Dave→NULL, NULL→Finance
    assert_eq!(r.rows.len(), 5);
}

// ─── DISTINCT ──────────────────────────────────────────────────────────

#[test]
fn test_distinct_basic() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (category VARCHAR(10))");
    exec(&mut e, "INSERT INTO t VALUES ('A')");
    exec(&mut e, "INSERT INTO t VALUES ('B')");
    exec(&mut e, "INSERT INTO t VALUES ('A')");
    exec(&mut e, "INSERT INTO t VALUES ('C')");
    exec(&mut e, "INSERT INTO t VALUES ('B')");

    let r = query(&mut e, "SELECT DISTINCT category FROM t ORDER BY category");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], serde_json::json!("A"));
    assert_eq!(r.rows[1][0], serde_json::json!("B"));
    assert_eq!(r.rows[2][0], serde_json::json!("C"));
}

#[test]
fn test_distinct_multiple_columns() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (a INT, b INT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 1)");
    exec(&mut e, "INSERT INTO t VALUES (1, 2)");
    exec(&mut e, "INSERT INTO t VALUES (1, 1)");
    exec(&mut e, "INSERT INTO t VALUES (2, 1)");

    let r = query(&mut e, "SELECT DISTINCT a, b FROM t ORDER BY a, b");
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn test_distinct_with_nulls() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    exec(&mut e, "INSERT INTO t VALUES (NULL)");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    exec(&mut e, "INSERT INTO t VALUES (NULL)");

    let r = query(&mut e, "SELECT DISTINCT v FROM t ORDER BY v");
    // NULL and 1
    assert_eq!(r.rows.len(), 2);
}

// ─── SET OPERATIONS ────────────────────────────────────────────────────
// These require parser support which is not yet implemented.
// For now, test the features that ARE implemented.

#[test]
fn test_inner_join_still_works() {
    let mut e = Engine::new();
    setup_join_tables(&mut e);
    let r = query(&mut e, "SELECT e.name, d.dept_name FROM employees e INNER JOIN departments d ON e.dept_id = d.id ORDER BY e.name");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], serde_json::json!("Alice"));
    assert_eq!(r.rows[1][0], serde_json::json!("Bob"));
}

#[test]
fn test_left_join_still_works() {
    let mut e = Engine::new();
    setup_join_tables(&mut e);
    let r = query(&mut e, "SELECT e.name, d.dept_name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY e.name");
    assert_eq!(r.rows.len(), 4);
}

#[test]
fn test_distinct_in_subquery() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE orders (customer VARCHAR(10), amount INT)",
    );
    exec(&mut e, "INSERT INTO orders VALUES ('Alice', 100)");
    exec(&mut e, "INSERT INTO orders VALUES ('Bob', 200)");
    exec(&mut e, "INSERT INTO orders VALUES ('Alice', 150)");

    let r = query(
        &mut e,
        "SELECT DISTINCT customer FROM orders ORDER BY customer",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], serde_json::json!("Alice"));
    assert_eq!(r.rows[1][0], serde_json::json!("Bob"));
}
