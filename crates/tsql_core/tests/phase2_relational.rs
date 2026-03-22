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
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Value::VarChar("Engineering".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("Finance".to_string()));
    assert_eq!(r.rows[2][1], Value::VarChar("Marketing".to_string()));
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
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[1][0], Value::VarChar("Bob".to_string()));
}

// ─── SET OPERATIONS ────────────────────────────────────────────────────

#[test]
fn test_union_all() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE a (v INT)");
    exec(&mut e, "INSERT INTO a VALUES (1)");
    exec(&mut e, "INSERT INTO a VALUES (2)");
    exec(&mut e, "CREATE TABLE b (v INT)");
    exec(&mut e, "INSERT INTO b VALUES (2)");
    exec(&mut e, "INSERT INTO b VALUES (3)");

    let r = query(&mut e, "SELECT v FROM a UNION ALL SELECT v FROM b");
    assert_eq!(r.rows.len(), 4);
}

#[test]
fn test_union() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE a (v INT)");
    exec(&mut e, "INSERT INTO a VALUES (1)");
    exec(&mut e, "INSERT INTO a VALUES (2)");
    exec(&mut e, "CREATE TABLE b (v INT)");
    exec(&mut e, "INSERT INTO b VALUES (2)");
    exec(&mut e, "INSERT INTO b VALUES (3)");

    let r = query(&mut e, "SELECT v FROM a UNION SELECT v FROM b");
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn test_intersect() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE a (v INT)");
    exec(&mut e, "INSERT INTO a VALUES (1)");
    exec(&mut e, "INSERT INTO a VALUES (2)");
    exec(&mut e, "INSERT INTO a VALUES (3)");
    exec(&mut e, "CREATE TABLE b (v INT)");
    exec(&mut e, "INSERT INTO b VALUES (2)");
    exec(&mut e, "INSERT INTO b VALUES (3)");
    exec(&mut e, "INSERT INTO b VALUES (4)");

    let r = query(&mut e, "SELECT v FROM a INTERSECT SELECT v FROM b");
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn test_except() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE a (v INT)");
    exec(&mut e, "INSERT INTO a VALUES (1)");
    exec(&mut e, "INSERT INTO a VALUES (2)");
    exec(&mut e, "INSERT INTO a VALUES (3)");
    exec(&mut e, "CREATE TABLE b (v INT)");
    exec(&mut e, "INSERT INTO b VALUES (2)");

    let r = query(&mut e, "SELECT v FROM a EXCEPT SELECT v FROM b");
    assert_eq!(r.rows.len(), 2);
}

// ─── CTEs ──────────────────────────────────────────────────────────────

#[test]
fn test_cte_basic() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE employees (name VARCHAR(50), salary INT)",
    );
    exec(&mut e, "INSERT INTO employees VALUES ('Alice', 100)");
    exec(&mut e, "INSERT INTO employees VALUES ('Bob', 200)");
    exec(&mut e, "INSERT INTO employees VALUES ('Charlie', 150)");

    let r = query(&mut e, "WITH high_earners AS (SELECT name, salary FROM employees WHERE salary >= 150) SELECT name FROM high_earners ORDER BY name");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("Bob".to_string()));
    assert_eq!(r.rows[1][0], Value::VarChar("Charlie".to_string()));
}

#[test]
fn test_cte_with_join() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE orders (id INT, customer_id INT, amount INT)",
    );
    exec(&mut e, "INSERT INTO orders VALUES (1, 1, 100)");
    exec(&mut e, "INSERT INTO orders VALUES (2, 1, 200)");
    exec(&mut e, "INSERT INTO orders VALUES (3, 2, 50)");
    exec(&mut e, "CREATE TABLE customers (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO customers VALUES (1, 'Alice')");
    exec(&mut e, "INSERT INTO customers VALUES (2, 'Bob')");

    let r = query(&mut e, "WITH order_totals AS (SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id) SELECT c.name, ot.total FROM customers c INNER JOIN order_totals ot ON c.id = ot.customer_id ORDER BY c.name");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
}

#[test]
fn test_multiple_ctes() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (v INT)");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    exec(&mut e, "INSERT INTO t VALUES (2)");
    exec(&mut e, "INSERT INTO t VALUES (3)");

    let r = query(&mut e, "WITH doubled AS (SELECT v, v * 2 AS v2 FROM t), tripled AS (SELECT v, v * 3 AS v3 FROM t) SELECT d.v, d.v2, t.v3 FROM doubled d INNER JOIN tripled t ON d.v = t.v ORDER BY d.v");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Value::BigInt(2));
    assert_eq!(r.rows[0][2], Value::BigInt(3));
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
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[1][0], Value::VarChar("Bob".to_string()));
}

#[test]
fn test_left_join_still_works() {
    let mut e = Engine::new();
    setup_join_tables(&mut e);
    let r = query(&mut e, "SELECT e.name, d.dept_name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ORDER BY e.name");
    assert_eq!(r.rows.len(), 4);
}

// ─── SET OPERATIONS ────────────────────────────────────────────────────
