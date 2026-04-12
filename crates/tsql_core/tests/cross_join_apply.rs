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

fn setup(engine: &mut Engine) {
    exec(
        engine,
        "CREATE TABLE employees (id INT, name VARCHAR(50), dept_id INT)",
    );
    exec(engine, "INSERT INTO employees VALUES (1, 'Alice', 10)");
    exec(engine, "INSERT INTO employees VALUES (2, 'Bob', 20)");
    exec(engine, "INSERT INTO employees VALUES (3, 'Charlie', 30)");

    exec(
        engine,
        "CREATE TABLE departments (id INT, dept_name VARCHAR(50))",
    );
    exec(engine, "INSERT INTO departments VALUES (10, 'Engineering')");
    exec(engine, "INSERT INTO departments VALUES (20, 'Marketing')");
    exec(engine, "INSERT INTO departments VALUES (40, 'Finance')");

    exec(
        engine,
        "CREATE TABLE orders (id INT, emp_id INT, amount INT)",
    );
    exec(engine, "INSERT INTO orders VALUES (1, 1, 100)");
    exec(engine, "INSERT INTO orders VALUES (2, 1, 200)");
    exec(engine, "INSERT INTO orders VALUES (3, 2, 50)");
}

// ─── CROSS JOIN ────────────────────────────────────────────────────────

#[test]
fn test_cross_join_basic() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE colors (c VARCHAR(10))");
    exec(&mut e, "INSERT INTO colors VALUES ('Red')");
    exec(&mut e, "INSERT INTO colors VALUES ('Blue')");

    exec(&mut e, "CREATE TABLE sizes (s VARCHAR(10))");
    exec(&mut e, "INSERT INTO sizes VALUES ('S')");
    exec(&mut e, "INSERT INTO sizes VALUES ('M')");
    exec(&mut e, "INSERT INTO sizes VALUES ('L')");

    let r = query(
        &mut e,
        "SELECT c.c, s.s FROM colors c CROSS JOIN sizes s ORDER BY c.c, s.s",
    );
    assert_eq!(r.rows.len(), 6); // 2 × 3 = 6
    assert_eq!(r.rows[0][0], Value::VarChar("Blue".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("L".to_string()));
}

#[test]
fn test_cross_join_empty_table() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE a (v INT)");
    exec(&mut e, "INSERT INTO a VALUES (1)");
    exec(&mut e, "INSERT INTO a VALUES (2)");
    exec(&mut e, "CREATE TABLE b (v INT)");

    let r = query(&mut e, "SELECT a.v, b.v FROM a CROSS JOIN b");
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn test_cross_join_single_row() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE a (v INT)");
    exec(&mut e, "INSERT INTO a VALUES (1)");
    exec(&mut e, "CREATE TABLE b (v INT)");
    exec(&mut e, "INSERT INTO b VALUES (10)");

    let r = query(&mut e, "SELECT a.v, b.v FROM a CROSS JOIN b");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Int(10));
}

// ─── CROSS APPLY ───────────────────────────────────────────────────────

#[test]
fn test_cross_apply_basic() {
    let mut e = Engine::new();
    setup(&mut e);

    let r = query(&mut e,
        "SELECT e.name, o.amount FROM employees e \
         CROSS APPLY (SELECT TOP 1 amount FROM orders WHERE orders.emp_id = e.id ORDER BY amount DESC) o \
         ORDER BY e.name"
    );
    // Alice has orders, Bob has orders, Charlie has none → 2 rows
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(200)); // top 1 desc → 200
    assert_eq!(r.rows[1][0], Value::VarChar("Bob".to_string()));
    assert_eq!(r.rows[1][1], Value::Int(50));
}

#[test]
fn test_cross_apply_no_match_excluded() {
    let mut e = Engine::new();
    setup(&mut e);

    let r = query(
        &mut e,
        "SELECT e.name, o.amount FROM employees e \
         CROSS APPLY (SELECT amount FROM orders WHERE orders.emp_id = e.id) o \
         ORDER BY e.name, o.amount",
    );
    // Alice: 2 orders, Bob: 1 order, Charlie: 0 → 3 rows total
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn test_cross_apply_join_group() {
    let mut e = Engine::new();
    setup(&mut e);

    let r = query(
        &mut e,
        "SELECT e.name, x.amount, x.dept_name FROM employees e \
         CROSS APPLY (orders o INNER JOIN departments d ON d.id = e.dept_id AND o.emp_id = e.id) x \
         ORDER BY e.name, x.amount",
    );

    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(100));
    assert_eq!(r.rows[0][2], Value::VarChar("Engineering".to_string()));
    assert_eq!(r.rows[1][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[1][1], Value::Int(200));
    assert_eq!(r.rows[1][2], Value::VarChar("Engineering".to_string()));
    assert_eq!(r.rows[2][0], Value::VarChar("Bob".to_string()));
    assert_eq!(r.rows[2][1], Value::Int(50));
    assert_eq!(r.rows[2][2], Value::VarChar("Marketing".to_string()));
}

// ─── OUTER APPLY ───────────────────────────────────────────────────────

#[test]
fn test_outer_apply_basic() {
    let mut e = Engine::new();
    setup(&mut e);

    let r = query(&mut e,
        "SELECT e.name, o.amount FROM employees e \
         OUTER APPLY (SELECT TOP 1 amount FROM orders WHERE orders.emp_id = e.id ORDER BY amount DESC) o \
         ORDER BY e.name"
    );
    // All 3 employees appear; Charlie has NULL amount
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(200));
    assert_eq!(r.rows[2][0], Value::VarChar("Charlie".to_string()));
    assert_eq!(r.rows[2][1], Value::Null);
}

#[test]
fn test_outer_apply_all_match() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t1 (id INT)");
    exec(&mut e, "INSERT INTO t1 VALUES (1)");
    exec(&mut e, "INSERT INTO t1 VALUES (2)");
    exec(&mut e, "CREATE TABLE t2 (pid INT, val INT)");
    exec(&mut e, "INSERT INTO t2 VALUES (1, 10)");
    exec(&mut e, "INSERT INTO t2 VALUES (2, 20)");

    let r = query(
        &mut e,
        "SELECT t1.id, x.val FROM t1 \
         OUTER APPLY (SELECT val FROM t2 WHERE t2.pid = t1.id) x \
         ORDER BY t1.id",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Value::Int(10));
    assert_eq!(r.rows[1][1], Value::Int(20));
}

#[test]
fn test_outer_apply_multiple_rows() {
    let mut e = Engine::new();
    setup(&mut e);

    let r = query(
        &mut e,
        "SELECT e.name, o.amount FROM employees e \
         OUTER APPLY (SELECT amount FROM orders WHERE orders.emp_id = e.id) o \
         ORDER BY e.name, o.amount",
    );
    // Alice: 2 rows (100, 200), Bob: 1 row (50), Charlie: 1 row (NULL) → 4 rows
    assert_eq!(r.rows.len(), 4);
}
