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

// ─── INSERT ... SELECT ─────────────────────────────────────────────────

#[test]
fn test_insert_select_basic() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE src (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE TABLE dst (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
    exec(&mut e, "INSERT INTO dst SELECT id, name FROM src WHERE id > 1");
    let r = query(&mut e, "SELECT * FROM dst ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(2));
    assert_eq!(r.rows[0][1], Value::VarChar("Bob".to_string()));
    assert_eq!(r.rows[1][0], Value::Int(3));
}

#[test]
fn test_insert_select_with_columns() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE src (a INT, b VARCHAR(10))");
    exec(&mut e, "CREATE TABLE dst (x INT, y VARCHAR(10))");
    exec(&mut e, "INSERT INTO src VALUES (1, 'hello'), (2, 'world')");
    exec(&mut e, "INSERT INTO dst (x, y) SELECT a, b FROM src");
    let r = query(&mut e, "SELECT * FROM dst ORDER BY x");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("hello".to_string()));
}

#[test]
fn test_insert_select_identity_preserved() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE src (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(50))",
    );
    exec(
        &mut e,
        "CREATE TABLE dst (id INT IDENTITY(100,1) PRIMARY KEY, name VARCHAR(50))",
    );
    exec(&mut e, "INSERT INTO src (name) VALUES ('Alice'), ('Bob')");
    exec(&mut e, "INSERT INTO dst (name) SELECT name FROM src");
    let r = query(&mut e, "SELECT id, name FROM dst ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(100));
    assert_eq!(r.rows[1][0], Value::Int(101));
}

// ─── OFFSET / FETCH ────────────────────────────────────────────────────

#[test]
fn test_offset_fetch_full_syntax() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT)");
    exec(&mut e, "INSERT INTO t VALUES (1), (2), (3), (4), (5)");
    let r = query(
        &mut e,
        "SELECT id FROM t ORDER BY id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(3));
    assert_eq!(r.rows[1][0], Value::Int(4));
}

#[test]
fn test_offset_only() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT)");
    exec(&mut e, "INSERT INTO t VALUES (1), (2), (3), (4), (5)");
    let r = query(&mut e, "SELECT id FROM t ORDER BY id OFFSET 3 ROWS");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(4));
    assert_eq!(r.rows[1][0], Value::Int(5));
}

#[test]
fn test_offset_zero() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT)");
    exec(&mut e, "INSERT INTO t VALUES (1), (2), (3)");
    let r = query(
        &mut e,
        "SELECT id FROM t ORDER BY id OFFSET 0 ROWS FETCH NEXT 2 ROWS ONLY",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[1][0], Value::Int(2));
}

#[test]
fn test_offset_beyond_results() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT)");
    exec(&mut e, "INSERT INTO t VALUES (1), (2)");
    let r = query(&mut e, "SELECT id FROM t ORDER BY id OFFSET 10 ROWS");
    assert_eq!(r.rows.len(), 0);
}

// ─── OUTPUT clause ─────────────────────────────────────────────────────

#[test]
fn test_output_update() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, val INT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 10)");
    let r = query(
        &mut e,
        "UPDATE t SET val = 100 OUTPUT INSERTED.id, INSERTED.val, DELETED.val WHERE id = 1",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Int(100));
    assert_eq!(r.rows[0][2], Value::Int(10));
}

#[test]
fn test_output_update_multiple_rows() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, val INT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = query(
        &mut e,
        "UPDATE t SET val = val * 2 OUTPUT INSERTED.id, INSERTED.val, DELETED.val WHERE id < 10",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[1][0], Value::Int(2));
}

#[test]
fn test_output_delete() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO t VALUES (1, 'Alice')");
    let r = query(
        &mut e,
        "DELETE FROM t OUTPUT DELETED.id, DELETED.name WHERE id = 1",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
    let r2 = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r2.rows[0][0], Value::BigInt(0));
}

#[test]
fn test_output_delete_multiple_rows() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')");
    let r = query(
        &mut e,
        "DELETE FROM t OUTPUT DELETED.id, DELETED.name",
    );
    assert_eq!(r.rows.len(), 2);
}
