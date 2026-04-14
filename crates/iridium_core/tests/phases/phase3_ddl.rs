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

// ─── ALTER TABLE ───────────────────────────────────────────────────────

#[test]
fn test_alter_table_add_column() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT)");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    exec(&mut e, "INSERT INTO t VALUES (2)");
    exec(&mut e, "ALTER TABLE t ADD name VARCHAR(50) NULL");

    let r = query(&mut e, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert!(r.rows[0][1].is_null());
    assert!(r.rows[1][1].is_null());
}

#[test]
fn test_alter_table_add_column_then_insert() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT)");
    exec(&mut e, "ALTER TABLE t ADD name VARCHAR(50) NULL");
    exec(&mut e, "INSERT INTO t (id, name) VALUES (1, 'Alice')");

    let r = query(&mut e, "SELECT id, name FROM t");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
}

#[test]
fn test_alter_table_drop_column() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, name VARCHAR(50), age INT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 'Alice', 30)");
    exec(&mut e, "ALTER TABLE t DROP COLUMN age");

    let r = query(&mut e, "SELECT id, name FROM t");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.columns.len(), 2);
}

// ─── TRUNCATE TABLE ────────────────────────────────────────────────────

#[test]
fn test_truncate_table() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT)");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    exec(&mut e, "INSERT INTO t VALUES (2)");
    exec(&mut e, "INSERT INTO t VALUES (3)");

    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r.rows[0][0], Value::BigInt(3));

    exec(&mut e, "TRUNCATE TABLE t");

    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r.rows[0][0], Value::BigInt(0));
}

#[test]
fn test_truncate_then_reuse() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t (id INT IDENTITY(1,1), name VARCHAR(50))",
    );
    exec(&mut e, "INSERT INTO t (name) VALUES ('Alice')");
    exec(&mut e, "INSERT INTO t (name) VALUES ('Bob')");
    exec(&mut e, "TRUNCATE TABLE t");
    exec(&mut e, "INSERT INTO t (name) VALUES ('Charlie')");

    let r = query(&mut e, "SELECT id, name FROM t");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Value::VarChar("Charlie".to_string()));
}

