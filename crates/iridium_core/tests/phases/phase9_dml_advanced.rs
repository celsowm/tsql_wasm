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

// ─── INSERT OUTPUT ──────────────────────────────────────────────────────

#[test]
fn test_output_insert_values() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, name VARCHAR(50))");
    let r = query(
        &mut e,
        "INSERT INTO t OUTPUT INSERTED.id, INSERTED.name VALUES (1, 'Alice')",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
}

#[test]
fn test_output_insert_multi_row() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, val INT)");
    let r = query(
        &mut e,
        "INSERT INTO t OUTPUT INSERTED.id, INSERTED.val VALUES (1, 10), (2, 20), (3, 30)",
    );
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[2][0], Value::Int(3));
}

#[test]
fn test_output_insert_select() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE src (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE TABLE dst (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob')");
    let r = query(
        &mut e,
        "INSERT INTO dst OUTPUT INSERTED.id, INSERTED.name SELECT id, name FROM src",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[1][0], Value::Int(2));
}

#[test]
fn test_output_insert_identity() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(50))",
    );
    let r = query(
        &mut e,
        "INSERT INTO t (name) OUTPUT INSERTED.id, INSERTED.name VALUES ('Alice'), ('Bob')",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[1][1], Value::VarChar("Bob".to_string()));
}

#[test]
fn test_output_insert_wildcard() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, name VARCHAR(50))");
    let r = query(
        &mut e,
        "INSERT INTO t OUTPUT INSERTED.* VALUES (1, 'Alice')",
    );
    assert_eq!(r.columns.len(), 2);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
}

// ─── OUTPUT wildcard ────────────────────────────────────────────────────

#[test]
fn test_output_update_wildcard() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, val INT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 10)");
    let r = query(
        &mut e,
        "UPDATE t SET val = 100 OUTPUT INSERTED.*, DELETED.* WHERE id = 1",
    );
    assert_eq!(r.columns.len(), 4); // INSERTED.id, INSERTED.val, DELETED.id, DELETED.val
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));   // INSERTED.id
    assert_eq!(r.rows[0][1], Value::Int(100));  // INSERTED.val
    assert_eq!(r.rows[0][2], Value::Int(1));    // DELETED.id
    assert_eq!(r.rows[0][3], Value::Int(10));   // DELETED.val
}

#[test]
fn test_output_delete_wildcard() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')");
    let r = query(
        &mut e,
        "DELETE FROM t OUTPUT DELETED.* WHERE id = 1",
    );
    assert_eq!(r.columns.len(), 2); // DELETED.id, DELETED.name
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
}

// ─── MERGE OUTPUT ───────────────────────────────────────────────────────

#[test]
fn test_output_merge_insert() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE target (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE TABLE source (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO target VALUES (1, 'Alice')");
    exec(&mut e, "INSERT INTO source VALUES (1, 'Alice Updated'), (2, 'Bob')");
    let r = query(
        &mut e,
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) \
         OUTPUT INSERTED.id, INSERTED.name",
    );
    // Actually our MERGE executor seems to return OUTPUT for all source rows processed?
    // Let's re-verify its behavior.
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(2));
    assert_eq!(r.rows[0][1], Value::VarChar("Bob".to_string()));
}

#[test]
fn test_output_merge_update() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE target (id INT, val INT)");
    exec(&mut e, "CREATE TABLE source (id INT, val INT)");
    exec(&mut e, "INSERT INTO target VALUES (1, 10), (2, 20)");
    exec(&mut e, "INSERT INTO source VALUES (1, 100), (2, 200)");
    let r = query(
        &mut e,
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.val = s.val \
         OUTPUT DELETED.id, DELETED.val AS old_val, INSERTED.val AS new_val",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Int(10));
    assert_eq!(r.rows[0][2], Value::Int(100));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[1][1], Value::Int(20));
    assert_eq!(r.rows[1][2], Value::Int(200));
}

#[test]
fn test_output_merge_delete() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE target (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE TABLE source (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO target VALUES (1, 'Alice'), (2, 'Bob')");
    exec(&mut e, "INSERT INTO source VALUES (1, 'Alice')");
    let r = query(
        &mut e,
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN DELETE \
         OUTPUT DELETED.id, DELETED.name",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
}

#[test]
fn test_output_merge_mixed() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE target (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE TABLE source (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO target VALUES (1, 'Alice'), (2, 'Bob')");
    exec(&mut e, "INSERT INTO source VALUES (1, 'Alice Updated'), (3, 'Charlie')");
    let r = query(
        &mut e,
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.name = s.name \
         WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) \
         OUTPUT INSERTED.id, INSERTED.name",
    );
    assert_eq!(r.rows.len(), 2);
    // Row 1: UPDATE (id=1, name='Alice Updated')
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice Updated".to_string()));
    // Row 2: INSERT (id=3, name='Charlie')
    assert_eq!(r.rows[1][0], Value::Int(3));
    assert_eq!(r.rows[1][1], Value::VarChar("Charlie".to_string()));
}

#[test]
fn test_output_merge_wildcard() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE target (id INT, val INT)");
    exec(&mut e, "CREATE TABLE source (id INT, val INT)");
    exec(&mut e, "INSERT INTO target VALUES (1, 10)");
    exec(&mut e, "INSERT INTO source VALUES (1, 100)");
    let r = query(
        &mut e,
        "MERGE INTO target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.val = s.val \
         OUTPUT INSERTED.*, DELETED.*",
    );
    assert_eq!(r.columns.len(), 4); // INSERTED.id, INSERTED.val, DELETED.id, DELETED.val
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));   // INSERTED.id
    assert_eq!(r.rows[0][1], Value::Int(100));  // INSERTED.val
    assert_eq!(r.rows[0][2], Value::Int(1));    // DELETED.id
    assert_eq!(r.rows[0][3], Value::Int(10));   // DELETED.val
}

