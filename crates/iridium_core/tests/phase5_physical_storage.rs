use iridium_core::types::Value;
use iridium_core::{parse_sql, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec");
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

fn query_value(engine: &mut Engine, sql: &str) -> Value {
    let result = query(engine, sql);
    assert_eq!(result.rows.len(), 1, "Expected 1 row for: {}", sql);
    assert_eq!(result.columns.len(), 1, "Expected 1 column for: {}", sql);
    result.rows[0][0].clone()
}

#[test]
fn test_index_seek_equality_predicate() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(
        &mut engine,
        "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
    );

    let result = query(&mut engine, "SELECT val FROM t WHERE id = 2");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(200));
}

#[test]
fn test_index_scan_range_predicate() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(
        &mut engine,
        "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)",
    );

    let result = query(&mut engine, "SELECT val FROM t WHERE id > 2 AND id < 5");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(300));
    assert_eq!(result.rows[1][0], Value::Int(400));
}

#[test]
fn test_table_scan_fallback_no_index() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT, val INT)");
    exec(
        &mut engine,
        "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
    );

    let result = query(&mut engine, "SELECT val FROM t WHERE id = 2");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(200));
}

#[test]
fn test_order_by_uses_index() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(
        &mut engine,
        "INSERT INTO t VALUES (3, 300), (1, 100), (2, 200)",
    );

    let result = query(&mut engine, "SELECT id FROM t ORDER BY id");
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[2][0], Value::Int(3));
}

#[test]
fn test_unique_index_enforcement() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT, val INT UNIQUE)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 100)");

    let result = engine.execute(parse_sql("INSERT INTO t VALUES (2, 100)").unwrap());
    assert!(
        result.is_err(),
        "Should fail due to unique constraint violation"
    );
}

#[test]
fn test_clustered_index_scan() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))",
    );
    exec(
        &mut engine,
        "INSERT INTO t VALUES (3, 'Charlie'), (1, 'Alice'), (2, 'Bob')",
    );

    let result = query(&mut engine, "SELECT name FROM t ORDER BY id");
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::VarChar("Alice".into()));
    assert_eq!(result.rows[1][0], Value::VarChar("Bob".into()));
    assert_eq!(result.rows[2][0], Value::VarChar("Charlie".into()));
}

#[test]
fn test_checkpoint_persists_views() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT, val INT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 100)");
    exec(
        &mut engine,
        "CREATE VIEW v AS SELECT id, val * 2 AS doubled FROM t",
    );

    let checkpoint = engine.export_checkpoint().expect("export");

    let mut engine2 = Engine::new();
    engine2.import_checkpoint(&checkpoint).expect("import");

    let result = query(&mut engine2, "SELECT doubled FROM v");
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(
        result.rows[0][0],
        Value::Int(200) | Value::BigInt(200)
    ));
}

#[test]
fn test_checkpoint_persists_indexes() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 100)");

    let checkpoint = engine.export_checkpoint().expect("export");

    let mut engine2 = Engine::new();
    engine2.import_checkpoint(&checkpoint).expect("import");

    let result = query(&mut engine2, "SELECT val FROM t WHERE id = 1");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(100));
}

#[test]
fn test_checkpoint_persists_multiple_tables() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE a (id INT PRIMARY KEY)");
    exec(&mut engine, "CREATE TABLE b (id INT PRIMARY KEY)");
    exec(&mut engine, "INSERT INTO a VALUES (1)");
    exec(&mut engine, "INSERT INTO b VALUES (2)");

    let checkpoint = engine.export_checkpoint().expect("export");

    let mut engine2 = Engine::new();
    engine2.import_checkpoint(&checkpoint).expect("import");

    let result = query(&mut engine2, "SELECT COUNT(*) FROM a");
    assert!(matches!(result.rows[0][0], Value::BigInt(1)));

    let result = query(&mut engine2, "SELECT COUNT(*) FROM b");
    assert!(matches!(result.rows[0][0], Value::BigInt(1)));
}

#[test]
fn test_checkpoint_persists_procedures() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT)");
    exec(&mut engine, "INSERT INTO t VALUES (1)");
    exec(
        &mut engine,
        "CREATE PROCEDURE p AS BEGIN SELECT id FROM t END",
    );

    let checkpoint = engine.export_checkpoint().expect("export");

    let mut engine2 = Engine::new();
    engine2.import_checkpoint(&checkpoint).expect("import");

    let result = query(&mut engine2, "EXEC p");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn test_index_on_varchar_column() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))",
    );
    exec(
        &mut engine,
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    );

    let result = query(&mut engine, "SELECT name FROM t WHERE name = 'Bob'");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::VarChar("Bob".into()));
}

#[test]
fn test_composite_index_two_columns() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (a INT, b INT, c INT, PRIMARY KEY (a, b))",
    );
    exec(&mut engine, "INSERT INTO t VALUES (1, 1, 100)");
    exec(&mut engine, "INSERT INTO t VALUES (1, 2, 200)");
    exec(&mut engine, "INSERT INTO t VALUES (2, 1, 300)");

    let result = query(&mut engine, "SELECT c FROM t WHERE a = 1 AND b = 2");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(200));
}

#[test]
fn test_index_join_two_tables() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE a (id INT PRIMARY KEY, val INT)");
    exec(&mut engine, "CREATE TABLE b (id INT PRIMARY KEY, val INT)");
    exec(&mut engine, "INSERT INTO a VALUES (1, 100), (2, 200)");
    exec(&mut engine, "INSERT INTO b VALUES (1, 1000), (2, 2000)");

    let result = query(
        &mut engine,
        "SELECT a.val, b.val FROM a JOIN b ON a.id = b.id",
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(100));
    assert_eq!(result.rows[0][1], Value::Int(1000));
}

#[test]
fn test_null_in_index_column() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, val INT NULL)",
    );
    exec(&mut engine, "INSERT INTO t VALUES (1, NULL)");
    exec(&mut engine, "INSERT INTO t VALUES (2, 100)");

    let result = query(&mut engine, "SELECT id FROM t WHERE val IS NULL");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn test_multiple_indexes_on_table() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    );
    exec(&mut engine, "CREATE INDEX idx_a ON t(a)");
    exec(
        &mut engine,
        "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)",
    );

    let result = query(&mut engine, "SELECT id FROM t WHERE a = 10");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn test_like_predicate_with_index() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))",
    );
    exec(
        &mut engine,
        "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Amanda')",
    );

    let result = query(&mut engine, "SELECT name FROM t WHERE name LIKE 'A%'");
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_in_predicate_with_index() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
    exec(
        &mut engine,
        "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)",
    );

    let result = query(&mut engine, "SELECT val FROM t WHERE id IN (1, 3)");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(100));
    assert_eq!(result.rows[1][0], Value::Int(300));
}

#[test]
fn test_checkpoint_with_transaction_state() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&mut engine, "INSERT INTO t VALUES (1)");

    exec(&mut engine, "BEGIN TRANSACTION");
    exec(&mut engine, "INSERT INTO t VALUES (2)");
    exec(&mut engine, "COMMIT");

    let checkpoint = engine.export_checkpoint().expect("export");

    let mut engine2 = Engine::new();
    engine2.import_checkpoint(&checkpoint).expect("import");

    let result = query(&mut engine2, "SELECT COUNT(*) FROM t");
    assert!(matches!(result.rows[0][0], Value::BigInt(2)));
}

#[test]
fn test_btree_index_seek_exact() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, data VARCHAR(100))",
    );
    for i in 1..=100 {
        exec(
            &mut engine,
            &format!("INSERT INTO t VALUES ({}, 'data{}')", i, i),
        );
    }

    let result = query(&mut engine, "SELECT data FROM t WHERE id = 50");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::VarChar("data50".into()));
}

#[test]
fn test_btree_index_range_lower_bound() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT PRIMARY KEY)");
    for i in 1..=50 {
        exec(&mut engine, &format!("INSERT INTO t VALUES ({})", i));
    }

    let result = query(&mut engine, "SELECT id FROM t WHERE id >= 25");
    assert_eq!(result.rows.len(), 26);
}

#[test]
fn test_btree_index_upper_bound() {
    let mut engine = Engine::new();

    exec(&mut engine, "CREATE TABLE t (id INT PRIMARY KEY)");
    for i in 1..=50 {
        exec(&mut engine, &format!("INSERT INTO t VALUES ({})", i));
    }

    let result = query(&mut engine, "SELECT id FROM t WHERE id <= 10");
    assert_eq!(result.rows.len(), 10);
}

#[test]
fn test_checkpoint_data_integrity() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, val INT CHECK (val > 0))",
    );
    exec(&mut engine, "INSERT INTO t VALUES (1, 10)");

    let checkpoint = engine.export_checkpoint().expect("export");

    let mut engine2 = Engine::new();
    engine2.import_checkpoint(&checkpoint).expect("import");

    let result = engine2.execute(parse_sql("INSERT INTO t VALUES (2, -1)").unwrap());
    assert!(
        result.is_err(),
        "Check constraint should be enforced after import"
    );
}

#[test]
fn test_dual_index_usage_different_queries() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE t (id INT PRIMARY KEY, x INT, y INT)",
    );
    exec(&mut engine, "CREATE INDEX idx_x ON t(x)");
    exec(
        &mut engine,
        "INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)",
    );

    let result1 = query(&mut engine, "SELECT * FROM t WHERE x = 10");
    assert_eq!(result1.rows.len(), 1);

    let result2 = query(&mut engine, "SELECT * FROM t WHERE y = 200");
    assert_eq!(result2.rows.len(), 1);
}

