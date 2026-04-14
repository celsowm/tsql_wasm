use tsql_core::types::Value;
use tsql_core::Engine;

#[test]
fn test_basic_transaction_commit() {
    let engine = Engine::new();

    engine
        .exec("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    engine.exec("INSERT INTO t VALUES (1, 10)").unwrap();

    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("INSERT INTO t VALUES (2, 20)").unwrap();
    engine.exec("INSERT INTO t VALUES (3, 30)").unwrap();
    engine.exec("COMMIT").unwrap();

    let r = engine.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(3));
}

#[test]
fn test_basic_transaction_rollback() {
    let engine = Engine::new();

    engine
        .exec("CREATE TABLE t (id INT PRIMARY KEY, val INT)")
        .unwrap();
    engine.exec("INSERT INTO t VALUES (1, 10)").unwrap();

    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("INSERT INTO t VALUES (2, 20)").unwrap();
    engine.exec("INSERT INTO t VALUES (3, 30)").unwrap();
    engine.exec("ROLLBACK").unwrap();

    let r = engine.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(1));
}

#[test]
fn test_savepoint_rollback() {
    let engine = Engine::new();

    engine.exec("CREATE TABLE t (id INT)").unwrap();
    engine.exec("DECLARE @x INT = 0").unwrap();

    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("SET @x = 1").unwrap();
    engine.exec("INSERT INTO t VALUES (@x)").unwrap();
    engine.exec("SAVE TRANSACTION sp1").unwrap();

    engine.exec("SET @x = 2").unwrap();
    engine.exec("INSERT INTO t VALUES (@x)").unwrap();

    engine.exec("ROLLBACK TRANSACTION sp1").unwrap();

    let r = engine.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));

    let r = engine.query("SELECT @x").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(1));

    engine.exec("COMMIT").unwrap();
}

#[test]
fn test_multiple_savepoints() {
    let engine = Engine::new();

    engine.exec("CREATE TABLE t (id INT)").unwrap();
    engine.exec("DECLARE @x INT = 0").unwrap();

    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("SET @x = 1").unwrap();
    engine.exec("INSERT INTO t VALUES (@x)").unwrap();
    engine.exec("SAVE TRANSACTION sp1").unwrap();

    engine.exec("SET @x = 2").unwrap();
    engine.exec("INSERT INTO t VALUES (@x)").unwrap();
    engine.exec("SAVE TRANSACTION sp2").unwrap();

    engine.exec("SET @x = 3").unwrap();
    engine.exec("INSERT INTO t VALUES (@x)").unwrap();

    engine.exec("ROLLBACK TRANSACTION sp1").unwrap();

    let r = engine.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));

    engine.exec("COMMIT").unwrap();
}

#[test]
fn test_nested_transaction_commit() {
    let engine = Engine::new();

    engine.exec("CREATE TABLE t (id INT)").unwrap();

    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("INSERT INTO t VALUES (1)").unwrap();
    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("INSERT INTO t VALUES (2)").unwrap();
    engine.exec("COMMIT").unwrap();

    let r = engine.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(2));

    engine.exec("ROLLBACK").unwrap();

    let r = engine.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(0));
}

#[test]
fn test_xact_state_in_transaction() {
    let engine = Engine::new();

    let r = engine.query("SELECT XACT_STATE()").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(0));

    engine.exec("BEGIN TRANSACTION").unwrap();
    let r = engine.query("SELECT XACT_STATE()").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(1));

    engine.exec("COMMIT").unwrap();
    let r = engine.query("SELECT XACT_STATE()").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(0));
}

#[test]
fn test_xact_state_after_error() {
    let engine = Engine::new();

    engine.exec("BEGIN TRANSACTION").unwrap();
    let _ = engine.exec("SELECT 1/0");

    let r = engine.query("SELECT XACT_STATE()").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(-1));

    engine.exec("ROLLBACK").unwrap();

    let r = engine.query("SELECT XACT_STATE()").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(0));
}

#[test]
fn test_set_isolation_level() {
    let engine = Engine::new();

    engine
        .exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .unwrap();
    engine
        .exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .unwrap();
    engine
        .exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .unwrap();
    engine
        .exec("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
        .unwrap();
    engine
        .exec("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        .unwrap();
}

#[test]
fn test_variable_rollback() {
    let engine = Engine::new();

    engine.exec("DECLARE @x INT = 10").unwrap();
    let r = engine.query("SELECT @x").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(10));

    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("SET @x = 20").unwrap();
    let r = engine.query("SELECT @x").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(20));

    engine.exec("ROLLBACK").unwrap();

    let r = engine.query("SELECT @x").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(10));
}

#[test]
fn test_identity_in_transaction() {
    let engine = Engine::new();

    engine
        .exec("CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, val INT)")
        .unwrap();

    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("INSERT INTO t (val) VALUES (100)").unwrap();
    let r = engine.query("SELECT SCOPE_IDENTITY()").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(1));

    engine.exec("ROLLBACK").unwrap();

    engine.exec("INSERT INTO t (val) VALUES (200)").unwrap();
    let r = engine.query("SELECT SCOPE_IDENTITY()").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(1));
}

#[test]
fn test_temp_table_transaction() {
    let engine = Engine::new();

    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("CREATE TABLE #temp (id INT)").unwrap();
    engine.exec("INSERT INTO #temp VALUES (1), (2)").unwrap();

    let r = engine.query("SELECT COUNT(*) FROM #temp").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(2));

    engine.exec("ROLLBACK").unwrap();

    let result = engine.query("SELECT * FROM #temp");
    assert!(result.is_err());
}
