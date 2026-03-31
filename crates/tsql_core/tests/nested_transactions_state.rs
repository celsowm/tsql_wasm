use tsql_core::{types::Value, Engine};

#[test]
fn test_state_restoration_on_savepoint_rollback() {
    let mut engine = Engine::new();
    
    // 1. Setup: Table with identity
    engine.exec("CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, val INT)").unwrap();
    engine.exec("DECLARE @x INT = 10").unwrap();
    
    // 2. Begin transaction
    engine.exec("BEGIN TRANSACTION").unwrap();
    
    // 3. Modify state
    engine.exec("SET @x = 20").unwrap();
    engine.exec("INSERT INTO t (val) VALUES (100)").unwrap();
    
    let r = engine.query("SELECT @x, SCOPE_IDENTITY()").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(20));
    assert_eq!(r.rows[0][1], Value::BigInt(1));
    
    // 4. Save transaction
    engine.exec("SAVE TRANSACTION sp1").unwrap();
    
    // 5. Modify state again
    engine.exec("SET @x = 30").unwrap();
    engine.exec("INSERT INTO t (val) VALUES (200)").unwrap();
    
    let r = engine.query("SELECT @x, SCOPE_IDENTITY()").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(30));
    assert_eq!(r.rows[0][1], Value::BigInt(2));
    
    // 6. Rollback to savepoint
    engine.exec("ROLLBACK TRANSACTION sp1").unwrap();
    
    // 7. Verify state is restored to step 4
    let r = engine.query("SELECT @x, SCOPE_IDENTITY()").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(20));
    assert_eq!(r.rows[0][1], Value::BigInt(1));
    
    // 8. Verify table content
    let r = engine.query("SELECT val FROM t ORDER BY val").unwrap();
    assert_eq!(r.rows, vec![vec![Value::Int(100)]]);
    
    engine.exec("COMMIT").unwrap();
}

#[test]
fn test_state_restoration_on_full_rollback() {
    let mut engine = Engine::new();
    
    engine.exec("DECLARE @x INT = 10").unwrap();
    
    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("SET @x = 20").unwrap();
    engine.exec("ROLLBACK TRANSACTION").unwrap();
    
    let r = engine.query("SELECT @x").unwrap();
    assert_eq!(r.rows[0][0], Value::Int(10));
}

#[test]
fn test_table_var_restoration_on_savepoint_rollback() {
    let mut engine = Engine::new();
    
    engine.exec("BEGIN TRANSACTION").unwrap();
    engine.exec("DECLARE @tv TABLE (id INT)").unwrap();
    engine.exec("INSERT INTO @tv VALUES (1)").unwrap();
    
    engine.exec("SAVE TRANSACTION sp1").unwrap();
    
    // In our implementation, table variables ARE backed by real tables in the workspace.
    // If we rollback, the table variable mapping should also be rolled back if it was created AFTER the savepoint.
    // But here it was created BEFORE. So the mapping remains, but let's see if its content is rolled back.
    // Actually, table variable content IS transactional in our implementation because it's in the workspace storage.
    
    engine.exec("INSERT INTO @tv VALUES (2)").unwrap();
    let r = engine.query("SELECT COUNT(*) FROM @tv").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(2));
    
    engine.exec("ROLLBACK TRANSACTION sp1").unwrap();
    
    let r = engine.query("SELECT COUNT(*) FROM @tv").unwrap();
    assert_eq!(r.rows[0][0], Value::BigInt(1));
    
    engine.exec("COMMIT").unwrap();
}

#[test]
fn test_nested_begin_transaction_state_restoration() {
    let mut engine = Engine::new();
    
    engine.exec("DECLARE @x INT = 10").unwrap();
    
    engine.exec("BEGIN TRANSACTION").unwrap(); // depth 1
    engine.exec("SET @x = 20").unwrap();
    
    engine.exec("BEGIN TRANSACTION").unwrap(); // depth 2
    engine.exec("SET @x = 30").unwrap();
    
    // In SQL Server, ROLLBACK always rolls back to depth 0.
    engine.exec("ROLLBACK TRANSACTION").unwrap();
    
    assert_eq!(engine.query("SELECT @@TRANCOUNT").unwrap().rows[0][0], Value::Int(0));
    assert_eq!(engine.query("SELECT @x").unwrap().rows[0][0], Value::Int(10));
}
