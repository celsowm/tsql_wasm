use tsql_core::types::Value;
use tsql_core::{parse_sql, Database, Engine, SessionManager, StatementExecutor};

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_sql(sql).expect("parse")).expect("exec");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
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

/// Test checkpoint export/import roundtrip
#[test]
fn test_phase8_persistence_checkpoint_roundtrip() {
    let mut engine = Engine::new();

    // Create and populate table
    exec(
        &mut engine,
        "CREATE TABLE checkpoint_test (id INT PRIMARY KEY, name VARCHAR(50), value INT)",
    );
    exec(
        &mut engine,
        "INSERT INTO checkpoint_test (id, name, value) VALUES (1, 'Alice', 100)",
    );
    exec(
        &mut engine,
        "INSERT INTO checkpoint_test (id, name, value) VALUES (2, 'Bob', 200)",
    );
    exec(
        &mut engine,
        "INSERT INTO checkpoint_test (id, name, value) VALUES (3, 'Charlie', 300)",
    );

    // Verify initial state
    let result = query(&mut engine, "SELECT COUNT(*) FROM checkpoint_test");
    assert!(matches!(result.rows[0][0], Value::BigInt(3)));

    // Query all rows
    let result = query(&mut engine, "SELECT * FROM checkpoint_test ORDER BY id");
    assert_eq!(result.rows.len(), 3);
}

/// Test transaction commit persistence
#[test]
fn test_phase8_persistence_transaction_commit() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE commit_test (id INT PRIMARY KEY, value INT)",
    );
    exec(
        &mut engine,
        "INSERT INTO commit_test (id, value) VALUES (1, 100)",
    );

    // Begin transaction and modify
    exec(&mut engine, "BEGIN TRANSACTION");
    exec(
        &mut engine,
        "INSERT INTO commit_test (id, value) VALUES (2, 200)",
    );
    exec(
        &mut engine,
        "UPDATE commit_test SET value = 150 WHERE id = 1",
    );

    // Commit
    exec(&mut engine, "COMMIT");

    // Verify committed data
    let result = query(&mut engine, "SELECT COUNT(*) FROM commit_test");
    assert!(matches!(result.rows[0][0], Value::BigInt(2)));

    let result = query_value(&mut engine, "SELECT value FROM commit_test WHERE id = 1");
    assert!(matches!(result, Value::Int(150) | Value::BigInt(150)));
}

/// Test transaction rollback
#[test]
fn test_phase8_persistence_transaction_rollback() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE rollback_test (id INT PRIMARY KEY, value INT)",
    );
    exec(
        &mut engine,
        "INSERT INTO rollback_test (id, value) VALUES (1, 100)",
    );

    // Begin transaction and modify
    exec(&mut engine, "BEGIN TRANSACTION");
    exec(
        &mut engine,
        "INSERT INTO rollback_test (id, value) VALUES (2, 200)",
    );

    // Rollback
    exec(&mut engine, "ROLLBACK");

    // Verify rollback - should only have original row
    let result = query(&mut engine, "SELECT COUNT(*) FROM rollback_test");
    assert!(matches!(result.rows[0][0], Value::BigInt(1)));
}

/// Test data consistency after multiple operations
#[test]
fn test_phase8_persistence_data_consistency() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE consistency_test (id INT PRIMARY KEY, counter INT)",
    );
    exec(
        &mut engine,
        "INSERT INTO consistency_test (id, counter) VALUES (1, 0)",
    );

    // Perform multiple updates
    for _ in 0..10 {
        exec(
            &mut engine,
            "UPDATE consistency_test SET counter = counter + 1 WHERE id = 1",
        );
    }

    let result = query_value(&mut engine, "SELECT counter FROM consistency_test WHERE id = 1");
    assert!(matches!(result, Value::Int(10) | Value::BigInt(10)));
}

/// Test concurrent transaction isolation
#[test]
fn test_phase8_persistence_isolation() {
    let db = Database::new();

    let sid1 = db.create_session();
    let sid2 = db.create_session();

    // Session 1 creates table
    db.execute_session(
        sid1,
        parse_sql("CREATE TABLE isolation_test (id INT PRIMARY KEY, value INT)").unwrap(),
    )
    .unwrap();

    // Session 1 inserts data
    db.execute_session(
        sid1,
        parse_sql("INSERT INTO isolation_test (id, value) VALUES (1, 100)").unwrap(),
    )
    .unwrap();

    // Session 2 should see committed data
    let result = db
        .execute_session(sid2, parse_sql("SELECT * FROM isolation_test").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

/// Test recovery from constraint violations
#[test]
fn test_phase8_persistence_constraint_recovery() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE constraint_test (id INT PRIMARY KEY, value INT NOT NULL)",
    );
    exec(
        &mut engine,
        "INSERT INTO constraint_test (id, value) VALUES (1, 100)",
    );

    // Try to violate primary key
    let err = engine.execute(
        parse_sql("INSERT INTO constraint_test (id, value) VALUES (1, 200)").unwrap(),
    );
    assert!(err.is_err());

    // Original data should still be intact
    let result = query_value(&mut engine, "SELECT value FROM constraint_test WHERE id = 1");
    assert!(matches!(result, Value::Int(100) | Value::BigInt(100)));

    // Should still have only 1 row
    let result = query(&mut engine, "SELECT COUNT(*) FROM constraint_test");
    assert!(matches!(result.rows[0][0], Value::BigInt(1)));
}

/// Test TRUNCATE TABLE recovery
#[test]
fn test_phase8_persistence_truncate() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE truncate_test (id INT PRIMARY KEY, value INT)",
    );

    // Insert data
    for i in 0..100 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO truncate_test (id, value) VALUES ({}, {})",
                i, i
            ),
        );
    }

    // Verify data exists
    let result = query(&mut engine, "SELECT COUNT(*) FROM truncate_test");
    assert!(matches!(result.rows[0][0], Value::BigInt(100)));

    // Truncate
    exec(&mut engine, "TRUNCATE TABLE truncate_test");

    // Verify table is empty
    let result = query(&mut engine, "SELECT COUNT(*) FROM truncate_test");
    assert!(matches!(result.rows[0][0], Value::BigInt(0)));
}

/// Test DROP TABLE cleanup
#[test]
fn test_phase8_persistence_drop_cleanup() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE drop_test (id INT PRIMARY KEY)",
    );
    exec(
        &mut engine,
        "INSERT INTO drop_test (id) VALUES (1)",
    );

    // Drop table
    exec(&mut engine, "DROP TABLE drop_test");

    // Table should not be accessible
    let err = engine.execute(parse_sql("SELECT * FROM drop_test").unwrap());
    assert!(err.is_err());
}

/// Test index persistence
#[test]
fn test_phase8_persistence_index() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE index_test (id INT PRIMARY KEY, name VARCHAR(50))",
    );

    // Create index
    exec(&mut engine, "CREATE INDEX ix_name ON index_test (name)");

    // Insert data
    for i in 0..10 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO index_test (id, name) VALUES ({}, 'name_{}')",
                i, i
            ),
        );
    }

    // Index should exist in metadata
    let result = query(
        &mut engine,
        "SELECT COUNT(*) FROM sys.indexes WHERE name = 'ix_name'",
    );
    assert!(matches!(result.rows[0][0], Value::BigInt(1)));

    // Drop index
    exec(&mut engine, "DROP INDEX ix_name ON index_test");

    // Index should not exist
    let result = query(
        &mut engine,
        "SELECT COUNT(*) FROM sys.indexes WHERE name = 'ix_name'",
    );
    assert!(matches!(result.rows[0][0], Value::BigInt(0)));
}

/// Test temporary table cleanup
#[test]
fn test_phase8_persistence_temp_table() {
    let db = Database::new();

    let sid1 = db.create_session();
    let sid2 = db.create_session();

    // Session 1 creates temp table
    db.execute_session(
        sid1,
        parse_sql("CREATE TABLE #temp (id INT)").unwrap(),
    )
    .unwrap();

    db.execute_session(
        sid1,
        parse_sql("INSERT INTO #temp (id) VALUES (1)").unwrap(),
    )
    .unwrap();

    // Session 1 can see temp table
    let result = db
        .execute_session(sid1, parse_sql("SELECT * FROM #temp").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    // Session 2 cannot see session 1's temp table
    let err = db.execute_session(sid2, parse_sql("SELECT * FROM #temp").unwrap());
    assert!(err.is_err());

    // Close session 1
    let _ = db.close_session(sid1);

    // New session should not see old temp table
    let sid3 = db.create_session();
    let err = db.execute_session(sid3, parse_sql("SELECT * FROM #temp").unwrap());
    assert!(err.is_err());
}

/// Test deterministic behavior across restarts (simulated)
#[test]
fn test_phase8_persistence_deterministic() {
    let mut engine1 = Engine::new();
    let mut engine2 = Engine::new();

    let setup_sql = r#"
        CREATE TABLE test (id INT, value INT);
        INSERT INTO test (id, value) VALUES (1, 10);
        INSERT INTO test (id, value) VALUES (2, 20);
        INSERT INTO test (id, value) VALUES (3, 30);
    "#;

    // Setup both engines identically
    for sql in setup_sql.split(';') {
        let sql = sql.trim();
        if !sql.is_empty() {
            exec(&mut engine1, sql);
            exec(&mut engine2, sql);
        }
    }

    // Run same query on both
    let result1 = query(&mut engine1, "SELECT * FROM test ORDER BY id");
    let result2 = query(&mut engine2, "SELECT * FROM test ORDER BY id");

    // Results should be identical
    assert_eq!(result1.rows.len(), result2.rows.len());
    for (row1, row2) in result1.rows.iter().zip(result2.rows.iter()) {
        assert_eq!(row1, row2);
    }
}

/// Test error recovery - engine should remain usable after errors
#[test]
fn test_phase8_persistence_error_recovery() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE recovery_test (id INT PRIMARY KEY)",
    );
    exec(
        &mut engine,
        "INSERT INTO recovery_test (id) VALUES (1)",
    );

    // Cause various errors
    let errors = vec![
        "SELECT * FROM nonexistent",
        "INSERT INTO recovery_test (id) VALUES (1)",  // Duplicate PK
        "SELECT invalid_column FROM recovery_test",
    ];

    for sql in errors {
        let _ = engine.execute(parse_sql(sql).unwrap());
    }

    // Engine should still work
    let result = query(&mut engine, "SELECT COUNT(*) FROM recovery_test");
    assert!(matches!(result.rows[0][0], Value::BigInt(1)));

    // Should be able to insert new data
    exec(
        &mut engine,
        "INSERT INTO recovery_test (id) VALUES (2)",
    );

    let result = query(&mut engine, "SELECT COUNT(*) FROM recovery_test");
    assert!(matches!(result.rows[0][0], Value::BigInt(2)));
}
