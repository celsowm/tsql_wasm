use tsql_core::ast::{IsolationLevel, Statement};
use tsql_core::{parse_batch, parse_sql, types::Value, Engine};

#[test]
fn test_phase5_parser_transaction_statements() {
    assert!(matches!(
        parse_sql("BEGIN TRANSACTION tx1").unwrap(),
        Statement::BeginTransaction(Some(_))
    ));
    assert!(matches!(
        parse_sql("COMMIT TRANSACTION").unwrap(),
        Statement::CommitTransaction(_)
    ));
    assert!(matches!(
        parse_sql("ROLLBACK TRANSACTION sp1").unwrap(),
        Statement::RollbackTransaction(Some(_))
    ));
    assert!(matches!(
        parse_sql("SAVE TRANSACTION sp1").unwrap(),
        Statement::SaveTransaction(_)
    ));
    assert!(matches!(
        parse_sql("SET TRANSACTION ISOLATION LEVEL SNAPSHOT").unwrap(),
        Statement::SetTransactionIsolationLevel(IsolationLevel::Snapshot)
    ));
}

#[test]
fn test_phase5_begin_commit_persists_changes() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();
    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();

    let result = engine
        .execute(parse_sql("SELECT id FROM t").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn test_phase5_begin_rollback_discards_changes() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();
    engine.execute(parse_sql("ROLLBACK").unwrap()).unwrap();

    let result = engine
        .execute(parse_sql("SELECT id FROM t").unwrap())
        .unwrap()
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn test_phase5_savepoint_rollback_restores_partial_state() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("SAVE TRANSACTION sp1").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id) VALUES (2)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("ROLLBACK TRANSACTION sp1").unwrap())
        .unwrap();
    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();

    let result = engine
        .execute(parse_sql("SELECT id FROM t ORDER BY id").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}

#[test]
fn test_phase5_commit_without_active_transaction_errors() {
    let mut engine = Engine::new();
    let err = engine.execute(parse_sql("COMMIT").unwrap()).unwrap_err();
    assert!(err
        .to_string()
        .contains("COMMIT without active transaction"));
}

#[test]
fn test_phase5_rollback_without_active_transaction_errors() {
    let mut engine = Engine::new();
    let err = engine.execute(parse_sql("ROLLBACK").unwrap()).unwrap_err();
    assert!(err
        .to_string()
        .contains("ROLLBACK without active transaction"));
}

#[test]
fn test_phase5_nested_begin_transaction_supported() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();
    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();
}

#[test]
fn test_phase5_trancount_reflects_depth() {
    let mut engine = Engine::new();

    let result = engine
        .execute(parse_sql("SELECT @@TRANCOUNT").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(0)]]);

    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    let result = engine
        .execute(parse_sql("SELECT @@TRANCOUNT").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    let result = engine
        .execute(parse_sql("SELECT @@TRANCOUNT").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(2)]]);

    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();
    let result = engine
        .execute(parse_sql("SELECT @@TRANCOUNT").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);

    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();
    let result = engine
        .execute(parse_sql("SELECT @@TRANCOUNT").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(0)]]);
}

#[test]
fn test_phase5_nested_commit_only_outermost_persists() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)").unwrap())
        .unwrap();

    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();

    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id) VALUES (2)").unwrap())
        .unwrap();
    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();

    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();

    let result = engine
        .execute(parse_sql("SELECT id FROM t ORDER BY id").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
}

#[test]
fn test_phase5_nested_rollback_rolls_back_all() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)").unwrap())
        .unwrap();

    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();

    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id) VALUES (2)").unwrap())
        .unwrap();
    engine.execute(parse_sql("ROLLBACK").unwrap()).unwrap();

    let result = engine
        .execute(parse_sql("SELECT id FROM t").unwrap())
        .unwrap()
        .unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn test_phase5_trancount_in_batch() {
    let mut engine = Engine::new();

    engine.execute(parse_sql("BEGIN TRANSACTION").unwrap()).unwrap();
    let r = engine.execute(parse_sql("SELECT @@TRANCOUNT").unwrap()).unwrap().unwrap();
    assert_eq!(r.rows[0][0], Value::Int(1));

    engine.execute(parse_sql("BEGIN TRANSACTION").unwrap()).unwrap();
    let r = engine.execute(parse_sql("SELECT @@TRANCOUNT").unwrap()).unwrap().unwrap();
    assert_eq!(r.rows[0][0], Value::Int(2));

    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();
    let r = engine.execute(parse_sql("SELECT @@TRANCOUNT").unwrap()).unwrap().unwrap();
    assert_eq!(r.rows[0][0], Value::Int(1));

    engine.execute(parse_sql("COMMIT").unwrap()).unwrap();
    let r = engine.execute(parse_sql("SELECT @@TRANCOUNT").unwrap()).unwrap().unwrap();
    assert_eq!(r.rows[0][0], Value::Int(0));
}

#[test]
fn test_phase5_set_isolation_level_session_and_tx() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").unwrap())
        .unwrap();
    assert_eq!(
        engine.session_isolation_level(),
        IsolationLevel::RepeatableRead
    );

    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("SET TRANSACTION ISOLATION LEVEL SNAPSHOT").unwrap())
        .unwrap();
    assert_eq!(engine.session_isolation_level(), IsolationLevel::Snapshot);
    engine.execute(parse_sql("ROLLBACK").unwrap()).unwrap();
}

#[test]
fn test_phase5_rollback_unknown_savepoint_errors() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    let err = engine
        .execute(parse_sql("ROLLBACK TRANSACTION nope").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("savepoint 'nope' not found"));
}

#[test]
fn test_phase5_execute_batch_with_transaction_keywords() {
    let mut engine = Engine::new();
    let setup = parse_batch("CREATE TABLE t (id INT NOT NULL PRIMARY KEY);").unwrap();
    engine.execute_batch(setup).unwrap();

    let batch = parse_batch(
        "BEGIN TRANSACTION; INSERT INTO t (id) VALUES (1); SAVE TRANSACTION s1; INSERT INTO t (id) VALUES (2); ROLLBACK TRANSACTION s1; COMMIT;",
    )
    .unwrap();
    engine.execute_batch(batch).unwrap();

    let result = engine
        .execute(parse_sql("SELECT id FROM t ORDER BY id").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
}
