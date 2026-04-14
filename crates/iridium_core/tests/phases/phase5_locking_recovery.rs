use iridium_core::{parse_sql, Database, Engine};

#[test]
fn test_phase5_lock_release_on_savepoint_rollback() {
    let db = Database::new();
    let s1 = db.create_session();
    let s2 = db.create_session();

    db.execute_session(
        s1,
        parse_sql("CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)").unwrap(),
    )
    .unwrap();
    db.execute_session(
        s1,
        parse_sql("CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)").unwrap(),
    )
    .unwrap();
    db.execute_session(
        s1,
        parse_sql("INSERT INTO t1 (id, v) VALUES (1, 10)").unwrap(),
    )
    .unwrap();
    db.execute_session(
        s1,
        parse_sql("INSERT INTO t2 (id, v) VALUES (1, 10)").unwrap(),
    )
    .unwrap();

    db.execute_session(s1, parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    db.execute_session(s1, parse_sql("UPDATE t1 SET v = 11 WHERE id = 1").unwrap())
        .unwrap();
    db.execute_session(s1, parse_sql("SAVE TRANSACTION sp1").unwrap())
        .unwrap();
    db.execute_session(s1, parse_sql("UPDATE t2 SET v = 12 WHERE id = 1").unwrap())
        .unwrap();
    db.execute_session(s1, parse_sql("ROLLBACK TRANSACTION sp1").unwrap())
        .unwrap();

    db.execute_session(s2, parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    db.execute_session(s2, parse_sql("UPDATE t2 SET v = 20 WHERE id = 1").unwrap())
        .unwrap();
    let err = db
        .execute_session(s2, parse_sql("UPDATE t1 SET v = 20 WHERE id = 1").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("lock conflict (no-wait)"));
    db.execute_session(s2, parse_sql("ROLLBACK").unwrap()).unwrap();
    db.execute_session(s1, parse_sql("ROLLBACK").unwrap()).unwrap();
}

#[test]
fn test_phase5_recovery_checkpoint_roundtrip() {
    let engine = Engine::new();
    engine
        .execute(parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id, v) VALUES (1, 100)").unwrap())
        .unwrap();

    let checkpoint = engine.export_checkpoint().unwrap();
    let restored = Database::from_checkpoint(&checkpoint).unwrap();
    let sid = restored.create_session();
    let result = restored
        .execute_session(sid, parse_sql("SELECT v FROM t WHERE id = 1").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_phase5_recovery_excludes_uncommitted_workspace_state() {
    let db = Database::new();
    let sid = db.create_session();
    db.execute_session(
        sid,
        parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)").unwrap(),
    )
    .unwrap();
    db.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("INSERT INTO t (id, v) VALUES (1, 10)").unwrap())
        .unwrap();

    let checkpoint = db.export_checkpoint().unwrap();
    let restored = Database::from_checkpoint(&checkpoint).unwrap();
    let rsid = restored.create_session();
    let result = restored
        .execute_session(rsid, parse_sql("SELECT COUNT(*) FROM t").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows[0][0].to_integer_i64(), Some(0));
}

#[test]
fn test_phase5_recovery_savepoint_rollback_then_commit() {
    let db = Database::new();
    let sid = db.create_session();
    db.execute_session(
        sid,
        parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)").unwrap(),
    )
    .unwrap();
    db.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("SAVE TRANSACTION sp1").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("INSERT INTO t (id) VALUES (2)").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("ROLLBACK TRANSACTION sp1").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("COMMIT").unwrap()).unwrap();

    let checkpoint = db.export_checkpoint().unwrap();
    let restored = Database::from_checkpoint(&checkpoint).unwrap();
    let rsid = restored.create_session();
    let result = restored
        .execute_session(rsid, parse_sql("SELECT id FROM t ORDER BY id").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

