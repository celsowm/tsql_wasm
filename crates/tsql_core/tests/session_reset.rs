use tsql_core::{parse_sql, Database, SessionManager, SqlAnalyzer};

#[test]
fn test_reset_session_clears_runtime_and_releases_locks() {
    let db = Database::new();

    let setup_sid = db.create_session();
    db.execute_session(
        setup_sid,
        parse_sql("CREATE TABLE t (id INT PRIMARY KEY)").unwrap(),
    )
    .unwrap();
    db.execute_session(
        setup_sid,
        parse_sql("INSERT INTO t (id) VALUES (1)").unwrap(),
    )
    .unwrap();
    db.close_session(setup_sid).unwrap();

    let sid = db.create_session();
    db.execute_session(sid, parse_sql("SET LOCK_TIMEOUT 100").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("CREATE TABLE #tmp (id INT)").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("UPDATE t SET id = 1 WHERE id = 1").unwrap())
        .unwrap();

    let opts = SqlAnalyzer::session_options(&db, sid).unwrap();
    assert_eq!(opts.lock_timeout_ms, 100);
    assert!(SqlAnalyzer::transaction_is_active(&db, sid).unwrap());

    let sid2 = db.create_session();
    db.execute_session(sid2, parse_sql("SET LOCK_TIMEOUT 0").unwrap())
        .unwrap();
    let lock_conflict =
        db.execute_session(sid2, parse_sql("UPDATE t SET id = 1 WHERE id = 1").unwrap());
    assert!(lock_conflict.is_err());

    db.reset_session(sid).unwrap();

    let opts_after = SqlAnalyzer::session_options(&db, sid).unwrap();
    assert_eq!(opts_after.lock_timeout_ms, 0);
    assert!(!SqlAnalyzer::transaction_is_active(&db, sid).unwrap());

    db.execute_session(sid2, parse_sql("UPDATE t SET id = 1 WHERE id = 1").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("CREATE TABLE #tmp (id INT)").unwrap())
        .unwrap();

    db.close_session(sid).unwrap();
    db.close_session(sid2).unwrap();
}

#[test]
fn test_reset_session_not_found() {
    let db = Database::new();
    let err = db.reset_session(9999).unwrap_err().to_string();
    assert!(err.contains("session 9999 not found"));
}
