use tsql_core::{Database, SessionManager, StatementExecutor};

#[test]
fn test_ansi_defaults_shortcut() {
    let db = Database::new();
    let session_id = db.create_session();

    // Verify initial defaults
    let opts = db.session_options(session_id).unwrap();
    assert_eq!(opts.ansi_nulls, true);
    assert_eq!(opts.implicit_transactions, false);

    // Turn ANSI_DEFAULTS ON
    db.execute_session_batch_sql(session_id, "SET ANSI_DEFAULTS ON")
        .unwrap();
    let opts = db.session_options(session_id).unwrap();
    assert_eq!(opts.ansi_nulls, true);
    assert_eq!(opts.quoted_identifier, true);
    assert_eq!(opts.ansi_null_dflt_on, true);
    assert_eq!(opts.ansi_padding, true);
    assert_eq!(opts.ansi_warnings, true);
    assert_eq!(opts.arithabort, true);
    assert_eq!(opts.cursor_close_on_commit, true);
    assert_eq!(opts.implicit_transactions, true);

    // Turn ANSI_DEFAULTS OFF
    db.execute_session_batch_sql(session_id, "SET ANSI_DEFAULTS OFF")
        .unwrap();
    let opts = db.session_options(session_id).unwrap();
    assert_eq!(opts.ansi_nulls, false);
    assert_eq!(opts.implicit_transactions, false);
}

#[test]
fn test_noexec_and_parseonly_options() {
    let db = Database::new();
    let session_id = db.create_session();

    db.execute_session_batch_sql(session_id, "SET NOEXEC ON")
        .unwrap();
    let opts = db.session_options(session_id).unwrap();
    assert_eq!(opts.noexec, true);

    db.execute_session_batch_sql(session_id, "SET PARSEONLY ON")
        .unwrap();
    let opts = db.session_options(session_id).unwrap();
    assert_eq!(opts.parseonly, true);
}

#[test]
fn test_noexec_prevents_execution() {
    let db = Database::new();
    let session_id = db.create_session();

    db.execute_session_batch_sql(session_id, "CREATE TABLE T(A INT)").unwrap();
    db.execute_session_batch_sql(session_id, "SET NOEXEC ON").unwrap();

    // This should NOT insert anything
    db.execute_session_batch_sql(session_id, "INSERT INTO T VALUES(1)").unwrap();

    db.execute_session_batch_sql(session_id, "SET NOEXEC OFF").unwrap();
    let res = db.execute_session_batch_sql(session_id, "SELECT COUNT(*) FROM T").unwrap().expect("should return result");
    assert_eq!(res.rows[0][0].to_integer_i64(), Some(0));
}

#[test]
fn test_parseonly_prevents_execution_of_batch() {
    let db = Database::new();
    let session_id = db.create_session();

    db.execute_session_batch_sql(session_id, "CREATE TABLE T(A INT)").unwrap();

    // Verify SELECT works
    let _ = db.execute_session_batch_sql(session_id, "SELECT COUNT(*) FROM T").unwrap().expect("SELECT should work here");

    // Batch that turns PARSEONLY ON should not execute any of its statements
    let res = db.execute_session_batch_sql(session_id, "INSERT INTO T VALUES(1); SET PARSEONLY ON; INSERT INTO T VALUES(2)").unwrap();
    assert!(res.is_none(), "Batch with PARSEONLY ON should return None");

    // Verify T is still empty
    let res = db.execute_session_batch_sql(session_id, "SELECT COUNT(*) FROM T").unwrap();
    assert!(res.is_none(), "Subsequent SELECT should return None when PARSEONLY is ON");

    // Turn it off
    let _ = db.execute_session_batch_sql(session_id, "SET PARSEONLY OFF").unwrap();

    // NOW it should work
    db.execute_session_batch_sql(session_id, "INSERT INTO T VALUES(4)").unwrap();
    let res = db.execute_session_batch_sql(session_id, "SELECT COUNT(*) FROM T").unwrap().expect("Should return result now");
    assert_eq!(res.rows[0][0].to_integer_i64(), Some(1));
}

#[test]
fn test_dm_exec_views_present() {
    let db = Database::new();
    let session_id = db.create_session();

    let res = db
        .execute_session_batch_sql(session_id, "SELECT session_id FROM sys.dm_exec_sessions")
        .unwrap();
    assert!(res.is_some());
    let qr = res.unwrap();
    assert!(!qr.rows.is_empty());

    let res = db
        .execute_session_batch_sql(session_id, "SELECT session_id FROM sys.dm_exec_connections")
        .unwrap();
    assert!(res.is_some());
    let qr = res.unwrap();
    assert!(!qr.rows.is_empty());

    let res = db
        .execute_session_batch_sql(session_id, "SELECT session_id FROM sys.dm_exec_requests")
        .unwrap();
    assert!(res.is_some());
    let qr = res.unwrap();
    assert!(!qr.rows.is_empty());
}
