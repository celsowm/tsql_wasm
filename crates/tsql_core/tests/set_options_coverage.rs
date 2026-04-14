use tsql_core::{parse_sql, types::Value, Database, Engine, SqlAnalyzer};

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect(sql);
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine.query(sql).expect(sql)
}

#[test]
fn test_ansi_warnings_off_divide_by_zero_returns_null() {
    let mut engine = Engine::new();
    exec(&mut engine, "SET ANSI_WARNINGS OFF");
    exec(&mut engine, "SET ARITHABORT OFF");

    let result = query(&mut engine, "SELECT 1 / 0");
    assert!(
        result.rows[0][0].is_null(),
        "ANSI_WARNINGS OFF + ARITHABORT OFF: divide by zero should return NULL"
    );
}

#[test]
fn test_arithabort_on_overflow_aborts() {
    let mut engine = Engine::new();
    exec(&mut engine, "SET ARITHABORT ON");
    exec(&mut engine, "SET ANSI_WARNINGS OFF");

    exec(&mut engine, "CREATE TABLE dbo.aa_test (v TINYINT)");
    exec(&mut engine, "INSERT INTO dbo.aa_test VALUES (250)");

    let result = engine.exec("UPDATE dbo.aa_test SET v = v + 10");
    assert!(
        result.is_err(),
        "ARITHABORT ON should abort on arithmetic overflow"
    );
}

#[test]
fn test_quoted_identifier_stored_and_roundtrips() {
    let db = Database::new();
    let sid = db.create_session();

    db.execute_session(sid, parse_sql("SET QUOTED_IDENTIFIER OFF").unwrap())
        .unwrap();

    let opts = db.session_options(sid).unwrap();
    assert!(!opts.quoted_identifier, "QUOTED_IDENTIFIER should be OFF");

    db.execute_session(sid, parse_sql("SET QUOTED_IDENTIFIER ON").unwrap())
        .unwrap();

    let opts = db.session_options(sid).unwrap();
    assert!(opts.quoted_identifier, "QUOTED_IDENTIFIER should be ON");
}

#[test]
fn test_query_governor_cost_limit_acceptance() {
    let db = Database::new();
    let sid = db.create_session();

    db.execute_session(sid, parse_sql("SET QUERY_GOVERNOR_COST_LIMIT 100").unwrap())
        .unwrap();

    let opts = db.session_options(sid).unwrap();
    assert_eq!(opts.query_governor_cost_limit, 100);

    db.execute_session(sid, parse_sql("SET QUERY_GOVERNOR_COST_LIMIT 0").unwrap())
        .unwrap();

    let opts = db.session_options(sid).unwrap();
    assert_eq!(opts.query_governor_cost_limit, 0);
}

#[test]
fn test_statistics_io_time_acceptance() {
    let db = Database::new();
    let sid = db.create_session();

    db.execute_session(sid, parse_sql("SET STATISTICS IO ON").unwrap())
        .unwrap();
    let opts = db.session_options(sid).unwrap();
    assert!(opts.statistics_io);

    db.execute_session(sid, parse_sql("SET STATISTICS TIME ON").unwrap())
        .unwrap();
    let opts = db.session_options(sid).unwrap();
    assert!(opts.statistics_time);

    db.execute_session(sid, parse_sql("SET STATISTICS IO OFF").unwrap())
        .unwrap();
    db.execute_session(sid, parse_sql("SET STATISTICS TIME OFF").unwrap())
        .unwrap();
    let opts = db.session_options(sid).unwrap();
    assert!(!opts.statistics_io);
    assert!(!opts.statistics_time);
}

#[test]
fn test_deadlock_priority_ordering() {
    let db = Database::new();
    let sid = db.create_session();

    db.execute_session(sid, parse_sql("SET DEADLOCK_PRIORITY LOW").unwrap())
        .unwrap();
    let opts = db.session_options(sid).unwrap();
    assert_eq!(opts.deadlock_priority, -5);

    db.execute_session(sid, parse_sql("SET DEADLOCK_PRIORITY NORMAL").unwrap())
        .unwrap();
    let opts = db.session_options(sid).unwrap();
    assert_eq!(opts.deadlock_priority, 0);

    db.execute_session(sid, parse_sql("SET DEADLOCK_PRIORITY HIGH").unwrap())
        .unwrap();
    let opts = db.session_options(sid).unwrap();
    assert_eq!(opts.deadlock_priority, 5);

    db.execute_session(sid, parse_sql("SET DEADLOCK_PRIORITY -10").unwrap())
        .unwrap();
    let opts = db.session_options(sid).unwrap();
    assert_eq!(opts.deadlock_priority, -10);

    db.execute_session(sid, parse_sql("SET DEADLOCK_PRIORITY 10").unwrap())
        .unwrap();
    let opts = db.session_options(sid).unwrap();
    assert_eq!(opts.deadlock_priority, 10);
}

#[test]
fn test_nocount_suppresses_rowcount_messages() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.nc_test (id INT)");
    exec(&mut engine, "SET NOCOUNT ON");

    exec(&mut engine, "INSERT INTO dbo.nc_test VALUES (1), (2), (3)");
    let result = query(&mut engine, "SELECT COUNT(*) FROM dbo.nc_test");
    assert_eq!(result.rows[0][0], Value::BigInt(3));

    exec(&mut engine, "SET NOCOUNT OFF");
    let result = query(&mut engine, "SELECT COUNT(*) FROM dbo.nc_test");
    assert_eq!(result.rows[0][0], Value::BigInt(3));
}

#[test]
fn test_set_options_chained_batch() {
    let db = Database::new();
    let sid = db.create_session();

    let sql = "SET NOCOUNT ON; SET XACT_ABORT ON; SET ANSI_NULLS OFF; SET DATEFIRST 1";
    db.execute_session_batch_sql(sid, sql).unwrap();

    let opts = db.session_options(sid).unwrap();
    assert!(opts.nocount);
    assert!(opts.xact_abort);
    assert!(!opts.ansi_nulls);
    assert_eq!(opts.datefirst, 1);
}
