use tsql_core::ast::IsolationLevel;
use tsql_core::types::Value;
use tsql_core::{parse_sql, Database};

#[derive(Clone, Copy)]
enum Step<'a> {
    Exec {
        sid: u64,
        sql: &'a str,
        expect_err: Option<&'a str>,
    },
    QueryI64 {
        sid: u64,
        sql: &'a str,
        expected: i64,
    },
}

fn iso_sql(level: IsolationLevel) -> &'static str {
    match level {
        IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
        IsolationLevel::ReadCommitted => "READ COMMITTED",
        IsolationLevel::RepeatableRead => "REPEATABLE READ",
        IsolationLevel::Serializable => "SERIALIZABLE",
        IsolationLevel::Snapshot => "SNAPSHOT",
    }
}

fn run_steps(db: &Database, steps: &[Step<'_>]) {
    for step in steps {
        match *step {
            Step::Exec {
                sid,
                sql,
                expect_err,
            } => {
                let stmt = parse_sql(sql).unwrap();
                let out = db.execute_session(sid, stmt);
                match expect_err {
                    Some(substr) => {
                        let err = out.unwrap_err();
                        assert!(
                            err.to_string().contains(substr),
                            "expected error containing '{substr}', got '{err}'"
                        );
                    }
                    None => {
                        out.unwrap();
                    }
                }
            }
            Step::QueryI64 { sid, sql, expected } => {
                let stmt = parse_sql(sql).unwrap();
                let out = db.execute_session(sid, stmt).unwrap().unwrap();
                assert_eq!(out.rows.len(), 1);
                assert_eq!(out.rows[0].len(), 1);
                let got = value_to_i64(&out.rows[0][0]);
                assert_eq!(got, expected, "query mismatch for SQL: {sql}");
            }
        }
    }
}

fn value_to_i64(v: &Value) -> i64 {
    match v {
        Value::TinyInt(x) => *x as i64,
        Value::SmallInt(x) => *x as i64,
        Value::Int(x) => *x as i64,
        Value::BigInt(x) => *x,
        _ => panic!("expected integer-ish scalar, got {v:?}"),
    }
}

fn new_db_with_sessions() -> (Database, u64, u64) {
    let db = Database::new();
    let s1 = db.create_session();
    let s2 = db.create_session();
    (db, s1, s2)
}

fn setup_single_counter_table(db: &Database, sid: u64) {
    run_steps(
        db,
        &[
            Step::Exec {
                sid,
                sql: "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
                expect_err: None,
            },
            Step::Exec {
                sid,
                sql: "INSERT INTO t (id, v) VALUES (1, 10)",
                expect_err: None,
            },
        ],
    );
}

#[test]
fn test_phase5_multisession_dirty_read_allowed_read_uncommitted() {
    let (db, s1, s2) = new_db_with_sessions();
    setup_single_counter_table(&db, s1);

    run_steps(
        &db,
        &[
            Step::Exec {
                sid: s1,
                sql: "BEGIN TRANSACTION",
                expect_err: None,
            },
            Step::Exec {
                sid: s1,
                sql: "UPDATE t SET v = 99 WHERE id = 1",
                expect_err: None,
            },
            Step::Exec {
                sid: s2,
                sql: "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
                expect_err: None,
            },
            Step::QueryI64 {
                sid: s2,
                sql: "SELECT v FROM t WHERE id = 1",
                expected: 99,
            },
            Step::Exec {
                sid: s1,
                sql: "ROLLBACK",
                expect_err: None,
            },
            Step::QueryI64 {
                sid: s2,
                sql: "SELECT v FROM t WHERE id = 1",
                expected: 10,
            },
        ],
    );
}

#[test]
fn test_phase5_multisession_nonrepeatable_read_matrix() {
    let levels = [
        (IsolationLevel::ReadUncommitted, true, None),
        (IsolationLevel::ReadCommitted, true, None),
        (
            IsolationLevel::RepeatableRead,
            false,
            Some("lock conflict (no-wait)"),
        ),
        (
            IsolationLevel::Serializable,
            false,
            Some("lock conflict (no-wait)"),
        ),
        (
            IsolationLevel::Snapshot,
            false,
            Some("lock conflict (no-wait)"),
        ),
    ];

    for (level, should_change, update_err) in levels {
        let (db, s1, s2) = new_db_with_sessions();
        setup_single_counter_table(&db, s1);

        run_steps(
            &db,
            &[
                Step::Exec {
                    sid: s1,
                    sql: &format!("SET TRANSACTION ISOLATION LEVEL {}", iso_sql(level)),
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "BEGIN TRANSACTION",
                    expect_err: None,
                },
                Step::QueryI64 {
                    sid: s1,
                    sql: "SELECT v FROM t WHERE id = 1",
                    expected: 10,
                },
                Step::Exec {
                    sid: s2,
                    sql: "BEGIN TRANSACTION",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s2,
                    sql: "UPDATE t SET v = 20 WHERE id = 1",
                    expect_err: update_err,
                },
                Step::Exec {
                    sid: s2,
                    sql: "COMMIT",
                    expect_err: None,
                },
                Step::QueryI64 {
                    sid: s1,
                    sql: "SELECT v FROM t WHERE id = 1",
                    expected: if should_change { 20 } else { 10 },
                },
                Step::Exec {
                    sid: s1,
                    sql: "ROLLBACK",
                    expect_err: None,
                },
            ],
        );
    }
}

#[test]
fn test_phase5_multisession_phantom_read_matrix() {
    let levels = [
        (IsolationLevel::ReadUncommitted, true, None),
        (IsolationLevel::ReadCommitted, true, None),
        (
            IsolationLevel::RepeatableRead,
            false,
            Some("lock conflict (no-wait)"),
        ),
        (
            IsolationLevel::Serializable,
            false,
            Some("lock conflict (no-wait)"),
        ),
        (
            IsolationLevel::Snapshot,
            false,
            Some("lock conflict (no-wait)"),
        ),
    ];

    for (level, should_change, insert_err) in levels {
        let (db, s1, s2) = new_db_with_sessions();
        run_steps(
            &db,
            &[
                Step::Exec {
                    sid: s1,
                    sql: "CREATE TABLE p (id INT NOT NULL PRIMARY KEY, flag INT NOT NULL)",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "INSERT INTO p (id, flag) VALUES (1, 1)",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "INSERT INTO p (id, flag) VALUES (2, 0)",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: &format!("SET TRANSACTION ISOLATION LEVEL {}", iso_sql(level)),
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "BEGIN TRANSACTION",
                    expect_err: None,
                },
                Step::QueryI64 {
                    sid: s1,
                    sql: "SELECT COUNT(*) FROM p WHERE flag = 1",
                    expected: 1,
                },
                Step::Exec {
                    sid: s2,
                    sql: "BEGIN TRANSACTION",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s2,
                    sql: "INSERT INTO p (id, flag) VALUES (3, 1)",
                    expect_err: insert_err,
                },
                Step::Exec {
                    sid: s2,
                    sql: "COMMIT",
                    expect_err: None,
                },
                Step::QueryI64 {
                    sid: s1,
                    sql: "SELECT COUNT(*) FROM p WHERE flag = 1",
                    expected: if should_change { 2 } else { 1 },
                },
                Step::Exec {
                    sid: s1,
                    sql: "ROLLBACK",
                    expect_err: None,
                },
            ],
        );
    }
}

#[test]
fn test_phase5_mvcc_conflict_matrix_lost_update_and_write_skew() {
    let levels = [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
        IsolationLevel::Snapshot,
    ];

    for level in levels {
        let (db, s1, s2) = new_db_with_sessions();
        setup_single_counter_table(&db, s1);

        run_steps(
            &db,
            &[
                Step::Exec {
                    sid: s1,
                    sql: &format!("SET TRANSACTION ISOLATION LEVEL {}", iso_sql(level)),
                    expect_err: None,
                },
                Step::Exec {
                    sid: s2,
                    sql: &format!("SET TRANSACTION ISOLATION LEVEL {}", iso_sql(level)),
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "BEGIN TRANSACTION",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s2,
                    sql: "BEGIN TRANSACTION",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "UPDATE t SET v = 11 WHERE id = 1",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s2,
                    sql: "UPDATE t SET v = 15 WHERE id = 1",
                    expect_err: Some("lock conflict (no-wait)"),
                },
                Step::Exec {
                    sid: s2,
                    sql: "COMMIT",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "COMMIT",
                    expect_err: None,
                },
            ],
        );

        run_steps(
            &db,
            &[Step::QueryI64 {
                sid: s2,
                sql: "SELECT v FROM t WHERE id = 1",
                expected: 11,
            }],
        );
    }
}

#[test]
fn test_phase5_mvcc_write_skew_matrix() {
    let levels = [
        (IsolationLevel::ReadUncommitted, None, Some("lock conflict (no-wait)"), 1),
        (IsolationLevel::ReadCommitted, None, Some("lock conflict (no-wait)"), 1),
        (
            IsolationLevel::RepeatableRead,
            Some("lock conflict (no-wait)"),
            Some("lock conflict (no-wait)"),
            2,
        ),
        (
            IsolationLevel::Serializable,
            Some("lock conflict (no-wait)"),
            Some("lock conflict (no-wait)"),
            2,
        ),
        (
            IsolationLevel::Snapshot,
            Some("lock conflict (no-wait)"),
            Some("lock conflict (no-wait)"),
            2,
        ),
    ];

    for (level, s1_update_err, s2_update_err, expected_on_call) in levels {
        let (db, s1, s2) = new_db_with_sessions();
        run_steps(
            &db,
            &[
                Step::Exec {
                    sid: s1,
                    sql: "CREATE TABLE duty (id INT NOT NULL PRIMARY KEY, on_call INT NOT NULL)",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "INSERT INTO duty (id, on_call) VALUES (1, 1)",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "INSERT INTO duty (id, on_call) VALUES (2, 1)",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: &format!("SET TRANSACTION ISOLATION LEVEL {}", iso_sql(level)),
                    expect_err: None,
                },
                Step::Exec {
                    sid: s2,
                    sql: &format!("SET TRANSACTION ISOLATION LEVEL {}", iso_sql(level)),
                    expect_err: None,
                },
                Step::Exec {
                    sid: s1,
                    sql: "BEGIN TRANSACTION",
                    expect_err: None,
                },
                Step::Exec {
                    sid: s2,
                    sql: "BEGIN TRANSACTION",
                    expect_err: None,
                },
                Step::QueryI64 {
                    sid: s1,
                    sql: "SELECT COUNT(*) FROM duty WHERE on_call = 1",
                    expected: 2,
                },
                Step::QueryI64 {
                    sid: s2,
                    sql: "SELECT COUNT(*) FROM duty WHERE on_call = 1",
                    expected: 2,
                },
                Step::Exec {
                    sid: s1,
                    sql: "UPDATE duty SET on_call = 0 WHERE id = 1",
                    expect_err: s1_update_err,
                },
                Step::Exec {
                    sid: s2,
                    sql: "UPDATE duty SET on_call = 0 WHERE id = 2",
                    expect_err: s2_update_err,
                },
                Step::Exec {
                    sid: s2,
                    sql: "COMMIT",
                    expect_err: None,
                },
            ],
        );

        db.execute_session(s1, parse_sql("COMMIT").unwrap()).unwrap();

        run_steps(
            &db,
            &[Step::QueryI64 {
                sid: s2,
                sql: "SELECT COUNT(*) FROM duty WHERE on_call = 1",
                expected: expected_on_call,
            }],
        );
    }
}
