use std::sync::Arc;
use std::thread;
use tsql_core::types::Value;
use tsql_core::{parse_sql, Database};

#[test]
fn test_concurrent_inserts_different_tables() {
    let db = Arc::new(Database::new());

    // Setup tables
    let s0 = db.create_session();
    db.execute_session(
        s0,
        parse_sql("CREATE TABLE t1 (id INT PRIMARY KEY)").unwrap(),
    )
    .unwrap();
    db.execute_session(
        s0,
        parse_sql("CREATE TABLE t2 (id INT PRIMARY KEY)").unwrap(),
    )
    .unwrap();
    db.close_session(s0).unwrap();

    let db1 = Arc::clone(&db);
    let h1 = thread::spawn(move || {
        let sid = db1.create_session();
        for i in 1..=50 {
            let sql = format!("INSERT INTO t1 (id) VALUES ({})", i);
            db1.execute_session(sid, parse_sql(&sql).unwrap()).unwrap();
        }
        db1.close_session(sid).unwrap();
    });

    let db2 = Arc::clone(&db);
    let h2 = thread::spawn(move || {
        let sid = db2.create_session();
        for i in 1..=50 {
            let sql = format!("INSERT INTO t2 (id) VALUES ({})", i);
            db2.execute_session(sid, parse_sql(&sql).unwrap()).unwrap();
        }
        db2.close_session(sid).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();

    // Verify results
    let s_final = db.create_session();
    let res1 = db
        .execute_session(s_final, parse_sql("SELECT COUNT(*) FROM t1").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(res1.rows[0][0], Value::BigInt(50));

    let res2 = db
        .execute_session(s_final, parse_sql("SELECT COUNT(*) FROM t2").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(res2.rows[0][0], Value::BigInt(50));
}

#[test]
fn test_concurrent_updates_same_table_lock_conflict() {
    let db = Arc::new(Database::new());

    // Setup table
    let s0 = db.create_session();
    db.execute_session(
        s0,
        parse_sql("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap(),
    )
    .unwrap();
    db.execute_session(
        s0,
        parse_sql("INSERT INTO t (id, v) VALUES (1, 10)").unwrap(),
    )
    .unwrap();
    db.close_session(s0).unwrap();

    let (tx1, rx1) = std::sync::mpsc::channel();
    let (tx2, rx2) = std::sync::mpsc::channel();

    let db1 = Arc::clone(&db);
    let h1 = thread::spawn(move || {
        let sid = db1.create_session();
        db1.execute_session(
            sid,
            parse_sql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap(),
        )
        .unwrap();
        db1.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
            .unwrap();
        db1.execute_session(sid, parse_sql("UPDATE t SET v = 20 WHERE id = 1").unwrap())
            .unwrap();

        tx1.send(()).unwrap(); // Signal that we hold the lock
        rx2.recv().unwrap(); // Wait for other thread to try and fail

        db1.execute_session(sid, parse_sql("COMMIT").unwrap())
            .unwrap();
        db1.close_session(sid).unwrap();
    });

    let db2 = Arc::clone(&db);
    let h2 = thread::spawn(move || {
        let sid = db2.create_session();
        rx1.recv().unwrap(); // Wait until thread 1 has the lock

        // This should fail immediately with no-wait policy
        db2.execute_session(
            sid,
            parse_sql("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap(),
        )
        .unwrap();
        db2.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
            .unwrap();
        let res = db2.execute_session(sid, parse_sql("UPDATE t SET v = 30 WHERE id = 1").unwrap());
        assert!(res.is_err(), "Expected lock conflict error but got success");
        assert!(res.unwrap_err().to_string().contains("lock conflict"));

        tx2.send(()).unwrap(); // Signal that we are done
        db2.close_session(sid).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();
}
