use iridium_core::{parse_sql, Database};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_lock_timeout_zero_fails_immediately() {
    let db = Arc::new(Database::new());

    // Setup table
    let s0 = db.create_session();
    db.execute_session(
        s0,
        parse_sql("CREATE TABLE t (id INT PRIMARY KEY)").unwrap(),
    )
    .unwrap();
    db.execute_session(s0, parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();
    db.close_session(s0).unwrap();

    let (tx1, rx1) = std::sync::mpsc::channel();
    let (tx2, rx2) = std::sync::mpsc::channel();

    let db1 = Arc::clone(&db);
    let h1 = thread::spawn(move || {
        let sid = db1.create_session();
        db1.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
            .unwrap();
        db1.execute_session(sid, parse_sql("UPDATE t SET id = 1 WHERE id = 1").unwrap())
            .unwrap();

        tx1.send(()).unwrap(); // Hold the lock
        rx2.recv().unwrap(); // Wait for other thread to try

        db1.execute_session(sid, parse_sql("COMMIT").unwrap())
            .unwrap();
        db1.close_session(sid).unwrap();
    });

    let db2 = Arc::clone(&db);
    let h2 = thread::spawn(move || {
        let sid = db2.create_session();
        rx1.recv().unwrap(); // Wait for h1 to hold lock

        db2.execute_session(sid, parse_sql("SET LOCK_TIMEOUT 0").unwrap())
            .unwrap();
        let start = Instant::now();
        let res = db2.execute_session(sid, parse_sql("UPDATE t SET id = 2 WHERE id = 1").unwrap());
        let elapsed = start.elapsed();

        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("lock conflict"));
        assert!(elapsed < Duration::from_millis(50));

        tx2.send(()).unwrap();
        db2.close_session(sid).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();
}

#[test]
fn test_lock_timeout_wait_success() {
    let db = Arc::new(Database::new());

    let s0 = db.create_session();
    db.execute_session(
        s0,
        parse_sql("CREATE TABLE t (id INT PRIMARY KEY)").unwrap(),
    )
    .unwrap();
    db.execute_session(s0, parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();
    db.close_session(s0).unwrap();

    let (tx1, rx1) = std::sync::mpsc::channel();

    let db1 = Arc::clone(&db);
    let h1 = thread::spawn(move || {
        let sid = db1.create_session();
        db1.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
            .unwrap();
        db1.execute_session(sid, parse_sql("UPDATE t SET id = 1 WHERE id = 1").unwrap())
            .unwrap();

        tx1.send(()).unwrap();
        thread::sleep(Duration::from_millis(200));

        db1.execute_session(sid, parse_sql("COMMIT").unwrap())
            .unwrap();
        db1.close_session(sid).unwrap();
    });

    let db2 = Arc::clone(&db);
    let h2 = thread::spawn(move || {
        let sid = db2.create_session();
        rx1.recv().unwrap();

        db2.execute_session(sid, parse_sql("SET LOCK_TIMEOUT 1000").unwrap())
            .unwrap();
        let start = Instant::now();
        db2.execute_session(sid, parse_sql("UPDATE t SET id = 2 WHERE id = 1").unwrap())
            .unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(200));
        db2.close_session(sid).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();
}

#[test]
fn test_lock_timeout_wait_fail() {
    let db = Arc::new(Database::new());

    let s0 = db.create_session();
    db.execute_session(
        s0,
        parse_sql("CREATE TABLE t (id INT PRIMARY KEY)").unwrap(),
    )
    .unwrap();
    db.execute_session(s0, parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();
    db.close_session(s0).unwrap();

    let (tx1, rx1) = std::sync::mpsc::channel();
    let (tx2, rx2) = std::sync::mpsc::channel();

    let db1 = Arc::clone(&db);
    let h1 = thread::spawn(move || {
        let sid = db1.create_session();
        db1.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
            .unwrap();
        db1.execute_session(sid, parse_sql("UPDATE t SET id = 1 WHERE id = 1").unwrap())
            .unwrap();

        tx1.send(()).unwrap();
        rx2.recv().unwrap();

        db1.execute_session(sid, parse_sql("COMMIT").unwrap())
            .unwrap();
        db1.close_session(sid).unwrap();
    });

    let db2 = Arc::clone(&db);
    let h2 = thread::spawn(move || {
        let sid = db2.create_session();
        rx1.recv().unwrap();

        db2.execute_session(sid, parse_sql("SET LOCK_TIMEOUT 100").unwrap())
            .unwrap();
        let start = Instant::now();
        let res = db2.execute_session(sid, parse_sql("UPDATE t SET id = 2 WHERE id = 1").unwrap());
        let elapsed = start.elapsed();

        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("lock timeout"));
        assert!(elapsed >= Duration::from_millis(100));

        tx2.send(()).unwrap();
        db2.close_session(sid).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();
}

#[test]
fn test_lock_timeout_infinite_wait() {
    let db = Arc::new(Database::new());

    let s0 = db.create_session();
    db.execute_session(
        s0,
        parse_sql("CREATE TABLE t (id INT PRIMARY KEY)").unwrap(),
    )
    .unwrap();
    db.execute_session(s0, parse_sql("INSERT INTO t (id) VALUES (1)").unwrap())
        .unwrap();
    db.close_session(s0).unwrap();

    let (tx1, rx1) = std::sync::mpsc::channel();

    let db1 = Arc::clone(&db);
    let h1 = thread::spawn(move || {
        let sid = db1.create_session();
        db1.execute_session(sid, parse_sql("BEGIN TRANSACTION").unwrap())
            .unwrap();
        db1.execute_session(sid, parse_sql("UPDATE t SET id = 1 WHERE id = 1").unwrap())
            .unwrap();

        tx1.send(()).unwrap();
        thread::sleep(Duration::from_millis(300));

        db1.execute_session(sid, parse_sql("COMMIT").unwrap())
            .unwrap();
        db1.close_session(sid).unwrap();
    });

    let db2 = Arc::clone(&db);
    let h2 = thread::spawn(move || {
        let sid = db2.create_session();
        rx1.recv().unwrap();

        db2.execute_session(sid, parse_sql("SET LOCK_TIMEOUT -1").unwrap())
            .unwrap();
        let start = Instant::now();
        db2.execute_session(sid, parse_sql("UPDATE t SET id = 2 WHERE id = 1").unwrap())
            .unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(300));
        db2.close_session(sid).unwrap();
    });

    h1.join().unwrap();
    h2.join().unwrap();
}
