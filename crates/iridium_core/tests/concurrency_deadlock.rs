use iridium_core::error::DbError;
use iridium_core::executor::engine::Engine;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_deadlock_2_sessions() {
    let engine = Arc::new(Engine::new());

    // Setup tables
    engine.exec("CREATE TABLE A (id INT)").unwrap();
    engine.exec("CREATE TABLE B (id INT)").unwrap();

    let s1 = engine.create_session();
    let s2 = engine.create_session();

    // Set lock timeout so they wait and can be deadlocked
    engine
        .execute_session_batch_sql(s1, "SET LOCK_TIMEOUT 5000;")
        .unwrap();
    engine
        .execute_session_batch_sql(s2, "SET LOCK_TIMEOUT 5000;")
        .unwrap();

    // Session 1: Begin Tran, Lock A
    engine
        .execute_session_batch_sql(s1, "BEGIN TRANSACTION; INSERT INTO A VALUES (1);")
        .unwrap();

    // Session 2: Begin Tran, Lock B
    engine
        .execute_session_batch_sql(s2, "BEGIN TRANSACTION; INSERT INTO B VALUES (1);")
        .unwrap();

    let (tx, rx) = std::sync::mpsc::channel();

    // Session 1 tries to lock B (will block)
    let engine_s1 = engine.clone();
    thread::spawn(move || {
        let res = engine_s1.execute_session_batch_sql(s1, "INSERT INTO B VALUES (2);");
        tx.send(res).unwrap();
    });

    // Wait a bit to ensure S1 is waiting
    thread::sleep(Duration::from_millis(100));

    // Session 2 tries to lock A (DEADLOCK!)
    let res_s2 = engine.execute_session_batch_sql(s2, "INSERT INTO A VALUES (2);");

    let res_s1 = rx.recv_timeout(Duration::from_secs(2)).unwrap();

    // One of them must be a deadlock victim
    let s1_deadlock = matches!(res_s1, Err(DbError::Deadlock(_)));
    let s2_deadlock = matches!(res_s2, Err(DbError::Deadlock(_)));

    assert!(
        s1_deadlock || s2_deadlock,
        "One session should have deadlocked. S1: {:?}, S2: {:?}",
        res_s1,
        res_s2
    );

    // The victim's transaction should be rolled back.
    if s1_deadlock {
        // S1 was victim, B should be unlocked, so S2 should have succeeded (eventually)
        assert!(res_s2.is_ok());
    } else {
        // S2 was victim, A should be unlocked, so S1 should have succeeded (eventually)
        assert!(res_s1.is_ok());
    }
}

#[test]
fn test_deadlock_3_sessions() {
    let engine = Arc::new(Engine::new());

    // Setup tables
    engine.exec("CREATE TABLE A (id INT)").unwrap();
    engine.exec("CREATE TABLE B (id INT)").unwrap();
    engine.exec("CREATE TABLE C (id INT)").unwrap();

    let s1 = engine.create_session();
    let s2 = engine.create_session();
    let s3 = engine.create_session();

    // Set lock timeout
    engine
        .execute_session_batch_sql(s1, "SET LOCK_TIMEOUT 5000;")
        .unwrap();
    engine
        .execute_session_batch_sql(s2, "SET LOCK_TIMEOUT 5000;")
        .unwrap();
    engine
        .execute_session_batch_sql(s3, "SET LOCK_TIMEOUT 5000;")
        .unwrap();

    // S1 locks A
    engine
        .execute_session_batch_sql(s1, "BEGIN TRANSACTION; INSERT INTO A VALUES (1);")
        .unwrap();
    // S2 locks B
    engine
        .execute_session_batch_sql(s2, "BEGIN TRANSACTION; INSERT INTO B VALUES (1);")
        .unwrap();
    // S3 locks C
    engine
        .execute_session_batch_sql(s3, "BEGIN TRANSACTION; INSERT INTO C VALUES (1);")
        .unwrap();

    let (tx1, rx1) = std::sync::mpsc::channel();
    let (tx2, rx2) = std::sync::mpsc::channel();

    // S1 waits for B (held by S2)
    let e1 = engine.clone();
    thread::spawn(move || {
        tx1.send(e1.execute_session_batch_sql(s1, "INSERT INTO B VALUES (2);"))
            .unwrap();
    });
    thread::sleep(Duration::from_millis(100));

    // S2 waits for C (held by S3)
    let e2 = engine.clone();
    thread::spawn(move || {
        tx2.send(e2.execute_session_batch_sql(s2, "INSERT INTO C VALUES (2);"))
            .unwrap();
    });
    thread::sleep(Duration::from_millis(100));

    // S3 waits for A (held by S1) -> CYCLE: S1->S2->S3->S1
    let res_s3 = engine.execute_session_batch_sql(s3, "INSERT INTO A VALUES (2);");

    let res_s1 = rx1
        .recv_timeout(Duration::from_secs(5))
        .expect("S1 failed to report");
    let res_s2 = rx2
        .recv_timeout(Duration::from_secs(5))
        .expect("S2 failed to report");

    let deadlocks = [res_s1, res_s2, res_s3]
        .iter()
        .filter(|r| matches!(r, Err(DbError::Deadlock(_))))
        .count();
    assert!(
        deadlocks >= 1,
        "At least one session should have deadlocked"
    );
}

#[test]
fn test_deadlock_priority_prefers_lower_priority_victim() {
    let engine = Arc::new(Engine::new());

    engine.exec("CREATE TABLE A (id INT)").unwrap();
    engine.exec("CREATE TABLE B (id INT)").unwrap();

    let s1 = engine.create_session();
    let s2 = engine.create_session();

    engine
        .execute_session_batch_sql(s1, "SET LOCK_TIMEOUT 5000; SET DEADLOCK_PRIORITY LOW;")
        .unwrap();
    engine
        .execute_session_batch_sql(s2, "SET LOCK_TIMEOUT 5000; SET DEADLOCK_PRIORITY HIGH;")
        .unwrap();

    engine
        .execute_session_batch_sql(s1, "BEGIN TRANSACTION; INSERT INTO A VALUES (1);")
        .unwrap();
    engine
        .execute_session_batch_sql(s2, "BEGIN TRANSACTION; INSERT INTO B VALUES (1);")
        .unwrap();

    let (tx, rx) = std::sync::mpsc::channel();

    let engine_s1 = engine.clone();
    thread::spawn(move || {
        let res = engine_s1.execute_session_batch_sql(s1, "INSERT INTO B VALUES (2);");
        tx.send(res).unwrap();
    });

    thread::sleep(Duration::from_millis(100));

    let res_s2 = engine.execute_session_batch_sql(s2, "INSERT INTO A VALUES (2);");
    let res_s1 = rx.recv_timeout(Duration::from_secs(5)).unwrap();

    assert!(matches!(res_s1, Err(DbError::Deadlock(_))));
    assert!(res_s2.is_ok());
}
