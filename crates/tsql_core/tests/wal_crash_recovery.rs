use tempfile::TempDir;
use tsql_core::types::Value;
use tsql_core::PersistentDatabase;

fn exec(db: &PersistentDatabase, sid: u64, sql: &str) {
    db.execute_session_batch_sql(sid, sql).unwrap();
}

fn query(db: &PersistentDatabase, sid: u64, sql: &str) -> tsql_core::QueryResult {
    db.execute_session_batch_sql(sid, sql).unwrap().unwrap()
}

#[test]
fn test_crash_recovery_basic_persistence() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path();

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        exec(&db, sid, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        exec(&db, sid, "INSERT INTO t VALUES (1, 100), (2, 200)");
    }

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        let result = query(&db, sid, "SELECT COUNT(*) FROM t");
        assert!(matches!(
            result.rows[0][0],
            Value::BigInt(2) | Value::Int(2)
        ));
    }
}

#[test]
fn test_crash_recovery_committed_transaction() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path();

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        exec(&db, sid, "CREATE TABLE t (id INT PRIMARY KEY)");
        exec(&db, sid, "INSERT INTO t VALUES (1)");
        exec(&db, sid, "BEGIN TRANSACTION");
        exec(&db, sid, "INSERT INTO t VALUES (2)");
        exec(&db, sid, "COMMIT");
    }

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        let result = query(&db, sid, "SELECT COUNT(*) FROM t");
        assert!(matches!(
            result.rows[0][0],
            Value::BigInt(2) | Value::Int(2)
        ));
    }
}

#[test]
fn test_crash_recovery_rolled_back_transaction() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path();

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        exec(&db, sid, "CREATE TABLE t (id INT PRIMARY KEY)");
        exec(&db, sid, "INSERT INTO t VALUES (1)");
        db.export_checkpoint().expect("checkpoint");
        exec(&db, sid, "BEGIN TRANSACTION");
        exec(&db, sid, "INSERT INTO t VALUES (2)");
        exec(&db, sid, "ROLLBACK");
    }

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        let result = query(&db, sid, "SELECT COUNT(*) FROM t");
        assert!(matches!(
            result.rows[0][0],
            Value::BigInt(1) | Value::Int(1)
        ));
    }
}

#[test]
fn test_crash_recovery_multiple_tables() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path();

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        exec(&db, sid, "CREATE TABLE a (id INT PRIMARY KEY)");
        exec(&db, sid, "CREATE TABLE b (id INT PRIMARY KEY)");
        exec(&db, sid, "INSERT INTO a VALUES (1)");
        exec(&db, sid, "INSERT INTO b VALUES (1)");
    }

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        let result_a = query(&db, sid, "SELECT COUNT(*) FROM a");
        let result_b = query(&db, sid, "SELECT COUNT(*) FROM b");
        assert!(matches!(
            result_a.rows[0][0],
            Value::BigInt(1) | Value::Int(1)
        ));
        assert!(matches!(
            result_b.rows[0][0],
            Value::BigInt(1) | Value::Int(1)
        ));
    }
}

#[test]
fn test_crash_recovery_update_delete() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path();

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        exec(&db, sid, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        exec(&db, sid, "INSERT INTO t VALUES (1, 100)");
        exec(&db, sid, "UPDATE t SET val = 200 WHERE id = 1");
        exec(&db, sid, "DELETE FROM t WHERE id = 1");
    }

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        let result = query(&db, sid, "SELECT COUNT(*) FROM t");
        assert!(matches!(
            result.rows[0][0],
            Value::BigInt(0) | Value::Int(0)
        ));
    }
}

#[test]
fn test_crash_recovery_sequential_restarts() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path();

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        exec(&db, sid, "CREATE TABLE t (id INT PRIMARY KEY)");
        for i in 1..=5 {
            exec(&db, sid, &format!("INSERT INTO t VALUES ({})", i));
        }
    }

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        let result = query(&db, sid, "SELECT COUNT(*) FROM t");
        assert!(matches!(
            result.rows[0][0],
            Value::BigInt(5) | Value::Int(5)
        ));
        exec(&db, sid, "INSERT INTO t VALUES (6)");
    }

    {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        let result = query(&db, sid, "SELECT COUNT(*) FROM t");
        assert!(matches!(
            result.rows[0][0],
            Value::BigInt(6) | Value::Int(6)
        ));
    }
}

#[test]
fn test_crash_recovery_checkpoint_export_import() {
    let dir = TempDir::new().expect("temp dir");
    let path = dir.path();

    let checkpoint = {
        let db = PersistentDatabase::new_persistent(path).expect("db");
        let sid = db.create_session();
        exec(&db, sid, "CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        exec(&db, sid, "INSERT INTO t VALUES (1, 100), (2, 200)");
        db.export_checkpoint().expect("export")
    };

    {
        let dir2 = TempDir::new().expect("temp dir 2");
        let db = PersistentDatabase::new_persistent(dir2.path()).expect("db");
        db.import_checkpoint(&checkpoint).expect("import");
        let sid = db.create_session();
        let result = query(&db, sid, "SELECT COUNT(*) FROM t");
        assert!(matches!(
            result.rows[0][0],
            Value::BigInt(2) | Value::Int(2)
        ));
    }
}
