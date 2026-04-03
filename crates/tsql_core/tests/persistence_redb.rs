use tsql_core::PersistentDatabase;
use tsql_core::types::Value;
use tempfile::tempdir;

#[test]
fn test_redb_persistence_roundtrip() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    // First session: create and insert
    {
        let db = PersistentDatabase::new_persistent(db_path).expect("failed to create db");
        let sid = db.create_session();

        db.execute_session_batch_sql(sid, "CREATE TABLE persist_test (id INT PRIMARY KEY, name NVARCHAR(100))").unwrap();
        db.execute_session_batch_sql(sid, "INSERT INTO persist_test (id, name) VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

        let res = db.execute_session_batch_sql(sid, "SELECT COUNT(*) FROM persist_test").unwrap().unwrap();
        assert_eq!(res.rows[0][0], Value::BigInt(2));

        db.close_session(sid).unwrap();
    }

    // Second session: reopen and verify
    {
        let db = PersistentDatabase::new_persistent(db_path).expect("failed to reopen db");
        let sid = db.create_session();

        let res = db.execute_session_batch_sql(sid, "SELECT name FROM persist_test ORDER BY id").unwrap().unwrap();
        assert_eq!(res.rows.len(), 2);
        assert_eq!(res.rows[0][0], Value::NVarChar("Alice".to_string()));
        assert_eq!(res.rows[1][0], Value::NVarChar("Bob".to_string()));

        db.close_session(sid).unwrap();
    }
}

#[test]
fn test_redb_persistence_update_delete() {
    let dir = tempdir().unwrap();
    let db_path = dir.path();

    // Initial setup
    {
        let db = PersistentDatabase::new_persistent(db_path).expect("failed to create db");
        let sid = db.create_session();
        db.execute_session_batch_sql(sid, "CREATE TABLE persist_test (id INT PRIMARY KEY, val INT)").unwrap();
        db.execute_session_batch_sql(sid, "INSERT INTO persist_test (id, val) VALUES (1, 10), (2, 20)").unwrap();
        db.close_session(sid).unwrap();
    }

    // Update and reopen
    {
        let db = PersistentDatabase::new_persistent(db_path).expect("failed to reopen db");
        let sid = db.create_session();
        db.execute_session_batch_sql(sid, "UPDATE persist_test SET val = 15 WHERE id = 1").unwrap();
        db.execute_session_batch_sql(sid, "DELETE FROM persist_test WHERE id = 2").unwrap();
        db.close_session(sid).unwrap();
    }

    // Final verify
    {
        let db = PersistentDatabase::new_persistent(db_path).expect("failed to reopen db");
        let sid = db.create_session();
        let res = db.execute_session_batch_sql(sid, "SELECT val FROM persist_test").unwrap().unwrap();
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0][0], Value::Int(15));
        db.close_session(sid).unwrap();
    }
}
