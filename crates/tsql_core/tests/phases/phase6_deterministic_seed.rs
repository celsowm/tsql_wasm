use tsql_core::types::Value;
use tsql_core::{parse_sql, Database, Engine};

#[allow(dead_code)]
fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_sql(sql).expect("parse")).expect("exec");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

#[test]
fn test_rand_basic() {
    let mut engine = Engine::new();
    
    let result = query(&mut engine, "SELECT RAND()");
    assert!(matches!(result.rows[0][0], Value::Decimal(_, _)));
}

#[test]
fn test_newid_basic() {
    let mut engine = Engine::new();
    
    let result = query(&mut engine, "SELECT NEWID()");
    assert!(matches!(result.rows[0][0], Value::UniqueIdentifier(_)));
}

#[test]
fn test_deterministic_seed_rand() {
    let db = Database::new();
    
    let session1 = db.create_session();
    let session2 = db.create_session();
    
    db.set_session_seed(session1, 42).unwrap();
    db.set_session_seed(session2, 42).unwrap();
    
    let r1 = db
        .execute_session(session1, parse_sql("SELECT RAND()").unwrap())
        .unwrap();
    let r2 = db
        .execute_session(session2, parse_sql("SELECT RAND()").unwrap())
        .unwrap();
    
    assert_eq!(r1.unwrap().rows[0][0], r2.unwrap().rows[0][0]);
    
    db.close_session(session1).unwrap();
    db.close_session(session2).unwrap();
}

#[test]
fn test_deterministic_seed_newid() {
    let db = Database::new();
    
    let session1 = db.create_session();
    let session2 = db.create_session();
    
    db.set_session_seed(session1, 123).unwrap();
    db.set_session_seed(session2, 123).unwrap();
    
    let r1 = db
        .execute_session(session1, parse_sql("SELECT NEWID()").unwrap())
        .unwrap();
    let r2 = db
        .execute_session(session2, parse_sql("SELECT NEWID()").unwrap())
        .unwrap();
    
    assert_eq!(r1.unwrap().rows[0][0], r2.unwrap().rows[0][0]);
    
    db.close_session(session1).unwrap();
    db.close_session(session2).unwrap();
}

#[test]
fn test_different_seeds_produce_different_values() {
    let db = Database::new();
    
    let session1 = db.create_session();
    let session2 = db.create_session();
    
    db.set_session_seed(session1, 42).unwrap();
    db.set_session_seed(session2, 100).unwrap();
    
    let r1 = db
        .execute_session(session1, parse_sql("SELECT RAND()").unwrap())
        .unwrap();
    let r2 = db
        .execute_session(session2, parse_sql("SELECT RAND()").unwrap())
        .unwrap();
    
    assert_ne!(r1.unwrap().rows[0][0], r2.unwrap().rows[0][0]);
    
    db.close_session(session1).unwrap();
    db.close_session(session2).unwrap();
}

#[test]
fn test_multiple_calls_sequential() {
    let db = Database::new();
    
    let session1 = db.create_session();
    let session2 = db.create_session();
    
    db.set_session_seed(session1, 42).unwrap();
    db.set_session_seed(session2, 42).unwrap();
    
    for _ in 0..5 {
        let r1 = db
            .execute_session(session1, parse_sql("SELECT RAND()").unwrap())
            .unwrap();
        let r2 = db
            .execute_session(session2, parse_sql("SELECT RAND()").unwrap())
            .unwrap();
        
        assert_eq!(r1.unwrap().rows[0][0], r2.unwrap().rows[0][0]);
    }
    
    db.close_session(session1).unwrap();
    db.close_session(session2).unwrap();
}
