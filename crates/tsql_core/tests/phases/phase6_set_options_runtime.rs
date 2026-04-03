use tsql_core::types::Value;
use tsql_core::{parse_sql, Database, Engine};

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
fn test_ansi_nulls_on_null_equals_null() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET ANSI_NULLS ON");
    
    let result = query(&mut engine, "SELECT NULL = NULL");
    assert_eq!(result.rows[0][0], Value::Null);
}

#[test]
fn test_ansi_nulls_off_null_equals_null() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET ANSI_NULLS OFF");
    
    let result = query(&mut engine, "SELECT NULL = NULL");
    assert_eq!(result.rows[0][0], Value::Bit(true));
}

#[test]
fn test_ansi_nulls_on_null_equals_value() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET ANSI_NULLS ON");
    
    let result = query(&mut engine, "SELECT NULL = CAST(5 AS INT)");
    assert_eq!(result.rows[0][0], Value::Null);
}

#[test]
fn test_ansi_nulls_off_null_equals_value() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET ANSI_NULLS OFF");
    
    let result = query(&mut engine, "SELECT NULL = CAST(5 AS INT)");
    assert_eq!(result.rows[0][0], Value::Bit(false));
}

#[test]
fn test_ansi_nulls_on_null_not_equals_null() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET ANSI_NULLS ON");
    
    let result = query(&mut engine, "SELECT NULL <> NULL");
    assert_eq!(result.rows[0][0], Value::Null);
}

#[test]
fn test_ansi_nulls_off_null_not_equals_null() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET ANSI_NULLS OFF");
    
    let result = query(&mut engine, "SELECT NULL <> NULL");
    assert_eq!(result.rows[0][0], Value::Bit(false));
}

#[test]
fn test_ansi_nulls_affects_where_clause() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "CREATE TABLE t (v INT)");
    exec(&mut engine, "INSERT INTO t (v) VALUES (1), (2), (3)");
    
    exec(&mut engine, "SET ANSI_NULLS ON");
    let result = query(&mut engine, "SELECT COUNT(*) FROM t WHERE v = NULL");
    assert_eq!(result.rows[0][0], Value::BigInt(0));
    
    exec(&mut engine, "SET ANSI_NULLS OFF");
    let result = query(&mut engine, "SELECT COUNT(*) FROM t WHERE v = NULL");
    assert_eq!(result.rows[0][0], Value::BigInt(0));
}

#[test]
fn test_ansi_nulls_on_in_list_with_null() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET ANSI_NULLS ON");
    
    let result = query(&mut engine, "SELECT 5 IN (1, 2, NULL)");
    assert_eq!(result.rows[0][0], Value::Null);
    
    let result2 = query(&mut engine, "SELECT 5 NOT IN (1, 2, NULL)");
    assert_eq!(result2.rows[0][0], Value::Null);
}

#[test]
fn test_ansi_nulls_on_between_with_null() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET ANSI_NULLS ON");
    
    let result = query(&mut engine, "SELECT NULL BETWEEN 1 AND 10");
    assert_eq!(result.rows[0][0], Value::Null);
}

#[test]
fn test_ansi_nulls_session_isolation() {
    let db = Database::new();
    
    let session1 = db.create_session();
    let session2 = db.create_session();
    
    db.execute_session(session1, parse_sql("SET ANSI_NULLS ON").unwrap()).unwrap();
    db.execute_session(session2, parse_sql("SET ANSI_NULLS OFF").unwrap()).unwrap();
    
    let result1 = db.execute_session(session1, parse_sql("SELECT NULL = NULL").unwrap()).unwrap().unwrap();
    let result2 = db.execute_session(session2, parse_sql("SELECT NULL = NULL").unwrap()).unwrap().unwrap();
    
    assert_eq!(result1.rows[0][0], Value::Null);
    assert_eq!(result2.rows[0][0], Value::Bit(true));
    
    db.close_session(session1).unwrap();
    db.close_session(session2).unwrap();
}

#[test]
fn test_datefirst_monday() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET DATEFIRST 1");
    
    let result = query(&mut engine, "SELECT DATEPART(weekday, '2026-01-05')");
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn test_datefirst_sunday() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET DATEFIRST 7");
    
    let result = query(&mut engine, "SELECT DATEPART(weekday, '2026-01-04')");
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn test_datefirst_default() {
    let mut engine = Engine::new();
    
    let result = query(&mut engine, "SELECT DATEPART(dayofweek, '2026-01-04')");
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[test]
fn test_datename_weekday() {
    let mut engine = Engine::new();
    
    exec(&mut engine, "SET DATEFIRST 7");
    
    let result = query(&mut engine, "SELECT DATENAME(weekday, '2026-01-04')");
    assert_eq!(result.rows[0][0], Value::VarChar("Sunday".to_string()));
}

#[test]
fn test_datename_month() {
    let mut engine = Engine::new();
    
    let result = query(&mut engine, "SELECT DATENAME(month, '2026-03-15')");
    assert_eq!(result.rows[0][0], Value::VarChar("March".to_string()));
}
