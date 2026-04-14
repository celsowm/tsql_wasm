use chrono::Datelike;
use iridium_core::types::Value;
use iridium_core::{parse_sql, Engine};

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

#[test]
fn test_current_date_returns_date() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT CURRENT_DATE");
    match &result.rows[0][0] {
        Value::Date(d) => {
            assert!(d.year() > 1900);
            assert!(d.month() >= 1 && d.month() <= 12);
            assert!(d.day() >= 1 && d.day() <= 31);
        }
        other => panic!("Expected Date, got {:?}", other),
    }
}

#[test]
fn test_current_date_format() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT CURRENT_DATE");
    let date_str = result.rows[0][0].to_string_value();
    // Should be in YYYY-MM-DD format
    assert!(date_str.len() == 10);
    let parts: Vec<&str> = date_str.split('-').collect();
    assert_eq!(parts.len(), 3);
}

#[test]
fn test_current_date_not_null() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT CURRENT_DATE");
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_current_date_in_insert() {
    let mut engine = Engine::new();
    engine
        .execute(parse_sql("CREATE TABLE t (id INT, created DATE)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO t (id, created) VALUES (1, CURRENT_DATE)").unwrap())
        .unwrap();
    let result = query(&mut engine, "SELECT created FROM t");
    match &result.rows[0][0] {
        Value::Date(d) => assert!(d.year() > 1900),
        Value::Null => panic!("CURRENT_DATE returned NULL in INSERT"),
        other => panic!("Expected Date, got {:?}", other),
    }
}

