use tsql_core::types::Value;
use tsql_core::{parse_sql, Engine};

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
fn test_regexp_like_true() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT REGEXP_LIKE('Hello World', '^Hello')");
    assert_eq!(result.rows[0][0], Value::Bit(true));
}

#[test]
fn test_regexp_like_false() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT REGEXP_LIKE('Hello World', '^World')");
    assert_eq!(result.rows[0][0], Value::Bit(false));
}

#[test]
fn test_regexp_like_case_insensitive() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT REGEXP_LIKE('Hello World', '^hello', 'i')");
    assert_eq!(result.rows[0][0], Value::Bit(true));
}

#[test]
fn test_regexp_replace() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        "SELECT REGEXP_REPLACE('Hello World 123', '\\d+', 'XXX')",
    );
    assert_eq!(
        result.rows[0][0],
        Value::NVarChar("Hello World XXX".to_string())
    );
}

#[test]
fn test_regexp_replace_all() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        "SELECT REGEXP_REPLACE('1 2 3 4', '\\d+', 'X')",
    );
    assert_eq!(result.rows[0][0], Value::NVarChar("X X X X".to_string()));
}

#[test]
fn test_regexp_substr_basic() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT REGEXP_SUBSTR('Hello 123 World', '\\d+')");
    assert_eq!(result.rows[0][0], Value::NVarChar("123".to_string()));
}

#[test]
fn test_regexp_substr_with_pos() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        "SELECT REGEXP_SUBSTR('abc 123 def 456', '\\d+', 8)",
    );
    assert_eq!(result.rows[0][0], Value::NVarChar("456".to_string()));
}

#[test]
fn test_regexp_substr_not_found() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT REGEXP_SUBSTR('Hello World', '\\d+')");
    assert!(result.rows[0][0].is_null());
}

#[test]
fn test_regexp_instr_basic() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        "SELECT REGEXP_INSTR('Hello 123 World', '\\d+')",
    );
    assert_eq!(result.rows[0][0], Value::Int(7));
}

#[test]
fn test_regexp_instr_end() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        "SELECT REGEXP_INSTR('Hello 123 World', '\\d+', 1, 0, 1)",
    );
    assert_eq!(result.rows[0][0], Value::Int(10));
}

#[test]
fn test_regexp_count() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        "SELECT REGEXP_COUNT('aaa bbb aaa ccc aaa', 'aaa')",
    );
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn test_regexp_count_with_pos() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        "SELECT REGEXP_COUNT('aaa bbb aaa ccc aaa', 'aaa', 5)",
    );
    assert_eq!(result.rows[0][0], Value::Int(2));
}

#[test]
fn test_regexp_in_where() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE t (id INT, email NVARCHAR(100))");
    exec(
        &mut engine,
        "INSERT INTO t (id, email) VALUES (1, 'user@example.com'), (2, 'invalid'), (3, 'admin@test.org')",
    );
    let result = query(
        &mut engine,
        "SELECT COUNT(*) FROM t WHERE REGEXP_LIKE(email, '^[^@]+@[^@]+\\.[^@]+$')",
    );
    assert_eq!(result.rows[0][0], Value::BigInt(2));
}

#[test]
fn test_regexp_replace_in_update() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE t (id INT, phone NVARCHAR(20))");
    exec(
        &mut engine,
        "INSERT INTO t (id, phone) VALUES (1, '123-456-7890'), (2, '987-654-3210')",
    );
    exec(
        &mut engine,
        "UPDATE t SET phone = REGEXP_REPLACE(phone, '-', '')",
    );
    let result = query(&mut engine, "SELECT phone FROM t ORDER BY id");
    assert_eq!(result.rows[0][0], Value::NVarChar("1234567890".to_string()));
    assert_eq!(result.rows[1][0], Value::NVarChar("9876543210".to_string()));
}

#[test]
fn test_unistr_basic() {
    let mut engine = Engine::new();
    let result = query(&mut engine, r"SELECT UNISTR('Hello \u0041')");
    assert_eq!(result.rows[0][0], Value::NVarChar("Hello A".to_string()));
}

#[test]
fn test_unistr_unicode() {
    let mut engine = Engine::new();
    let result = query(&mut engine, r"SELECT UNISTR('\u0048\u0065\u006C\u006C\u006F')");
    assert_eq!(result.rows[0][0], Value::NVarChar("Hello".to_string()));
}
