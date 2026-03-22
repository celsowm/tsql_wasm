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
fn test_isjson_valid() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT ISJSON('{}')");
    assert_eq!(result.rows[0][0], Value::Bit(true));
}

#[test]
fn test_isjson_invalid() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT ISJSON('not json')");
    assert_eq!(result.rows[0][0], Value::Bit(false));
}

#[test]
fn test_json_value_object() {
    let mut engine = Engine::new();
    let result = query(&mut engine, r#"SELECT JSON_VALUE('{"name": "test"}', '$.name')"#);
    assert_eq!(result.rows[0][0], Value::NVarChar("test".to_string()));
}

#[test]
fn test_json_value_number() {
    let mut engine = Engine::new();
    let result = query(&mut engine, r#"SELECT JSON_VALUE('{"value": 42}', '$.value')"#);
    assert_eq!(result.rows[0][0], Value::NVarChar("42".to_string()));
}

#[test]
fn test_json_value_nested() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        r#"SELECT JSON_VALUE('{"user": {"name": "John"}}', '$.user.name')"#,
    );
    assert_eq!(result.rows[0][0], Value::NVarChar("John".to_string()));
}

#[test]
fn test_json_value_not_found() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        r#"SELECT JSON_VALUE('{"name": "test"}', '$.missing')"#,
    );
    assert!(result.rows[0][0].is_null());
}

#[test]
fn test_json_query_object() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        r#"SELECT JSON_QUERY('{"obj": {"a": 1}}', '$.obj')"#,
    );
    assert!(matches!(result.rows[0][0], Value::NVarChar(_)));
    let val = result.rows[0][0].to_string_value();
    assert!(val.contains("a"));
}

#[test]
fn test_json_query_array() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        r#"SELECT JSON_QUERY('[1, 2, 3]', '$')"#,
    );
    assert!(matches!(result.rows[0][0], Value::NVarChar(_)));
    let val = result.rows[0][0].to_string_value();
    assert!(val.contains("[1,2,3]") || val.contains("[1, 2, 3]"));
}

#[test]
fn test_json_modify_simple() {
    let mut engine = Engine::new();
    let result = query(
        &mut engine,
        r#"SELECT JSON_MODIFY('{"name": "old"}', 'name', 'new')"#,
    );
    assert!(matches!(result.rows[0][0], Value::NVarChar(_)));
    let val = result.rows[0][0].to_string_value();
    assert!(val.contains("new"));
}

#[test]
fn test_json_array_length() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT JSON_ARRAY_LENGTH('[1, 2, 3]')");
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn test_json_array_length_empty() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT JSON_ARRAY_LENGTH('[]')");
    assert_eq!(result.rows[0][0], Value::Int(0));
}

#[test]
fn test_json_array_length_not_array() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT JSON_ARRAY_LENGTH('{}')");
    assert!(result.rows[0][0].is_null());
}

#[test]
fn test_json_keys_object() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT JSON_KEYS('{\"a\": 1, \"b\": 2}')");
    assert!(matches!(result.rows[0][0], Value::NVarChar(_)));
    let val = result.rows[0][0].to_string_value();
    assert!(val.contains("a"));
    assert!(val.contains("b"));
}

#[test]
fn test_json_keys_not_object() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT JSON_KEYS('[1, 2, 3]')");
    assert!(result.rows[0][0].is_null());
}

#[test]
fn test_json_value_from_table() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        r#"CREATE TABLE t (id INT, data NVARCHAR(500))"#,
    );
    exec(
        &mut engine,
        r#"INSERT INTO t (id, data) VALUES (1, '{"name": "Alice"}'), (2, '{"name": "Bob"}')"#,
    );
    let result = query(
        &mut engine,
        "SELECT JSON_VALUE(data, '$.name') FROM t ORDER BY id",
    );
    assert_eq!(result.rows[0][0], Value::NVarChar("Alice".to_string()));
    assert_eq!(result.rows[1][0], Value::NVarChar("Bob".to_string()));
}

#[test]
fn test_json_in_where() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        r#"CREATE TABLE t (id INT, data NVARCHAR(500))"#,
    );
    exec(
        &mut engine,
        r#"INSERT INTO t (id, data) VALUES (1, '{"active": true}'), (2, '{"active": false}')"#,
    );
    let result = query(
        &mut engine,
        "SELECT COUNT(*) FROM t WHERE JSON_VALUE(data, '$.active') = 'true'",
    );
    assert_eq!(result.rows[0][0], Value::BigInt(1));
}
