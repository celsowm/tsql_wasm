use tsql_core::types::Value;
use tsql_core::{parse_sql, Engine};

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

#[test]
fn test_edit_distance_identical() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT EDIT_DISTANCE('hello', 'hello')");
    assert_eq!(result.rows[0][0], Value::Int(0));
}

#[test]
fn test_edit_distance_insertions() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT EDIT_DISTANCE('abc', 'abcde')");
    assert_eq!(result.rows[0][0], Value::Int(2));
}

#[test]
fn test_edit_distance_deletions() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT EDIT_DISTANCE('abcde', 'abc')");
    assert_eq!(result.rows[0][0], Value::Int(2));
}

#[test]
fn test_edit_distance_substitutions() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT EDIT_DISTANCE('kitten', 'sitting')");
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn test_edit_distance_empty() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT EDIT_DISTANCE('', 'abc')");
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[test]
fn test_edit_distance_similarity_identical() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT EDIT_DISTANCE_SIMILARITY('hello', 'hello')");
    match &result.rows[0][0] {
        Value::Decimal(v, _) => assert!(*v > 999_000_000), // ~1.0
        _ => panic!("Expected Decimal"),
    }
}

#[test]
fn test_edit_distance_similarity_different() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT EDIT_DISTANCE_SIMILARITY('abc', 'xyz')");
    match &result.rows[0][0] {
        Value::Decimal(v, _) => assert!(*v < 100_000_000), // < 0.1
        _ => panic!("Expected Decimal"),
    }
}

#[test]
fn test_jaro_winkler_similarity_identical() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT JARO_WINKLER_SIMILARITY('hello', 'hello')");
    match &result.rows[0][0] {
        Value::Decimal(v, _) => assert!(*v > 999_000_000), // ~1.0
        _ => panic!("Expected Decimal"),
    }
}

#[test]
fn test_jaro_winkler_similarity_similar() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT JARO_WINKLER_SIMILARITY('MARTHA', 'MARHTA')");
    match &result.rows[0][0] {
        Value::Decimal(v, _) => assert!(*v > 900_000_000), // > 0.9
        _ => panic!("Expected Decimal"),
    }
}

#[test]
fn test_jaro_winkler_distance_identical() {
    let mut engine = Engine::new();
    let result = query(&mut engine, "SELECT JARO_WINKLER_DISTANCE('hello', 'hello')");
    match &result.rows[0][0] {
        Value::Decimal(v, _) => assert!(*v < 1_000_000), // ~0.0
        _ => panic!("Expected Decimal"),
    }
}

#[test]
fn test_fuzzy_in_where() {
    let mut engine = Engine::new();
    engine.execute(parse_sql("CREATE TABLE names (id INT, name NVARCHAR(100))").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO names (id, name) VALUES (1, 'John Smith'), (2, 'Jon Smyth'), (3, 'Jane Doe')").unwrap()).unwrap();
    
    let result = query(&mut engine, "SELECT COUNT(*) FROM names WHERE EDIT_DISTANCE(name, 'John Smith') <= 2");
    assert_eq!(result.rows[0][0], Value::BigInt(2)); // 'John Smith' and 'Jon Smyth'
}

#[test]
fn test_fuzzy_join() {
    let mut engine = Engine::new();
    engine.execute(parse_sql("CREATE TABLE customers (id INT, name NVARCHAR(100))").unwrap()).unwrap();
    engine.execute(parse_sql("CREATE TABLE orders (id INT, customer_name NVARCHAR(100))").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO customers (id, name) VALUES (1, 'John Smith')").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO orders (id, customer_name) VALUES (1, 'Jon Smyth'), (2, 'Jane Doe')").unwrap()).unwrap();
    
    // Just verify the query runs without error and returns results
    let result = query(&mut engine, "SELECT c.name, o.customer_name, EDIT_DISTANCE(c.name, o.customer_name) AS dist FROM customers c, orders o");
    assert!(!result.rows.is_empty());
}
