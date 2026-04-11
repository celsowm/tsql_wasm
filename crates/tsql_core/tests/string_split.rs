use tsql_core::{parse_sql, Engine};

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .unwrap_or_else(|_| panic!("execute failed: {}", sql))
        .expect("expected result")
}

#[test]
fn test_string_split_no_match() {
    let mut e = Engine::new();

    let r = query(&mut e, "SELECT * FROM STRING_SPLIT('abc', ',')");
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn test_string_split_comma_separator() {
    let mut e = Engine::new();

    let r = query(&mut e, "SELECT * FROM STRING_SPLIT('a,b,c', ',')");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], tsql_core::types::Value::VarChar("a".into()));
    assert_eq!(r.rows[1][0], tsql_core::types::Value::VarChar("b".into()));
    assert_eq!(r.rows[2][0], tsql_core::types::Value::VarChar("c".into()));
}

#[test]
fn test_string_split_pipe_separator() {
    let mut e = Engine::new();

    let r = query(&mut e, "SELECT * FROM STRING_SPLIT('x|y|z', '|')");
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn test_string_split_multi_char_separator() {
    let mut e = Engine::new();

    let r = query(&mut e, "SELECT * FROM STRING_SPLIT('a::b::c', '::')");
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn test_string_split_with_alias() {
    let mut e = Engine::new();

    let r = query(&mut e, "SELECT s.value FROM STRING_SPLIT('a,b', ',') AS s");
    assert_eq!(r.rows.len(), 2);
}

#[test]
fn test_string_split_ordinal_disabled() {
    let mut e = Engine::new();

    let r = query(&mut e, "SELECT value FROM STRING_SPLIT('a,b,c', ',', 0)");
    assert_eq!(r.columns.len(), 1);
    assert_eq!(r.columns[0], "value");
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn test_string_split_ordinal_enabled() {
    let mut e = Engine::new();

    let r = query(
        &mut e,
        "SELECT value, ordinal FROM STRING_SPLIT('a,b,c', ',', 1)",
    );
    assert_eq!(r.columns.len(), 2);
    assert_eq!(r.columns[0], "value");
    assert_eq!(r.columns[1], "ordinal");
    assert_eq!(r.rows.len(), 3);

    assert_eq!(r.rows[0][0], tsql_core::types::Value::VarChar("a".into()));
    assert_eq!(r.rows[0][1], tsql_core::types::Value::Int(1));

    assert_eq!(r.rows[1][0], tsql_core::types::Value::VarChar("b".into()));
    assert_eq!(r.rows[1][1], tsql_core::types::Value::Int(2));

    assert_eq!(r.rows[2][0], tsql_core::types::Value::VarChar("c".into()));
    assert_eq!(r.rows[2][1], tsql_core::types::Value::Int(3));
}

#[test]
fn test_string_split_ordinal_only() {
    let mut e = Engine::new();

    let r = query(&mut e, "SELECT ordinal FROM STRING_SPLIT('x|y', '|', 1)");
    assert_eq!(r.columns.len(), 1);
    assert_eq!(r.columns[0], "ordinal");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], tsql_core::types::Value::Int(1));
    assert_eq!(r.rows[1][0], tsql_core::types::Value::Int(2));
}
