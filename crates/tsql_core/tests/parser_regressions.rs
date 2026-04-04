use tsql_core::{types::Value, Engine, parse_sql};

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

#[test]
fn test_qualified_wildcard_parse() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT t.* FROM (SELECT 1 as a) AS t");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.columns, vec!["a"]);
    assert_eq!(r.rows[0][0], Value::Int(1));
}

#[test]
fn test_values_subquery_cross_apply() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "
        SELECT t.*
        FROM (SELECT 1 as id) as base
        CROSS APPLY (
            VALUES (1, 'foo'), (2, 'bar')
        ) AS t(col1, col2)
        ORDER BY t.col1
    ",
    );

    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.columns, vec!["col1", "col2"]);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("foo".to_string()));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[1][1], Value::VarChar("bar".to_string()));
}
