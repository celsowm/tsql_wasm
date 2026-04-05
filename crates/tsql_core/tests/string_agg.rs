use tsql_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect(&format!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .expect(&format!("execute failed: {}", sql));
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).expect(&format!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .expect(&format!("execute failed: {}", sql))
        .expect("expected result")
}

#[test]
fn test_string_agg_basic() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, val VARCHAR(10))");
    exec(&mut e, "INSERT INTO t VALUES (1, 'a')");
    exec(&mut e, "INSERT INTO t VALUES (2, 'b')");
    exec(&mut e, "INSERT INTO t VALUES (3, 'c')");

    let r = query(&mut e, "SELECT STRING_AGG(val, ',') FROM t");
    println!("test_string_agg_basic: {:?}", r);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("a,b,c".to_string()));
}

#[test]
fn test_string_agg_with_group_by() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (dept INT, name VARCHAR(10))");
    exec(&mut e, "INSERT INTO t VALUES (1, 'Alice')");
    exec(&mut e, "INSERT INTO t VALUES (1, 'Bob')");
    exec(&mut e, "INSERT INTO t VALUES (2, 'Carol')");
    exec(&mut e, "INSERT INTO t VALUES (2, 'Dave')");

    let r = query(
        &mut e,
        "SELECT dept, STRING_AGG(name, ';') FROM t GROUP BY dept ORDER BY dept",
    );
    println!("test_string_agg_with_group_by: {:?}", r);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("Alice;Bob".to_string()));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[1][1], Value::VarChar("Carol;Dave".to_string()));
}

#[test]
fn test_string_agg_null_values_skipped() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, val VARCHAR(10))");
    exec(&mut e, "INSERT INTO t VALUES (1, 'a')");
    exec(&mut e, "INSERT INTO t VALUES (2, NULL)");
    exec(&mut e, "INSERT INTO t VALUES (3, 'b')");

    let r = query(&mut e, "SELECT STRING_AGG(val, ',') FROM t");
    println!("test_string_agg_null_values_skipped: {:?}", r);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("a,b".to_string()));
}

#[test]
fn test_string_agg_empty_group_returns_null() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (dept INT, val VARCHAR(10))");
    exec(&mut e, "INSERT INTO t VALUES (1, 'a')");

    // When filtering to a non-existent dept, no rows are returned (not NULL)
    let r = query(
        &mut e,
        "SELECT dept, STRING_AGG(val, ',') FROM t WHERE dept = 99 GROUP BY dept",
    );
    println!("test_string_agg_empty_group_returns_null: {:?}", r);
    // No rows returned for empty group - this is SQL Server behavior
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn test_string_agg_different_separators() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (val VARCHAR(10))");
    exec(&mut e, "INSERT INTO t VALUES ('a')");
    exec(&mut e, "INSERT INTO t VALUES ('b')");

    let r = query(&mut e, "SELECT STRING_AGG(val, ' | ') FROM t");
    println!("test_string_agg_different_separators: {:?}", r);
    assert_eq!(r.rows[0][0], Value::VarChar("a | b".to_string()));
}
