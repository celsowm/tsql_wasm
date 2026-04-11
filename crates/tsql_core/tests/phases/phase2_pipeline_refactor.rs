use tsql_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("parse failed: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("execute failed: {}", sql));
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .unwrap_or_else(|_| panic!("execute failed: {}", sql))
        .expect("expected result")
}

#[test]
fn test_order_by_simple_post_sort() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE nums (id INT)");
    exec(&mut e, "INSERT INTO nums VALUES (3)");
    exec(&mut e, "INSERT INTO nums VALUES (1)");
    exec(&mut e, "INSERT INTO nums VALUES (2)");

    let r = query(&mut e, "SELECT id FROM nums ORDER BY id");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[2][0], Value::Int(3));
}

#[test]
fn test_order_by_expression_pre_sort() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE nums (id INT)");
    exec(&mut e, "INSERT INTO nums VALUES (3)");
    exec(&mut e, "INSERT INTO nums VALUES (1)");
    exec(&mut e, "INSERT INTO nums VALUES (2)");

    let r = query(&mut e, "SELECT id FROM nums ORDER BY id + 0 DESC");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Int(3));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[2][0], Value::Int(1));
}

#[test]
fn test_distinct_combined_with_order_by() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dups (name VARCHAR(20))");
    exec(&mut e, "INSERT INTO dups VALUES ('Bob')");
    exec(&mut e, "INSERT INTO dups VALUES ('Alice')");
    exec(&mut e, "INSERT INTO dups VALUES ('Bob')");

    let r = query(&mut e, "SELECT DISTINCT name FROM dups ORDER BY name");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[1][0], Value::VarChar("Bob".to_string()));
}

#[test]
fn test_top_limit() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE nums (id INT)");
    exec(&mut e, "INSERT INTO nums VALUES (1)");
    exec(&mut e, "INSERT INTO nums VALUES (2)");
    exec(&mut e, "INSERT INTO nums VALUES (3)");

    let r = query(&mut e, "SELECT TOP 2 id FROM nums ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[1][0], Value::Int(2));
}

#[test]
fn test_offset_fetch_pagination() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE nums (id INT)");
    exec(&mut e, "INSERT INTO nums VALUES (1)");
    exec(&mut e, "INSERT INTO nums VALUES (2)");
    exec(&mut e, "INSERT INTO nums VALUES (3)");
    exec(&mut e, "INSERT INTO nums VALUES (4)");

    let r = query(&mut e, "SELECT id FROM nums ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(2));
    assert_eq!(r.rows[1][0], Value::Int(3));
}

#[test]
fn test_group_by_having_and_order_by() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE sales (grp VARCHAR(20), val INT)");
    exec(&mut e, "INSERT INTO sales VALUES ('A', 5)");
    exec(&mut e, "INSERT INTO sales VALUES ('A', 7)");
    exec(&mut e, "INSERT INTO sales VALUES ('B', 4)");
    exec(&mut e, "INSERT INTO sales VALUES ('B', 6)");
    exec(&mut e, "INSERT INTO sales VALUES ('C', 1)");

    let r = query(
        &mut e,
        "SELECT grp, SUM(val) AS total FROM sales GROUP BY grp HAVING SUM(val) >= 10 ORDER BY grp",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("A".to_string()));
    assert_eq!(r.rows[0][1], Value::BigInt(12));
    assert_eq!(r.rows[1][0], Value::VarChar("B".to_string()));
    assert_eq!(r.rows[1][1], Value::BigInt(10));
}

#[test]
fn test_window_function_branch() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE items (id INT)");
    exec(&mut e, "INSERT INTO items VALUES (30)");
    exec(&mut e, "INSERT INTO items VALUES (10)");
    exec(&mut e, "INSERT INTO items VALUES (20)");

    let r = query(
        &mut e,
        "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, id FROM items ORDER BY id",
    );
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Int(10));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[2][0], Value::Int(3));
}
