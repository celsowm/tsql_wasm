use iridium_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

#[test]
fn test_drop_table() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t1 (id INT NOT NULL)");
    exec(&mut engine, "INSERT INTO dbo.t1 (id) VALUES (1)");
    let r = query(&mut engine, "SELECT * FROM dbo.t1");
    assert_eq!(r.rows.len(), 1);

    exec(&mut engine, "DROP TABLE dbo.t1");

    // Table should no longer exist
    let stmt = parse_sql("SELECT * FROM dbo.t1").unwrap();
    let result = engine.execute(stmt);
    assert!(result.is_err());
}

#[test]
fn test_create_and_drop_schema() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE SCHEMA sales");
    exec(&mut engine, "CREATE TABLE sales.orders (id INT NOT NULL)");
    exec(&mut engine, "INSERT INTO sales.orders (id) VALUES (42)");
    let r = query(&mut engine, "SELECT * FROM sales.orders");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(42));

    // Can't drop schema with tables
    let stmt = parse_sql("DROP SCHEMA sales").unwrap();
    let result = engine.execute(stmt);
    assert!(result.is_err());

    exec(&mut engine, "DROP TABLE sales.orders");
    exec(&mut engine, "DROP SCHEMA sales");
}

#[test]
fn test_unique_constraint_enforcement() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.users (id INT NOT NULL, email VARCHAR(100) UNIQUE)",
    );

    exec(
        &mut engine,
        "INSERT INTO dbo.users (id, email) VALUES (1, 'a@b.com')",
    );

    // Duplicate should fail
    let stmt = parse_sql("INSERT INTO dbo.users (id, email) VALUES (2, 'a@b.com')").unwrap();
    let result = engine.execute(stmt);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("UNIQUE"));
}

#[test]
fn test_unique_allows_nulls() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (id INT NOT NULL, email VARCHAR(100) UNIQUE)",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.t (id, email) VALUES (1, NULL)",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.t (id, email) VALUES (2, NULL)",
    );
    let r = query(&mut engine, "SELECT * FROM dbo.t");
    assert_eq!(r.rows.len(), 2);
}

