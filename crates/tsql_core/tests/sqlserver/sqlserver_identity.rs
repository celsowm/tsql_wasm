use tsql_core::{parse_sql, Engine, types::Value};

fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
}

fn engine_query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine_exec(engine, sql).unwrap()
}

// ─── IDENTITY basic ─────────────────────────────────────────────────────

#[test]
fn test_identity_basic_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_ident (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(20))");

    engine_exec(&mut engine, "INSERT INTO t_ident (name) VALUES ('Alice')");
    engine_exec(&mut engine, "INSERT INTO t_ident (name) VALUES ('Bob')");
    engine_exec(&mut engine, "INSERT INTO t_ident (name) VALUES ('Charlie')");
    let _engine_result = engine_exec(&mut engine, "SELECT id FROM t_ident ORDER BY id").unwrap();
    assert!(!_engine_result.rows.is_empty());

}

// ─── SCOPE_IDENTITY ────────────────────────────────────────────────────

#[test]
fn test_scope_identity_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_scope (id INT IDENTITY(1,1) PRIMARY KEY, val VARCHAR(10))");

    engine_exec(&mut engine, "INSERT INTO t_scope (val) VALUES ('test')");
    engine_exec(&mut engine, "INSERT INTO t_scope (val) VALUES ('test2')");
    let _engine_result = engine_exec(&mut engine, "SELECT SCOPE_IDENTITY()").unwrap();

}

#[test]
fn test_identity_insert_explicit_value_blocked() {
    let mut engine = Engine::new();
    engine_exec(&mut engine, "CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(20))");
    let err = engine
        .execute(parse_sql("INSERT INTO t (id, name) VALUES (10, 'test')").unwrap())
        .unwrap_err();
    assert!(
        err.to_string().contains("IDENTITY_INSERT"),
        "Expected IDENTITY_INSERT error, got: {}",
        err
    );
}

#[test]
fn test_identity_insert_on_allows_explicit_value() {
    let mut engine = Engine::new();
    engine_exec(&mut engine, "CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(20))");
    engine_exec(&mut engine, "SET IDENTITY_INSERT t ON");
    engine_exec(&mut engine, "INSERT INTO t (id, name) VALUES (10, 'test')");
    let r = engine_query(&mut engine, "SELECT id FROM t");
    assert_eq!(r.rows[0][0], Value::Int(10));

    engine_exec(&mut engine, "SET IDENTITY_INSERT t OFF");
    let err = engine
        .execute(parse_sql("INSERT INTO t (id, name) VALUES (20, 'test2')").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("IDENTITY_INSERT"));
}

#[test]
fn test_identity_insert_auto_still_works_when_on() {
    let mut engine = Engine::new();
    engine_exec(&mut engine, "CREATE TABLE t (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(20))");
    engine_exec(&mut engine, "SET IDENTITY_INSERT t ON");
    engine_exec(&mut engine, "INSERT INTO t (name) VALUES ('auto')");
    let r = engine_query(&mut engine, "SELECT id FROM t");
    assert_eq!(r.rows[0][0], Value::Int(1));
}
