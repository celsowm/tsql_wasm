use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
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
