use iridium_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<iridium_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

// ─── LIKE % ──────────────────────────────────────────────────────────────

#[test]
fn test_like_pattern_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_like (name VARCHAR(20))");

    engine_exec(&mut engine, "INSERT INTO t_like VALUES ('Apple'), ('Banana'), ('Apricot')");

    let sql = "SELECT name FROM t_like WHERE name LIKE 'A%' ORDER BY name";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}

