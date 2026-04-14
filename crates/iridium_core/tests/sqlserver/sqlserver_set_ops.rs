use iridium_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<iridium_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

// ─── DISTINCT ───────────────────────────────────────────────────────────

#[test]
fn test_distinct_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_dist (val VARCHAR(10))");

    engine_exec(&mut engine, "INSERT INTO t_dist VALUES ('A'), ('B'), ('A'), ('C')");
    let _engine_result = engine_exec(&mut engine, "SELECT DISTINCT val FROM t_dist ORDER BY val").unwrap();
    assert!(!_engine_result.rows.is_empty());

}

