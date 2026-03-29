use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
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
