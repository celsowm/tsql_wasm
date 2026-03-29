use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
}

// ─── OFFSET/FETCH ──────────────────────────────────────────────────────

#[test]
fn test_offset_fetch_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_offset (id INT)");

    engine_exec(&mut engine, "INSERT INTO t_offset VALUES (1), (2), (3), (4), (5)");

    let sql = "SELECT id FROM t_offset ORDER BY id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}
