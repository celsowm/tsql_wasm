use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
}

// ─── CTE SIMPLE ─────────────────────────────────────────────────────────

#[test]
fn test_cte_simple_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_cte_src (id INT, val INT)");

    engine_exec(&mut engine, "INSERT INTO t_cte_src VALUES (1, 100), (2, 200), (3, 300)");

    let sql = "WITH cte AS (SELECT id, val FROM t_cte_src WHERE id > 1) SELECT * FROM cte ORDER BY id";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}
