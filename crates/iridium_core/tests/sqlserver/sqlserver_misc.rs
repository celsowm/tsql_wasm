use iridium_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<iridium_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

// ─── CASE SIMPLE ───────────────────────────────────────────────────────

#[test]
fn test_case_simple_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── CASE SEARCHED ─────────────────────────────────────────────────────

#[test]
fn test_case_searched_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT CASE WHEN 1 > 0 THEN 'positive' ELSE 'non-positive' END";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── HAVING ─────────────────────────────────────────────────────────────

#[test]
fn test_having_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_hav (cat VARCHAR(10), val INT)");

    engine_exec(&mut engine, "INSERT INTO t_hav VALUES ('A', 10), ('A', 30), ('B', 20)");

    let sql = "SELECT cat, SUM(val) AS total FROM t_hav GROUP BY cat HAVING SUM(val) > 25 ORDER BY cat";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}

