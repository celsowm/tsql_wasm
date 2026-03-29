use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
}

// ─── COUNT(*) ───────────────────────────────────────────────────────────

#[test]
fn test_count_star_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_agg (id INT, val INT)");

    engine_exec(&mut engine, "INSERT INTO t_agg VALUES (1, 10), (2, 20), (3, 30)");

    let sql = "SELECT COUNT(*) FROM t_agg";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── SUM ────────────────────────────────────────────────────────────────

#[test]
fn test_sum_aggregate_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_sum (val INT)");

    engine_exec(&mut engine, "INSERT INTO t_sum VALUES (100), (200), (300)");

    let sql = "SELECT SUM(val) FROM t_sum";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── AVG ────────────────────────────────────────────────────────────────

#[test]
fn test_avg_aggregate_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_avg (val INT)");

    engine_exec(&mut engine, "INSERT INTO t_avg VALUES (10), (20), (30)");

    let sql = "SELECT AVG(val) FROM t_avg";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── MIN/MAX ────────────────────────────────────────────────────────────

#[test]
fn test_min_max_aggregate_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_minmax (val INT)");

    engine_exec(&mut engine, "INSERT INTO t_minmax VALUES (5), (15), (25)");

    let sql = "SELECT MIN(val), MAX(val) FROM t_minmax";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}
