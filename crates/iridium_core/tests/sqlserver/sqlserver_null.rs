use iridium_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<iridium_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

// ─── IS NULL ────────────────────────────────────────────────────────────

#[test]
fn test_null_is_null_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_null (id INT, val VARCHAR(10))");

    engine_exec(&mut engine, "INSERT INTO t_null VALUES (1, NULL), (2, 'A')");

    let sql = "SELECT id FROM t_null WHERE val IS NULL";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}

// ─── IS NOT NULL ────────────────────────────────────────────────────────

#[test]
fn test_null_is_not_null_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_notnull (id INT, val VARCHAR(10))");

    engine_exec(&mut engine, "INSERT INTO t_notnull VALUES (1, NULL), (2, 'A')");

    let sql = "SELECT id FROM t_notnull WHERE val IS NOT NULL";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}

// ─── COALESCE ───────────────────────────────────────────────────────────

#[test]
fn test_coalesce_null_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_coal (id INT, a VARCHAR(10), b VARCHAR(10))");

    engine_exec(&mut engine, "INSERT INTO t_coal VALUES (1, NULL, 'X'), (2, 'Y', 'Z')");

    let sql = "SELECT id, COALESCE(a, b) AS result FROM t_coal ORDER BY id";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}

