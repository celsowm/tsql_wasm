use tsql_core::{parse_sql, Engine};






fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
}

// ─── ABS ────────────────────────────────────────────────────────────────

#[test]
fn test_abs_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT ABS(-42)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── ROUND ──────────────────────────────────────────────────────────────

#[test]
fn test_round_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT ROUND(1.567, 2)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── CEILING ────────────────────────────────────────────────────────────

#[test]
fn test_ceiling_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT CEILING(4.2)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── FLOOR ──────────────────────────────────────────────────────────────

#[test]
fn test_floor_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT FLOOR(4.8)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── POWER ───────────────────────────────────────────────────────────────

#[test]
fn test_power_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT POWER(2, 8)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── SQRT ───────────────────────────────────────────────────────────────

#[test]
fn test_sqrt_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT SQRT(16)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── SIGN ───────────────────────────────────────────────────────────────

#[test]
fn test_sign_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT SIGN(-5), SIGN(0), SIGN(5)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();

    assert!(!_engine_result.rows.is_empty());
}
