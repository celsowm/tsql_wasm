use iridium_core::{parse_sql, Engine};






fn engine_exec(engine: &mut Engine, sql: &str) -> Option<iridium_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

// ─── DATEADD ────────────────────────────────────────────────────────────

#[test]
fn test_dateadd_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT DATEADD(day, 5, '2024-01-01')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── DATEDIFF ───────────────────────────────────────────────────────────

#[test]
fn test_datediff_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT DATEDIFF(day, '2024-01-01', '2024-01-10')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── YEAR/MONTH/DAY ─────────────────────────────────────────────────────

#[test]
fn test_year_month_day_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT YEAR('2024-06-15'), MONTH('2024-06-15'), DAY('2024-06-15')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();

    assert!(!_engine_result.rows.is_empty());
}

