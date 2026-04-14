use iridium_core::{parse_sql, Engine};





fn engine_exec(engine: &mut Engine, sql: &str) -> Option<iridium_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}


// ─── CAST INT -> VARCHAR ───────────────────────────────────────────────

#[test]
fn test_cast_int_varchar_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT CAST(123 AS VARCHAR(10))";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── CAST VARCHAR -> INT ───────────────────────────────────────────────

#[test]
fn test_cast_varchar_int_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT CAST('456' AS INT)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── CONVERT DATE STYLE ────────────────────────────────────────────────

#[test]
fn test_convert_date_style_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT CONVERT(VARCHAR(10), CAST('2024-01-15' AS DATE), 23)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── CAST DECIMAL ──────────────────────────────────────────────────────

#[test]
fn test_cast_decimal_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT CAST(123.456 AS DECIMAL(10,2))";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

