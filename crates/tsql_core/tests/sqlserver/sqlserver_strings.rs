use tsql_core::{parse_sql, Engine};






fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

// ─── LEN ────────────────────────────────────────────────────────────────

#[test]
fn test_len_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT LEN('hello world')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── SUBSTRING ──────────────────────────────────────────────────────────

#[test]
fn test_substring_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT SUBSTRING('hello', 2, 3)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── UPPER ───────────────────────────────────────────────────────────────

#[test]
fn test_upper_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT UPPER('hello')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── LOWER ───────────────────────────────────────────────────────────────

#[test]
fn test_lower_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT LOWER('HELLO')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── LTRIM ───────────────────────────────────────────────────────────────

#[test]
fn test_ltrim_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT LTRIM('   hello')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── RTRIM ───────────────────────────────────────────────────────────────

#[test]
fn test_rtrim_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT RTRIM('hello   ')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── TRIM ────────────────────────────────────────────────────────────────

#[test]
fn test_trim_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT TRIM('   hello   ')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── REPLACE ─────────────────────────────────────────────────────────────

#[test]
fn test_replace_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT REPLACE('hello world', 'world', 'there')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── CHARINDEX ───────────────────────────────────────────────────────────

#[test]
fn test_charindex_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT CHARINDEX('world', 'hello world')";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

// ─── LEFT/RIGHT string functions ───────────────────────────────────────

#[test]
fn test_left_string_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT LEFT('hello', 3)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}

#[test]
fn test_right_string_compare() {
    let mut engine = Engine::new();

    let sql = "SELECT RIGHT('hello', 3)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
}
