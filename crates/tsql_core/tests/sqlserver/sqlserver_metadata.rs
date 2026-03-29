use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
}

// ─── sys.tables ────────────────────────────────────────────────────────

#[test]
fn test_sys_tables_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_meta1 (id INT, name VARCHAR(20))");
    let _engine_result = engine_exec(&mut engine, "SELECT name FROM sys.tables WHERE name = 't_meta1'").unwrap();
    assert!(!_engine_result.rows.is_empty());

}

// ─── sys.columns ────────────────────────────────────────────────────────

#[test]
fn test_sys_columns_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_meta2 (id INT, name VARCHAR(20), age INT)");
    let _engine_result = engine_exec(&mut engine, "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('t_meta2') ORDER BY column_id").unwrap();
    assert!(!_engine_result.rows.is_empty());

}

// ─── OBJECT_ID function ────────────────────────────────────────────────

#[test]
fn test_object_id_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_objid (id INT)");
    let _engine_result = engine_exec(&mut engine, "SELECT OBJECT_ID('t_objid')").unwrap();

}
