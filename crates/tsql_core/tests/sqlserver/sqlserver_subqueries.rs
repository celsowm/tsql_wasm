use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).expect(&format!("Parser falhou: {}", sql));
    engine.execute(stmt).expect(&format!("Engine falhou: {}", sql))
}

// ─── EXISTS ─────────────────────────────────────────────────────────────

#[test]
fn test_subquery_exists_compare() {
    let mut engine = Engine::new();
    engine_exec(&mut engine, "CREATE TABLE t_sq_dept (id INT PRIMARY KEY, name VARCHAR(20))");
    engine_exec(&mut engine, "CREATE TABLE t_sq_emp (id INT, dept_id INT)");

    engine_exec(&mut engine, "INSERT INTO t_sq_dept VALUES (10, 'Sales'), (20, 'IT')");
    engine_exec(&mut engine, "INSERT INTO t_sq_emp VALUES (1, 10), (2, 20)");

    let sql = "SELECT name FROM t_sq_dept WHERE EXISTS (SELECT 1 FROM t_sq_emp WHERE dept_id = t_sq_dept.id)";
    let _engine_result = engine_exec(&mut engine, sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}
