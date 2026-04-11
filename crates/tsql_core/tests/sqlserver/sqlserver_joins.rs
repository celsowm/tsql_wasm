use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

// ─── LEFT OUTER JOIN ─────────────────────────────────────────────────────

#[test]
fn test_left_outer_join_compare() {
    let mut engine = Engine::new();
    engine_exec(&mut engine, "CREATE TABLE t_customers (id INT PRIMARY KEY, name VARCHAR(50))");
    engine_exec(&mut engine, "CREATE TABLE t_orders (id INT PRIMARY KEY, customer_id INT, amount INT)");

    engine_exec(&mut engine, "INSERT INTO t_customers VALUES (1, 'Alice'), (2, 'Bob')");
    engine_exec(&mut engine, "INSERT INTO t_orders VALUES (100, 1, 500)");

    let join_sql = "SELECT c.name, o.amount FROM t_customers c LEFT JOIN t_orders o ON c.id = o.customer_id ORDER BY c.id";
    let _engine_result = engine_exec(&mut engine, join_sql).unwrap();
    assert!(!_engine_result.rows.is_empty());

}
