use tsql_core::{parse_sql, Engine};







fn engine_exec(engine: &mut Engine, sql: &str) -> Option<tsql_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("Parser falhou: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("Engine falhou: {}", sql))
}

// ─── INSERT single row ─────────────────────────────────────────────────

#[test]
fn test_insert_single_row_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_ins1 (id INT, name VARCHAR(20))");

    engine_exec(&mut engine, "INSERT INTO t_ins1 VALUES (1, 'Alice')");
    let _engine_result = engine_exec(&mut engine, "SELECT * FROM t_ins1").unwrap();
    assert!(!_engine_result.rows.is_empty());

}

// ─── UPDATE ────────────────────────────────────────────────────────────

#[test]
fn test_update_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_upd (id INT, val INT)");
    engine_exec(&mut engine, "INSERT INTO t_upd VALUES (1, 10), (2, 20)");

    engine_exec(&mut engine, "UPDATE t_upd SET val = 99 WHERE id = 1");
    let _engine_result = engine_exec(&mut engine, "SELECT val FROM t_upd WHERE id = 1").unwrap();

}

// ─── UPDATE multiple rows ─────────────────────────────────────────────

#[test]
fn test_update_multiple_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_updmulti (id INT, val INT)");
    engine_exec(&mut engine, "INSERT INTO t_updmulti VALUES (1, 10), (2, 20), (3, 30)");

    engine_exec(&mut engine, "UPDATE t_updmulti SET val = val * 2");
    let _engine_result = engine_exec(&mut engine, "SELECT SUM(val) FROM t_updmulti").unwrap();

}

// ─── DELETE ────────────────────────────────────────────────────────────

#[test]
fn test_delete_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_del (id INT)");
    engine_exec(&mut engine, "INSERT INTO t_del VALUES (1), (2), (3)");

    engine_exec(&mut engine, "DELETE FROM t_del WHERE id = 2");
    let _engine_result = engine_exec(&mut engine, "SELECT COUNT(*) FROM t_del").unwrap();

}

// ─── DELETE all ────────────────────────────────────────────────────────

#[test]
fn test_delete_all_compare() {
    let mut engine = Engine::new();

    engine_exec(&mut engine, "CREATE TABLE t_delall (id INT)");
    engine_exec(&mut engine, "INSERT INTO t_delall VALUES (1), (2)");

    engine_exec(&mut engine, "DELETE FROM t_delall");
    let _engine_result = engine_exec(&mut engine, "SELECT COUNT(*) FROM t_delall").unwrap();

}
