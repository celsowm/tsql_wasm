use tsql_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect(&format!("parse failed: {}", sql));
    engine.execute(stmt).expect(&format!("execute failed: {}", sql));
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).expect(&format!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .expect(&format!("execute failed: {}", sql))
        .expect("expected result")
}

#[test]
fn test_update_from_basic() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE target (id INT, val INT)");
    exec(&mut e, "CREATE TABLE source (id INT, new_val INT)");
    exec(&mut e, "INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)");
    exec(&mut e, "INSERT INTO source VALUES (1, 100), (2, 200)");
    
    // UPDATE with FROM clause - no alias
    exec(&mut e, "UPDATE target SET val = new_val FROM target INNER JOIN source ON target.id = source.id");
    
    let r = query(&mut e, "SELECT id, val FROM target ORDER BY id");
    println!("test_update_from_basic: {:?}", r);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Int(100));
    assert_eq!(r.rows[1][1], Value::Int(200));
    assert_eq!(r.rows[2][1], Value::Int(30)); // unchanged
}

#[test]
fn test_update_from_with_alias() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT, val INT)");
    exec(&mut e, "CREATE TABLE s (id INT, new_val INT)");
    exec(&mut e, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&mut e, "INSERT INTO s VALUES (1, 100), (2, 200)");
    
    // UPDATE with alias
    exec(&mut e, "UPDATE t SET t.val = s.new_val FROM t INNER JOIN s ON t.id = s.id");
    
    let r = query(&mut e, "SELECT * FROM t ORDER BY id");
    println!("test_update_from_with_alias: {:?}", r);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Value::Int(100));
    assert_eq!(r.rows[1][1], Value::Int(200));
}

#[test]
fn test_delete_from_basic() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE target (id INT, val INT)");
    exec(&mut e, "CREATE TABLE source (id INT)");
    exec(&mut e, "INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)");
    exec(&mut e, "INSERT INTO source VALUES (1), (2)");
    
    exec(&mut e, "DELETE FROM target FROM target INNER JOIN source ON target.id = source.id");
    
    let r = query(&mut e, "SELECT * FROM target ORDER BY id");
    println!("test_delete_from_basic: {:?}", r);
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(3));
}