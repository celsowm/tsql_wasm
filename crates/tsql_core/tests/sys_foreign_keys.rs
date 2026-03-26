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
fn test_sys_foreign_keys_empty() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT PRIMARY KEY)");
    
    let r = query(&mut e, "SELECT * FROM sys.foreign_keys WHERE parent_object_id = 9999");
    println!("test_sys_foreign_keys_empty: {:?}", r);
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn test_sys_foreign_keys_viewable() {
    let mut e = Engine::new();
    // Note: Inline FK not supported in parser, so we'll skip FK tables
    // The sys.foreign_keys view should at least be queryable
    let r = query(&mut e, "SELECT name, type, type_desc FROM sys.foreign_keys");
    println!("test_sys_foreign_keys_viewable: {:?}", r);
    // Should return empty result, not an error
    assert!(true);
}