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
fn test_merge_basic_matched() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE target (id INT PRIMARY KEY, val VARCHAR(10))");
    exec(&mut e, "CREATE TABLE source (id INT, val VARCHAR(10))");
    exec(&mut e, "INSERT INTO target VALUES (1, 'old'), (2, 'old')");
    exec(&mut e, "INSERT INTO source VALUES (1, 'new'), (3, 'new')");
    
    exec(&mut e, "MERGE target t USING source s ON t.id = s.id \
        WHEN MATCHED THEN UPDATE SET val = s.val");
    
    let r = query(&mut e, "SELECT * FROM target ORDER BY id");
    println!("test_merge_basic_matched: {:?}", r);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Value::VarChar("new".to_string())); // updated
    assert_eq!(r.rows[1][1], Value::VarChar("old".to_string())); // not matched
}