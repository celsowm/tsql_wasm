use iridium_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .unwrap_or_else(|_| panic!("execute failed: {}", sql));
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .unwrap_or_else(|_| panic!("execute failed: {}", sql))
        .expect("expected result")
}

#[test]
fn test_merge_basic_matched() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE target (id INT PRIMARY KEY, val VARCHAR(10))",
    );
    exec(&mut e, "CREATE TABLE source (id INT, val VARCHAR(10))");
    exec(&mut e, "INSERT INTO target VALUES (1, 'old'), (2, 'old')");
    exec(&mut e, "INSERT INTO source VALUES (1, 'new'), (3, 'new')");

    exec(
        &mut e,
        "MERGE target t USING source s ON t.id = s.id \
        WHEN MATCHED THEN UPDATE SET val = s.val",
    );

    let r = query(&mut e, "SELECT * FROM target ORDER BY id");
    println!("test_merge_basic_matched: {:?}", r);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Value::VarChar("new".to_string())); // updated
    assert_eq!(r.rows[1][1], Value::VarChar("old".to_string())); // not matched
}

#[test]
fn test_merge_not_matched_by_source_delete() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE target (id INT PRIMARY KEY, val VARCHAR(10))",
    );
    exec(&mut e, "CREATE TABLE source (id INT, val VARCHAR(10))");
    exec(
        &mut e,
        "INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    exec(&mut e, "INSERT INTO source VALUES (1, 'x')");

    exec(
        &mut e,
        "MERGE target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET val = s.val \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    );

    let r = query(&mut e, "SELECT * FROM target ORDER BY id");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("x".to_string()));
}

#[test]
fn test_merge_not_matched_by_source_update() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE target (id INT PRIMARY KEY, val VARCHAR(10))",
    );
    exec(&mut e, "CREATE TABLE source (id INT, val VARCHAR(10))");
    exec(
        &mut e,
        "INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    exec(&mut e, "INSERT INTO source VALUES (1, 'x')");

    exec(
        &mut e,
        "MERGE target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET val = s.val \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET val = 'orphaned'",
    );

    let r = query(&mut e, "SELECT * FROM target ORDER BY id");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Value::VarChar("x".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("orphaned".to_string()));
    assert_eq!(r.rows[2][1], Value::VarChar("orphaned".to_string()));
}

#[test]
fn test_merge_not_matched_by_source_with_condition() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE target (id INT PRIMARY KEY, val VARCHAR(10))",
    );
    exec(&mut e, "CREATE TABLE source (id INT, val VARCHAR(10))");
    exec(
        &mut e,
        "INSERT INTO target VALUES (1, 'keep'), (2, 'delete'), (3, 'delete')",
    );
    exec(&mut e, "INSERT INTO source VALUES (1, 'updated')");

    exec(
        &mut e,
        "MERGE target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET val = s.val \
         WHEN NOT MATCHED BY SOURCE AND t.val = 'delete' THEN DELETE",
    );

    let r = query(&mut e, "SELECT * FROM target ORDER BY id");
    // id=1 MATCHED -> updated, id=2 and id=3 NOT MATCHED BY SOURCE with val='delete' -> deleted
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("updated".to_string()));
}

#[test]
fn test_merge_all_three_clauses() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE target (id INT PRIMARY KEY, val VARCHAR(10))",
    );
    exec(&mut e, "CREATE TABLE source (id INT, val VARCHAR(10))");
    exec(&mut e, "INSERT INTO target VALUES (1, 'old1'), (3, 'old3')");
    exec(&mut e, "INSERT INTO source VALUES (1, 'new1'), (2, 'new2')");

    exec(
        &mut e,
        "MERGE target t USING source s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET val = s.val \
         WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val) \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    );

    let r = query(&mut e, "SELECT * FROM target ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("new1".to_string()));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[1][1], Value::VarChar("new2".to_string()));
}

