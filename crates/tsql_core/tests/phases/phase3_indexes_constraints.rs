use tsql_core::{parse_sql, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

#[test]
fn test_create_and_drop_index_catalog_only() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE INDEX dbo.ix_t_name ON dbo.t (name)");

    let r = query(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.indexes WHERE name = 'ix_t_name'",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "1");

    exec(&mut e, "DROP INDEX dbo.ix_t_name ON dbo.t");
    let r2 = query(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.indexes WHERE name = 'ix_t_name'",
    );
    assert_eq!(r2.rows[0][0].to_string_value(), "0");
}

#[test]
fn test_named_default_constraint_table_level() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.t (id INT, val INT, CONSTRAINT DF_t_val DEFAULT 9 FOR val)",
    );
    exec(&mut e, "INSERT INTO dbo.t (id) VALUES (1)");
    let r = query(&mut e, "SELECT val FROM dbo.t");
    assert_eq!(r.rows[0][0].to_string_value(), "9");
}

#[test]
fn test_named_check_constraint_column_level() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.t (v INT CONSTRAINT CK_t_v CHECK (v > 0))",
    );
    let err = e
        .execute(parse_sql("INSERT INTO dbo.t VALUES (-1)").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("CK_t_v"));
}

#[test]
fn test_named_check_constraint_table_level() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.pairs (a INT, b INT, CONSTRAINT CK_pairs CHECK (a < b))",
    );
    let err = e
        .execute(parse_sql("INSERT INTO dbo.pairs VALUES (2, 1)").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("CK_pairs"));
}

#[test]
fn test_drop_table_removes_index_metadata() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE INDEX dbo.ix_t_name ON dbo.t (name)");
    exec(&mut e, "DROP TABLE dbo.t");
    let r = query(
        &mut e,
        "SELECT COUNT(*) AS cnt FROM sys.indexes WHERE name = 'ix_t_name'",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "0");
}

#[test]
fn test_indexed_predicate_and_order_semantics() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (id INT, name VARCHAR(50))");
    exec(&mut e, "CREATE INDEX dbo.ix_t_id ON dbo.t (id)");
    exec(
        &mut e,
        "INSERT INTO dbo.t VALUES (3, 'c'), (1, 'a'), (2, 'b'), (4, 'd')",
    );
    let r = query(&mut e, "SELECT id FROM dbo.t WHERE id >= 2 ORDER BY id");
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0].to_string_value(), "2");
    assert_eq!(r.rows[1][0].to_string_value(), "3");
    assert_eq!(r.rows[2][0].to_string_value(), "4");
}

#[test]
fn test_table_level_primary_key_composite() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.orders (order_id INT, line_no INT, qty INT, CONSTRAINT PK_orders PRIMARY KEY (order_id, line_no))",
    );
    exec(&mut e, "INSERT INTO dbo.orders VALUES (1, 1, 10)");
    exec(&mut e, "INSERT INTO dbo.orders VALUES (1, 2, 20)");
    let r = query(&mut e, "SELECT COUNT(*) FROM dbo.orders");
    assert_eq!(r.rows[0][0].to_string_value(), "2");

    let err = e
        .execute(parse_sql("INSERT INTO dbo.orders VALUES (NULL, 1, 30)").unwrap())
        .unwrap_err();
    assert!(
        err.to_string().contains("NULL") || err.to_string().contains("null"),
        "Expected null constraint error, got: {}",
        err
    );
}

#[test]
fn test_table_level_primary_key_not_null_enforcement() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.t (a INT, b INT, CONSTRAINT PK_t PRIMARY KEY (a, b))",
    );
    let err = e
        .execute(parse_sql("INSERT INTO dbo.t (a) VALUES (1)").unwrap())
        .unwrap_err();
    assert!(
        err.to_string().contains("NULL") || err.to_string().contains("null"),
        "Expected null constraint error, got: {}",
        err
    );
}

#[test]
fn test_table_level_unique_constraint() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.t (id INT, email VARCHAR(100), CONSTRAINT UQ_t_email UNIQUE (email))",
    );
    exec(&mut e, "INSERT INTO dbo.t VALUES (1, 'a@b.com')");
    let err = e
        .execute(parse_sql("INSERT INTO dbo.t VALUES (2, 'a@b.com')").unwrap())
        .unwrap_err();
    assert!(
        err.to_string().contains("uplicate") || err.to_string().contains("unique"),
        "Expected duplicate unique error, got: {}",
        err
    );
}

#[test]
fn test_column_level_references_creates_fk() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.parent (id INT PRIMARY KEY)",
    );
    exec(
        &mut e,
        "CREATE TABLE dbo.child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))",
    );
    let r = query(
        &mut e,
        "SELECT COUNT(*) FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('dbo.child')",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "1", "Expected 1 FK from column-level REFERENCES");

    exec(&mut e, "INSERT INTO dbo.parent VALUES (1)");
    exec(&mut e, "INSERT INTO dbo.child VALUES (1, 1)");
    let err = e
        .execute(parse_sql("INSERT INTO dbo.child VALUES (2, 999)").unwrap())
        .unwrap_err();
    assert!(
        err.to_string().contains("foreign") || err.to_string().contains("FK"),
        "Expected FK violation, got: {}",
        err
    );
}

#[test]
fn test_alter_table_add_check_constraint() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (val INT)");
    exec(&mut e, "ALTER TABLE dbo.t ADD CONSTRAINT CK_t_val CHECK (val > 0)");
    
    exec(&mut e, "INSERT INTO dbo.t VALUES (5)");
    let err = e
        .execute(parse_sql("INSERT INTO dbo.t VALUES (-1)").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("CK_t_val"));
}

#[test]
fn test_alter_table_add_foreign_key_constraint() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.parent (id INT PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE dbo.child (id INT PRIMARY KEY, parent_id INT)");
    exec(&mut e, "ALTER TABLE dbo.child ADD CONSTRAINT FK_child_parent FOREIGN KEY (parent_id) REFERENCES parent(id)");
    
    let r = query(
        &mut e,
        "SELECT COUNT(*) FROM sys.foreign_keys WHERE name = 'FK_child_parent'",
    );
    assert_eq!(r.rows[0][0].to_string_value(), "1");
}

#[test]
fn test_alter_table_drop_constraint() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (val INT)");
    exec(&mut e, "ALTER TABLE dbo.t ADD CONSTRAINT CK_t_val CHECK (val > 0)");
    exec(&mut e, "INSERT INTO dbo.t VALUES (5)");
    
    exec(&mut e, "ALTER TABLE dbo.t DROP CONSTRAINT CK_t_val");
    
    exec(&mut e, "INSERT INTO dbo.t VALUES (-1)");
    let r = query(&mut e, "SELECT COUNT(*) FROM dbo.t");
    assert_eq!(r.rows[0][0].to_string_value(), "2");
}

#[test]
fn test_alter_table_add_primary_key_constraint() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (id INT NOT NULL)");
    exec(&mut e, "ALTER TABLE dbo.t ADD CONSTRAINT PK_t PRIMARY KEY (id)");
    
    exec(&mut e, "INSERT INTO dbo.t VALUES (1)");
    let err = e
        .execute(parse_sql("INSERT INTO dbo.t VALUES (1)").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("uplicate") || err.to_string().contains("unique"));
}

#[test]
fn test_fk_on_delete_cascade() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.parent (id INT PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE dbo.child (id INT PRIMARY KEY, parent_id INT, CONSTRAINT FK_child_parent FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE)");
    
    exec(&mut e, "INSERT INTO dbo.parent VALUES (1)");
    exec(&mut e, "INSERT INTO dbo.child VALUES (1, 1)");
    exec(&mut e, "INSERT INTO dbo.child VALUES (2, 1)");
    
    exec(&mut e, "DELETE FROM dbo.parent WHERE id = 1");
    
    let r = query(&mut e, "SELECT COUNT(*) FROM dbo.child");
    assert_eq!(r.rows[0][0].to_string_value(), "0", "Child rows should be cascade deleted");
}

#[test]
fn test_fk_on_update_no_action_blocks() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.parent (id INT PRIMARY KEY)");
    exec(&mut e, "CREATE TABLE dbo.child (id INT PRIMARY KEY, parent_id INT, CONSTRAINT FK_child_parent FOREIGN KEY (parent_id) REFERENCES parent(id) ON UPDATE NO ACTION)");
    
    exec(&mut e, "INSERT INTO dbo.parent VALUES (1)");
    exec(&mut e, "INSERT INTO dbo.child VALUES (1, 1)");
    
    let err = e
        .execute(parse_sql("UPDATE dbo.parent SET id = 2 WHERE id = 1").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("UPDATE") || err.to_string().contains("conflicted"));
}
