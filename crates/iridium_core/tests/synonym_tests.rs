use iridium_core::{types::Value, Engine};

#[test]
fn test_synonym_basic() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE BaseTable (id INT, name VARCHAR(50))")
        .unwrap();
    engine
        .exec("INSERT INTO BaseTable VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    engine.exec("CREATE SYNONYM MyTable FOR BaseTable").unwrap();

    let res = engine.query("SELECT * FROM MyTable ORDER BY id").unwrap();
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0][1], Value::VarChar("Alice".to_string()));
    assert_eq!(res.rows[1][1], Value::VarChar("Bob".to_string()));
}

#[test]
fn test_synonym_metadata() {
    let engine = Engine::new();
    engine.exec("CREATE TABLE T1 (id INT)").unwrap();
    engine.exec("CREATE SYNONYM S1 FOR T1").unwrap();

    let res = engine
        .query("SELECT name, base_object_name FROM sys.synonyms WHERE name = 'S1'")
        .unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][1], Value::NVarChar("dbo.T1".to_string()));

    let res = engine
        .query("SELECT name, type FROM sys.objects WHERE name = 'S1'")
        .unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][1], Value::Char("SN".to_string()));
}

#[test]
fn test_drop_synonym() {
    let engine = Engine::new();
    engine.exec("CREATE TABLE T1 (id INT)").unwrap();
    engine.exec("CREATE SYNONYM S1 FOR T1").unwrap();
    engine.exec("DROP SYNONYM S1").unwrap();

    let res = engine.query("SELECT * FROM sys.synonyms").unwrap();
    assert_eq!(res.rows.len(), 0);
}

#[test]
fn test_synonym_to_view() {
    let engine = Engine::new();
    engine.exec("CREATE TABLE T1 (id INT)").unwrap();
    engine.exec("INSERT INTO T1 VALUES (1)").unwrap();
    engine.exec("CREATE VIEW V1 AS SELECT * FROM T1").unwrap();
    engine.exec("CREATE SYNONYM S1 FOR V1").unwrap();

    let res = engine.query("SELECT * FROM S1").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], Value::Int(1));
}

#[test]
fn test_nested_synonyms() {
    let engine = Engine::new();
    engine.exec("CREATE TABLE T1 (id INT)").unwrap();
    engine.exec("INSERT INTO T1 VALUES (42)").unwrap();
    engine.exec("CREATE SYNONYM S1 FOR T1").unwrap();
    engine.exec("CREATE SYNONYM S2 FOR S1").unwrap();

    let res = engine.query("SELECT * FROM S2").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], Value::Int(42));
}
