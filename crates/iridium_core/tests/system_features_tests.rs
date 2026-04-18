use iridium_core::{types::Value, Engine};

#[test]
fn test_sp_helpindex() {
    let engine = Engine::new();
    engine
        .exec("CREATE TABLE T1 (c1 INT PRIMARY KEY, c2 VARCHAR(10) UNIQUE)")
        .unwrap();

    let res = engine.query("EXEC sp_helpindex 'T1'").unwrap();
    // Clustered PK index should be there.
    // Unique constraints also create indexes.
    assert!(res.rows.len() >= 1);
}

#[test]
fn test_identity_builtins() {
    let engine = Engine::new();
    let res = engine
        .query("SELECT SUSER_NAME(), SUSER_SID(), USER_SID()")
        .unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0], Value::NVarChar("sa".to_string()));
    assert!(matches!(res.rows[0][1], Value::VarBinary(_)));
    assert!(matches!(res.rows[0][2], Value::VarBinary(_)));
}

#[test]
fn test_metadata_synonyms_objects() {
    let engine = Engine::new();
    engine.exec("CREATE TABLE T1 (id INT)").unwrap();
    engine.exec("CREATE SYNONYM S1 FOR T1").unwrap();

    let res = engine
        .query("SELECT * FROM sys.synonyms WHERE name = 'S1'")
        .unwrap();
    assert_eq!(res.rows.len(), 1, "S1 should be in sys.synonyms");

    let res = engine
        .query("SELECT * FROM sys.objects WHERE name = 'S1'")
        .unwrap();
    assert_eq!(res.rows.len(), 1, "S1 should be in sys.objects");
}
