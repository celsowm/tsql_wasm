use iridium_core::{types::Value, Engine};

#[test]
fn test_sequence_basic() {
    let engine = Engine::new();
    engine
        .exec("CREATE SEQUENCE MySeq START WITH 1 INCREMENT BY 1")
        .unwrap();

    let res = engine.query("SELECT NEXT VALUE FOR MySeq").unwrap();
    // Placeholder value is BigInt(1) for now
    assert_eq!(res.rows[0][0], Value::BigInt(1));
}

#[test]
fn test_sequence_metadata() {
    let engine = Engine::new();
    engine.exec("CREATE SEQUENCE S1 START WITH 100").unwrap();

    let res = engine
        .query("SELECT name, start_value FROM sys.sequences WHERE name = 'S1'")
        .unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][1], Value::BigInt(100));
}

#[test]
fn test_drop_sequence() {
    let engine = Engine::new();
    engine.exec("CREATE SEQUENCE S1").unwrap();
    engine.exec("DROP SEQUENCE S1").unwrap();

    let res = engine.query("SELECT * FROM sys.sequences").unwrap();
    assert_eq!(res.rows.len(), 0);
}
