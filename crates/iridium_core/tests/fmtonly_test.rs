use iridium_core::Engine;

#[test]
fn test_set_fmtonly_on_returns_metadata_no_rows() {
    let engine = Engine::new();

    engine
        .exec("CREATE TABLE t (id INT, name VARCHAR(50))")
        .unwrap();
    engine.exec("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    engine.exec("SET FMTONLY ON").unwrap();

    let result = engine.query("SELECT id, name FROM t").unwrap();

    assert_eq!(result.columns, vec!["id", "name"]);
    assert_eq!(result.rows.len(), 0);

    engine.exec("SET FMTONLY OFF").unwrap();

    let result2 = engine.query("SELECT id, name FROM t").unwrap();
    assert_eq!(result2.rows.len(), 1);
}

#[test]
fn test_set_fmtonly_with_join_metadata() {
    let engine = Engine::new();

    engine
        .exec("CREATE TABLE a (id INT, val VARCHAR(10))")
        .unwrap();
    engine
        .exec("CREATE TABLE b (id INT, desc VARCHAR(10))")
        .unwrap();
    engine.exec("INSERT INTO a VALUES (1, 'x')").unwrap();
    engine.exec("INSERT INTO b VALUES (1, 'y')").unwrap();

    let result_normal = engine
        .query("SELECT a.id, b.desc FROM a JOIN b ON a.id = b.id")
        .unwrap();

    engine.exec("SET FMTONLY ON").unwrap();

    let result = engine
        .query("SELECT a.id, b.desc FROM a JOIN b ON a.id = b.id")
        .unwrap();

    assert_eq!(result_normal.columns, result.columns);
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn test_set_noexec_on_skips_execution() {
    let engine = Engine::new();

    engine
        .exec("CREATE TABLE t (id INT, name VARCHAR(50))")
        .unwrap();
    engine.exec("INSERT INTO t VALUES (1, 'Alice')").unwrap();

    engine.exec("SET NOEXEC ON").unwrap();

    let result = engine.query("SELECT id, name FROM t").unwrap();

    assert!(result.rows.is_empty());

    engine.exec("SET NOEXEC OFF").unwrap();

    let result2 = engine.query("SELECT id, name FROM t").unwrap();
    assert_eq!(result2.rows.len(), 1);
}
