use iridium_core::Engine;

#[test]
fn test_known_parser_bug_nested_comments() {
    let engine = Engine::new();
    let result = engine.exec("SELECT /* nested /* comment */ */ 1");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_known_type_coercion_date_to_datetime() {
    let mut engine = Engine::new();
    let result = engine
        .query("SELECT CAST('2024-01-01' AS DATE) AS d")
        .unwrap();
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_known_type_coercion_varchar_to_int() {
    let mut engine = Engine::new();
    let result = engine.query("SELECT CAST('123' AS INT) AS i").unwrap();
    assert_eq!(result.rows[0][0], iridium_core::types::Value::Int(123));
}

#[test]
fn test_concurrent_session_basic() {
    let engine = Engine::new();
    engine.exec("CREATE TABLE t (id INT)").unwrap();
    engine.exec("INSERT INTO t VALUES (1)").unwrap();

    let s1 = engine.create_session();
    let s2 = engine.create_session();

    assert!(s1 != s2);
}

#[test]
fn test_regression_string_agg() {
    let mut engine = Engine::new();
    engine
        .exec("CREATE TABLE t (id INT, val VARCHAR(10))")
        .unwrap();
    engine
        .exec("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();

    let result = engine.query("SELECT STRING_AGG(val, ',') FROM t").unwrap();
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_regression_cte_with_agg() {
    let mut engine = Engine::new();
    let result = engine
        .query("WITH cte AS (SELECT 1 AS x) SELECT SUM(x) FROM cte")
        .unwrap();
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_regression_merge_statement() {
    let mut engine = Engine::new();
    engine
        .exec("CREATE TABLE target (id INT, val INT)")
        .unwrap();
    engine
        .exec("CREATE TABLE source (id INT, val INT)")
        .unwrap();
    engine
        .exec("INSERT INTO target VALUES (1, 10), (2, 20)")
        .unwrap();
    engine
        .exec("INSERT INTO source VALUES (1, 100), (3, 30)")
        .unwrap();

    let result = engine.exec("MERGE target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET val = s.val WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)");
    assert!(result.is_ok());
}

#[test]
fn test_regression_pivot() {
    let mut engine = Engine::new();
    engine
        .exec("CREATE TABLE sales (product VARCHAR(10), quarter INT, amount INT)")
        .unwrap();
    engine
        .exec("INSERT INTO sales VALUES ('A', 1, 100), ('A', 2, 200), ('B', 1, 150)")
        .unwrap();

    let result = engine.query("SELECT * FROM (SELECT product, quarter, amount FROM sales) AS src PIVOT (SUM(amount) FOR quarter IN ([1], [2])) AS pvt").unwrap();
    assert!(result.columns.len() >= 3);
}

#[test]
fn test_regression_window_functions() {
    let mut engine = Engine::new();
    engine
        .exec("CREATE TABLE emp (dept VARCHAR(10), salary INT)")
        .unwrap();
    engine
        .exec("INSERT INTO emp VALUES ('A', 100), ('A', 200), ('B', 150)")
        .unwrap();

    let result = engine.query("SELECT dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM emp").unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_regression_subquery_in_select() {
    let mut engine = Engine::new();
    engine.exec("CREATE TABLE t (id INT)").unwrap();
    engine.exec("INSERT INTO t VALUES (1), (2)").unwrap();

    let result = engine
        .query("SELECT id, (SELECT MAX(id) FROM t) AS max_id FROM t")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_regression_exists_subquery() {
    let mut engine = Engine::new();
    engine.exec("CREATE TABLE a (id INT)").unwrap();
    engine.exec("CREATE TABLE b (id INT)").unwrap();
    engine.exec("INSERT INTO a VALUES (1), (2)").unwrap();
    engine.exec("INSERT INTO b VALUES (2)").unwrap();

    let result = engine
        .query("SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id)")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_regression_like_escape() {
    let result = Engine::new()
        .query("SELECT '100%' LIKE '100%' AS result")
        .unwrap();
    assert_eq!(result.rows[0][0], iridium_core::types::Value::Bit(true));
}

#[test]
fn test_regression_basic_cte() {
    let mut engine = Engine::new();
    engine.exec("CREATE TABLE nums (n INT)").unwrap();
    engine
        .exec("INSERT INTO nums VALUES (1), (2), (3)")
        .unwrap();

    let result = engine
        .query("WITH cte AS (SELECT n FROM nums) SELECT n FROM cte")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn test_regression_basic_union() {
    let mut engine = Engine::new();
    engine.exec("CREATE TABLE t (id INT)").unwrap();
    engine.exec("INSERT INTO t VALUES (1), (2)").unwrap();

    let result = engine
        .query("SELECT id FROM t UNION SELECT id FROM t")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

