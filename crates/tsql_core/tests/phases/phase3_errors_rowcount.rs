use tsql_core::error::ErrorClass;
use tsql_core::{parse_batch, parse_sql, Engine};

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
fn test_error_taxonomy_codes() {
    let parse_err = parse_sql("SELECT").unwrap_err();
    assert_eq!(parse_err.class(), ErrorClass::Parse);
    assert_eq!(parse_err.code(), "TSQL_PARSE_ERROR");

    let e = Engine::new();
    let semantic_err = e
        .execute(parse_sql("SELECT * FROM dbo.unknown_table").unwrap())
        .unwrap_err();
    assert_eq!(semantic_err.class(), ErrorClass::Semantic);
    assert_eq!(semantic_err.code(), "TSQL_TABLE_NOT_FOUND");

    e.execute(parse_sql("CREATE TABLE t_err (name VARCHAR(1))").unwrap())
        .expect("create table");
    let execution_err = e
        .execute(parse_sql("INSERT INTO t_err VALUES ('ab')").unwrap())
        .unwrap_err();
    assert_eq!(execution_err.class(), ErrorClass::Execution);
    assert_eq!(execution_err.code(), "TSQL_EXECUTION_ERROR");
}

#[test]
fn test_batch_stops_on_first_error() {
    let mut e = Engine::new();
    let batch = parse_batch(
        "CREATE TABLE t (id INT);
         INSERT INTO t VALUES (1);
         INSERT INTO missing VALUES (2);
         INSERT INTO t VALUES (3);",
    )
    .expect("batch parse failed");
    let err = e.execute_batch(batch).unwrap_err();
    assert_eq!(err.class(), ErrorClass::Semantic);

    let r = query(&mut e, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(r.rows[0][0].to_string_value(), "1");
}

#[test]
fn test_select_row_count_json_semantics() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (id INT)");
    exec(&mut e, "INSERT INTO t VALUES (1)");
    exec(&mut e, "INSERT INTO t VALUES (2)");
    let r = query(&mut e, "SELECT id FROM t ORDER BY id");
    let json = r.to_json_result();
    assert_eq!(json.row_count, 2);
}
