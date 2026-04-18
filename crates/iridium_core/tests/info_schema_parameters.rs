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
fn test_info_schema_parameters_procedure() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE PROCEDURE test_proc @p1 INT, @p2 BIGINT AS BEGIN END",
    );

    let r = query(&mut e, "SELECT SPECIFIC_NAME, PARAMETER_NAME, PARAMETER_MODE, DATA_TYPE FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_NAME = 'test_proc' ORDER BY ORDINAL_POSITION");
    println!("test_info_schema_parameters_procedure: {:?}", r);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][1], Value::VarChar("@p1".to_string()));
    assert_eq!(r.rows[0][2], Value::VarChar("IN".to_string()));
    assert_eq!(r.rows[0][3], Value::VarChar("int".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("@p2".to_string()));
    assert_eq!(r.rows[1][3], Value::VarChar("bigint".to_string()));
}

#[test]
fn test_info_schema_parameters_function() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE FUNCTION test_func (@x INT, @y BIGINT) RETURNS INT AS BEGIN RETURN 0 END",
    );

    let r = query(&mut e, "SELECT PARAMETER_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_NAME = 'test_func' ORDER BY ORDINAL_POSITION");
    println!("test_info_schema_parameters_function: {:?}", r);
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("@x".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("int".to_string()));
}

#[test]
fn test_info_schema_parameters_empty() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE PROCEDURE no_params AS BEGIN END");

    let r = query(
        &mut e,
        "SELECT * FROM INFORMATION_SCHEMA.PARAMETERS WHERE SPECIFIC_NAME = 'no_params'",
    );
    println!("test_info_schema_parameters_empty: {:?}", r);
    assert_eq!(r.rows.len(), 0);
}
