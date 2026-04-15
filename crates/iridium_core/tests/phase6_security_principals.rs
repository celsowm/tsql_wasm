use iridium_core::{parse_sql, types::Value, Engine};

#[allow(dead_code)]
fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

#[test]
fn test_suser_sname_function() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT SUSER_SNAME() AS usr");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_suser_id_function() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT SUSER_ID() AS id");
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(
        result.rows[0][0],
        Value::Int(_) | Value::BigInt(_)
    ));
}

#[test]
fn test_user_name_function() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT USER_NAME() AS usr");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_user_id_function() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT USER_ID() AS id");
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(
        result.rows[0][0],
        Value::Int(_) | Value::BigInt(_)
    ));
}

#[test]
fn test_database_principals_view() {
    let mut engine = Engine::new();

    let result = query(
        &mut engine,
        "SELECT name, type, type_desc FROM sys.database_principals",
    );
    assert!(result.rows.len() >= 1);
}

#[test]
fn test_database_permissions_view() {
    let mut engine = Engine::new();

    let result = query(
        &mut engine,
        "SELECT class_desc, permission_name, state_desc FROM sys.database_permissions",
    );
    assert!(result.columns.iter().any(|c| c == "class_desc"));
    assert!(result.columns.iter().any(|c| c == "permission_name"));
    assert!(result.columns.iter().any(|c| c == "state_desc"));
}

#[test]
fn test_database_role_members_view() {
    let mut engine = Engine::new();

    let result = query(
        &mut engine,
        "SELECT role_principal_id, member_principal_id FROM sys.database_role_members",
    );
    assert!(result.columns.len() >= 2);
}

#[test]
fn test_builtin_logins_view() {
    let mut engine = Engine::new();

    let result = query(
        &mut engine,
        "SELECT name, type_desc FROM sys.server_principals",
    );
    assert!(result.rows.len() >= 1);
}

#[test]
fn test_app_name_function() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT APP_NAME() AS app");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_host_name_function() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT HOST_NAME() AS host");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_user_name_with_id() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT USER_NAME(1) AS usr");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_suser_sname_with_sid() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT SUSER_SNAME(0x01) AS usr");
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_db_name_function() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT DB_NAME() AS db");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.rows[0][0].is_null());
}

#[test]
fn test_db_id_function() {
    let mut engine = Engine::new();

    let result = query(&mut engine, "SELECT DB_ID() AS id");
    assert_eq!(result.rows.len(), 1);
    assert!(matches!(
        result.rows[0][0],
        Value::Int(_) | Value::BigInt(_)
    ));
}
