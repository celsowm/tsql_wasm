use iridium_core::executor::database::Engine;

#[test]
fn test_niladic_functions_datetime() {
    let engine = Engine::new();

    // CURRENT_TIMESTAMP
    let res = engine.query("SELECT CURRENT_TIMESTAMP").unwrap();
    assert_eq!(res.rows.len(), 1);
    // Based on Value::to_string_value for NaiveDateTime: v.format("%Y-%m-%d %H:%M:%S%.f").to_string()
    // It doesn't contain 'T'.
    assert!(res.rows[0][0].to_string_value().contains(' '));
    assert!(res.rows[0][0].to_string_value().contains(':'));

    // CURRENT_DATE
    let res = engine.query("SELECT CURRENT_DATE").unwrap();
    assert_eq!(res.rows.len(), 1);
    // Date format is YYYY-MM-DD
    assert!(res.rows[0][0].to_string_value().len() >= 10);
    assert!(res.rows[0][0].to_string_value().contains('-'));

    // CURRENT_TIME
    let res = engine.query("SELECT CURRENT_TIME").unwrap();
    assert_eq!(res.rows.len(), 1);
    // Time format HH:MM:SS...
    assert!(res.rows[0][0].to_string_value().contains(':'));
}

#[test]
fn test_niladic_functions_user() {
    let engine = Engine::new();

    // CURRENT_USER
    let res = engine.query("SELECT CURRENT_USER").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_string_value(), "dbo");

    // SESSION_USER
    let res = engine.query("SELECT SESSION_USER").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_string_value(), "dbo");

    // SYSTEM_USER
    let res = engine.query("SELECT SYSTEM_USER").unwrap();
    assert_eq!(res.rows.len(), 1);
    assert_eq!(res.rows[0][0].to_string_value(), "sa");
}

#[test]
fn test_niladic_functions_in_expressions() {
    let engine = Engine::new();

    // Using in a WHERE clause (even if it's a bit silly)
    let res = engine.query("SELECT 1 WHERE CURRENT_USER = 'dbo'").unwrap();
    assert_eq!(res.rows.len(), 1);

    let res = engine.query("SELECT 1 WHERE SYSTEM_USER = 'sa'").unwrap();
    assert_eq!(res.rows.len(), 1);
}

#[test]
fn test_niladic_functions_case_insensitivity() {
    let engine = Engine::new();

    let res = engine.query("SELECT current_timestamp").unwrap();
    assert_eq!(res.rows.len(), 1);

    let res = engine.query("SELECT CuRrEnT_uSeR").unwrap();
    assert_eq!(res.rows.len(), 1);
}
