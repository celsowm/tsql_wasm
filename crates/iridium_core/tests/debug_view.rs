use iridium_core::{parse_sql, Engine};

#[test]
fn debug_view_smoke() {
    let db = Engine::new();
    let stmt = parse_sql("SELECT 1 AS n").expect("parse failed");
    let result = db
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result set");

    assert_eq!(result.columns, vec!["n".to_string()]);
    assert_eq!(result.rows.len(), 1);
}

