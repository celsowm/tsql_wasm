use iridium_core::{parse_sql, DbError, Engine};

fn make_abs_chain(depth: usize) -> String {
    let mut sql = "1".to_string();
    for _ in 0..depth {
        sql = format!("ABS({})", sql);
    }
    sql
}

#[test]
fn test_parser_recursion_limit() {
    // Keep this well above the parser limit so we still verify the guard rail.
    let sql = format!("SELECT {}", make_abs_chain(64));

    // In some environments, this might still blow the stack before hitting the limit
    // if the stack is very small. We rely on the limit being hit first.
    let result = parse_sql(&sql);

    match result {
        Err(DbError::Parse(msg)) => {
            assert!(msg.contains("recursion limit exceeded"));
        }
        _ => panic!("Expected parser recursion depth error, got {:?}", result),
    }
}

#[test]
fn test_executor_recursion_limit_safe() {
    let engine = Engine::new();

    // Test a depth that is safe for both parser and evaluator.
    let sql = format!("SELECT {}", make_abs_chain(2));

    let stmt = parse_sql(&sql).expect("Should pass parser at depth 2");
    let result = engine.execute(stmt);

    assert!(result.is_ok(), "Expected OK for depth 2, got {:?}", result);
}
