
use tsql_core::executor::engine::Engine;
use tsql_core::types::Value;

#[test]
fn test_ceiling_formatting() {
    let mut engine = Engine::new();
    let sql = "SELECT CEILING(4.2)";
    let stmt = tsql_core::parser::parse_sql(sql).unwrap();
    let result = engine.execute(stmt).unwrap();
    
    let row = &result.rows[0];
    let val = &row[0];
    
    println!("Value: {:?}", val);
    
    match val {
        Value::VarChar(s) => {
            assert_eq!(s, "5", "CEILING(4.2) should be '5', got '{}'", s);
        }
        _ => panic!("Expected VarChar, got {:?}", val),
    }
}

#[test]
fn test_floor_formatting() {
    let mut engine = Engine::new();
    let sql = "SELECT FLOOR(4.8)";
    let stmt = tsql_core::parser::parse_sql(sql).unwrap();
    let result = engine.execute(stmt).unwrap();
    
    let row = &result.rows[0];
    let val = &row[0];
    
    println!("Value: {:?}", val);
    
    match val {
        Value::VarChar(s) => {
            assert_eq!(s, "4", "FLOOR(4.8) should be '4', got '{}'", s);
        }
        _ => panic!("Expected VarChar, got {:?}", val),
    }
}
