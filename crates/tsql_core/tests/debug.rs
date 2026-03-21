use tsql_core::{parse_sql, Engine};

#[test]
fn test_debug_join() {
    let mut engine = Engine::new();
    
    // Criar tabela orders primeiro
    let stmt = parse_sql("CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, user_id INT)").unwrap();
    engine.execute(stmt).unwrap();
    
    // Criar tabela users
    let stmt = parse_sql("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))").unwrap();
    engine.execute(stmt).unwrap();
    
    // Inserir dados
    engine.execute(parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO orders (id, user_id) VALUES (101, 1)").unwrap()).unwrap();
    engine.execute(parse_sql("INSERT INTO orders (id, user_id) VALUES (102, 2)").unwrap()).unwrap();
    
    // Testar SELECT simples primeiro
    let stmt = parse_sql("SELECT * FROM orders").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("Orders: {:?}", result);
    assert_eq!(result.rows.len(), 2);
    
    let stmt = parse_sql("SELECT * FROM users").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    println!("Users: {:?}", result);
    assert_eq!(result.rows.len(), 2);
}
