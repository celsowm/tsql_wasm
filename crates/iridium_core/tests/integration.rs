use iridium_core::{parse_sql, types::Value, Engine};

#[test]
fn test_basic_flow() {
    let engine = Engine::new();

    // CREATE TABLE
    let stmt =
        parse_sql("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))").unwrap();
    assert!(engine.execute(stmt).unwrap().is_none());

    // INSERT
    let stmt = parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    assert!(engine.execute(stmt).unwrap().is_none());

    // SELECT
    let stmt = parse_sql("SELECT * FROM users").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    assert_eq!(result.columns, vec!["id", "name"]);
    assert_eq!(result.rows.len(), 1);

    // UPDATE
    let stmt = parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
    assert!(engine.execute(stmt).unwrap().is_none());

    // DELETE
    let stmt = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
    assert!(engine.execute(stmt).unwrap().is_none());

    // Verificar que deletou
    let stmt = parse_sql("SELECT * FROM users").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn test_join() {
    let engine = Engine::new();

    // Criar tabelas
    engine
        .execute(
            parse_sql("CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, user_id INT)").unwrap(),
        )
        .unwrap();
    engine
        .execute(
            parse_sql("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))")
                .unwrap(),
        )
        .unwrap();

    // Inserir dados
    engine
        .execute(parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO orders (id, user_id) VALUES (101, 1)").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO orders (id, user_id) VALUES (102, 2)").unwrap())
        .unwrap();

    // Testar JOIN
    let stmt =
        parse_sql("SELECT o.id, u.name FROM orders o INNER JOIN users u ON o.user_id = u.id")
            .unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    assert_eq!(result.columns, vec!["id", "name"]);
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_group_by() {
    let engine = Engine::new();

    engine.execute(parse_sql("CREATE TABLE sales (id INT NOT NULL PRIMARY KEY, category VARCHAR(50), amount INT)").unwrap()).unwrap();
    engine
        .execute(
            parse_sql("INSERT INTO sales (id, category, amount) VALUES (1, 'A', 100)").unwrap(),
        )
        .unwrap();
    engine
        .execute(
            parse_sql("INSERT INTO sales (id, category, amount) VALUES (2, 'A', 200)").unwrap(),
        )
        .unwrap();
    engine
        .execute(
            parse_sql("INSERT INTO sales (id, category, amount) VALUES (3, 'B', 150)").unwrap(),
        )
        .unwrap();

    let stmt = parse_sql("SELECT category, COUNT(*) as cnt FROM sales GROUP BY category").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    assert_eq!(result.columns, vec!["category", "cnt"]);
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_order_by() {
    let engine = Engine::new();

    engine
        .execute(
            parse_sql("CREATE TABLE items (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))")
                .unwrap(),
        )
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO items (id, name) VALUES (3, 'C')").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO items (id, name) VALUES (1, 'A')").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO items (id, name) VALUES (2, 'B')").unwrap())
        .unwrap();

    let stmt = parse_sql("SELECT * FROM items ORDER BY id").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    assert_eq!(result.rows.len(), 3);

    let first_row = &result.rows[0];
    if let Value::Int(n) = &first_row[0] {
        assert_eq!(*n, 1);
    } else {
        panic!("Expected Int, got {:?}", first_row[0]);
    }
}

#[test]
fn test_identity() {
    let engine = Engine::new();

    engine
        .execute(
            parse_sql(
                "CREATE TABLE auto (id INT IDENTITY(1,1) NOT NULL PRIMARY KEY, data VARCHAR(50))",
            )
            .unwrap(),
        )
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO auto (data) VALUES ('first')").unwrap())
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO auto (data) VALUES ('second')").unwrap())
        .unwrap();

    let stmt = parse_sql("SELECT id FROM auto ORDER BY id").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[test]
fn test_default_values() {
    let engine = Engine::new();

    engine
        .execute(
            parse_sql("CREATE TABLE def (id INT NOT NULL PRIMARY KEY, val INT DEFAULT 42)")
                .unwrap(),
        )
        .unwrap();
    engine
        .execute(parse_sql("INSERT INTO def (id) VALUES (1)").unwrap())
        .unwrap();

    let stmt = parse_sql("SELECT val FROM def").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_top() {
    let engine = Engine::new();

    engine
        .execute(parse_sql("CREATE TABLE nums (id INT NOT NULL PRIMARY KEY)").unwrap())
        .unwrap();
    for i in 1..=10 {
        engine
            .execute(parse_sql(&format!("INSERT INTO nums (id) VALUES ({})", i)).unwrap())
            .unwrap();
    }

    let stmt = parse_sql("SELECT TOP 3 * FROM nums ORDER BY id").unwrap();
    let result = engine.execute(stmt).unwrap().unwrap();
    assert_eq!(result.rows.len(), 3);
}

