use tsql_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect(&format!("parse failed: {}", sql));
    engine.execute(stmt).expect(&format!("execute failed: {}", sql));
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    let stmt = parse_sql(sql).expect(&format!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .expect(&format!("execute failed: {}", sql))
        .expect("expected result")
}

#[test]
fn test_row_number_basic() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE employees (id INT, name VARCHAR(50))");
    exec(&mut e, "INSERT INTO employees VALUES (1, 'Alice')");
    exec(&mut e, "INSERT INTO employees VALUES (2, 'Bob')");
    exec(&mut e, "INSERT INTO employees VALUES (3, 'Charlie')");
    
    let r = query(&mut e, "SELECT ROW_NUMBER() OVER (ORDER BY name) AS rownum, name FROM employees ORDER BY name");
    println!("test_row_number_basic: {:?}", r);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("Bob".to_string()));
}

#[test]
fn test_row_number_partition() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE sales (id INT, product VARCHAR(20), amount INT)");
    exec(&mut e, "INSERT INTO sales VALUES (1, 'A', 100)");
    exec(&mut e, "INSERT INTO sales VALUES (2, 'A', 200)");
    exec(&mut e, "INSERT INTO sales VALUES (3, 'B', 150)");
    exec(&mut e, "INSERT INTO sales VALUES (4, 'B', 250)");
    
    let r = query(&mut e, "SELECT ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount) AS rn, product, amount FROM sales ORDER BY product, amount");
    println!("test_row_number_partition: {:?}", r);
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[2][0], Value::Int(1));
}

#[test]
fn test_rank() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE scores (id INT, player VARCHAR(50), points INT)");
    exec(&mut e, "INSERT INTO scores VALUES (1, 'Alice', 100)");
    exec(&mut e, "INSERT INTO scores VALUES (2, 'Bob', 100)");
    exec(&mut e, "INSERT INTO scores VALUES (3, 'Charlie', 90)");
    
    let r = query(&mut e, "SELECT player, points, RANK() OVER (ORDER BY points DESC) AS rk FROM scores ORDER BY points DESC, player");
    println!("test_rank: {:?}", r);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(1));
    assert_eq!(r.rows[1][0], Value::VarChar("Bob".to_string()));
    assert_eq!(r.rows[1][2], Value::Int(1));
}

#[test]
fn test_dense_rank() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE scores2 (id INT, player VARCHAR(50), points INT)");
    exec(&mut e, "INSERT INTO scores2 VALUES (1, 'Alice', 100)");
    exec(&mut e, "INSERT INTO scores2 VALUES (2, 'Bob', 100)");
    exec(&mut e, "INSERT INTO scores2 VALUES (3, 'Charlie', 90)");
    
    let r = query(&mut e, "SELECT player, points, DENSE_RANK() OVER (ORDER BY points DESC) AS dr FROM scores2 ORDER BY points DESC, player");
    println!("test_dense_rank: {:?}", r);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][2], Value::Int(1));
    assert_eq!(r.rows[1][2], Value::Int(1));
    assert_eq!(r.rows[2][2], Value::Int(2));
}

#[test]
fn test_lead() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE timeline (id INT, event VARCHAR(50), day INT)");
    exec(&mut e, "INSERT INTO timeline VALUES (1, 'Start', 1)");
    exec(&mut e, "INSERT INTO timeline VALUES (2, 'Middle', 2)");
    exec(&mut e, "INSERT INTO timeline VALUES (3, 'End', 3)");
    
    let r = query(&mut e, "SELECT event, day, LEAD(event) OVER (ORDER BY day) AS next_event FROM timeline ORDER BY day");
    println!("test_lead: {:?}", r);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][2], Value::VarChar("Middle".to_string()));
    assert_eq!(r.rows[1][2], Value::VarChar("End".to_string()));
    assert_eq!(r.rows[2][2], Value::Null);
}

#[test]
fn test_lag() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE timeline2 (id INT, event VARCHAR(50), day INT)");
    exec(&mut e, "INSERT INTO timeline2 VALUES (1, 'Start', 1)");
    exec(&mut e, "INSERT INTO timeline2 VALUES (2, 'Middle', 2)");
    exec(&mut e, "INSERT INTO timeline2 VALUES (3, 'End', 3)");
    
    let r = query(&mut e, "SELECT event, day, LAG(event) OVER (ORDER BY day) AS prev_event FROM timeline2 ORDER BY day");
    println!("test_lag: {:?}", r);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][2], Value::Null);
    assert_eq!(r.rows[1][2], Value::VarChar("Start".to_string()));
    assert_eq!(r.rows[2][2], Value::VarChar("Middle".to_string()));
}