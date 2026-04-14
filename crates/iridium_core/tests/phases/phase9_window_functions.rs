use iridium_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("parse failed: {}", sql));
    engine.execute(stmt).unwrap_or_else(|_| panic!("execute failed: {}", sql));
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    let stmt = parse_sql(sql).unwrap_or_else(|_| panic!("parse failed: {}", sql));
    engine
        .execute(stmt)
        .unwrap_or_else(|_| panic!("execute failed: {}", sql))
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
    assert_eq!(r.rows[2][2], Value::Int(3));
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

#[test]
fn test_window_aggregates() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE sales_agg (id INT, product VARCHAR(20), amount INT)");
    exec(&mut e, "INSERT INTO sales_agg VALUES (1, 'A', 100)");
    exec(&mut e, "INSERT INTO sales_agg VALUES (2, 'A', 200)");
    exec(&mut e, "INSERT INTO sales_agg VALUES (3, 'B', 150)");
    exec(&mut e, "INSERT INTO sales_agg VALUES (4, 'B', 250)");

    // Running total
    let r = query(&mut e, "SELECT id, amount, SUM(amount) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS running_total FROM sales_agg ORDER BY id");
    println!("test_window_aggregates (SUM): {:?}", r);
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0][2], Value::BigInt(100));
    assert_eq!(r.rows[1][2], Value::BigInt(300));
    assert_eq!(r.rows[2][2], Value::BigInt(450));
    assert_eq!(r.rows[3][2], Value::BigInt(700));

    // Partitioned average
    let r = query(&mut e, "SELECT product, amount, AVG(amount) OVER (PARTITION BY product ORDER BY product ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS avg_amount FROM sales_agg ORDER BY product, id");
    println!("test_window_aggregates (AVG): {:?}", r);
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0][2], Value::Int(150)); // 150
    assert_eq!(r.rows[1][2], Value::Int(150));
    assert_eq!(r.rows[2][2], Value::Int(200)); // 200
    assert_eq!(r.rows[3][2], Value::Int(200));
}

#[test]
fn test_window_frames() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE frames (id INT, val INT)");
    exec(&mut e, "INSERT INTO frames VALUES (1, 10)");
    exec(&mut e, "INSERT INTO frames VALUES (2, 20)");
    exec(&mut e, "INSERT INTO frames VALUES (3, 30)");
    exec(&mut e, "INSERT INTO frames VALUES (4, 40)");

    // Moving average (3 rows: preceding, current, following)
    let r = query(&mut e, "SELECT val, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_sum FROM frames ORDER BY id");
    println!("test_window_frames: {:?}", r);
    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0][1], Value::BigInt(30)); // 10+20
    assert_eq!(r.rows[1][1], Value::BigInt(60)); // 10+20+30
    assert_eq!(r.rows[2][1], Value::BigInt(90)); // 20+30+40
    assert_eq!(r.rows[3][1], Value::BigInt(70)); // 30+40
}

#[test]
fn test_ntile() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE ntile_test (id INT)");
    for i in 1..=10 {
        exec(&mut e, &format!("INSERT INTO ntile_test VALUES ({})", i));
    }

    let r = query(&mut e, "SELECT id, NTILE(4) OVER (ORDER BY id) AS tile FROM ntile_test ORDER BY id");
    println!("test_ntile: {:?}", r);
    // 10 rows into 4 buckets: 3, 3, 2, 2
    assert_eq!(r.rows[0][1], Value::BigInt(1));
    assert_eq!(r.rows[2][1], Value::BigInt(1));
    assert_eq!(r.rows[3][1], Value::BigInt(2));
    assert_eq!(r.rows[5][1], Value::BigInt(2));
    assert_eq!(r.rows[6][1], Value::BigInt(3));
    assert_eq!(r.rows[7][1], Value::BigInt(3));
    assert_eq!(r.rows[8][1], Value::BigInt(4));
    assert_eq!(r.rows[9][1], Value::BigInt(4));
}

#[test]
fn test_first_last_value() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE first_last (id INT, val VARCHAR(10))");
    exec(&mut e, "INSERT INTO first_last VALUES (1, 'A')");
    exec(&mut e, "INSERT INTO first_last VALUES (2, 'B')");
    exec(&mut e, "INSERT INTO first_last VALUES (3, 'C')");

    let r = query(&mut e, "SELECT id, FIRST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fv,
                                  LAST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv
                           FROM first_last ORDER BY id");
    println!("test_first_last_value: {:?}", r);
    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Value::VarChar("A".to_string()));
    assert_eq!(r.rows[0][2], Value::VarChar("C".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("A".to_string()));
    assert_eq!(r.rows[1][2], Value::VarChar("C".to_string()));
    assert_eq!(r.rows[2][1], Value::VarChar("A".to_string()));
    assert_eq!(r.rows[2][2], Value::VarChar("C".to_string()));
}

