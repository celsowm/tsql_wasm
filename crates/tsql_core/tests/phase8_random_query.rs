use tsql_core::{parse_sql, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_sql(sql).expect("parse")).expect("exec");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

/// Simple deterministic PRNG for reproducible tests
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.state
    }

    fn next_range(&mut self, max: usize) -> usize {
        (self.next() as usize) % max
    }

    fn next_bool(&mut self) -> bool {
        self.next() % 2 == 0
    }
}

/// Generate a random SELECT query
fn generate_select(rng: &mut SimpleRng, table: &str, columns: &[String]) -> String {
    let mut query = String::from("SELECT ");

    // Select columns or *
    if rng.next_bool() {
        query.push('*');
    } else {
        let num_cols = rng.next_range(columns.len()) + 1;
        let mut selected = Vec::new();
        for _ in 0..num_cols {
            let col = &columns[rng.next_range(columns.len())];
            if !selected.contains(col) {
                selected.push(col.clone());
            }
        }
        query.push_str(&selected.join(", "));
    }

    query.push_str(" FROM ");
    query.push_str(table);

    // Add WHERE clause
    if rng.next_bool() {
        let col = &columns[rng.next_range(columns.len())];
        let val = rng.next_range(100);
        let op = match rng.next_range(4) {
            0 => "=",
            1 => ">",
            2 => "<",
            _ => "<>",
        };
        query.push_str(&format!(" WHERE {} {} {}", col, op, val));
    }

    // Add ORDER BY
    if rng.next_bool() {
        let col = &columns[rng.next_range(columns.len())];
        let direction = if rng.next_bool() { "ASC" } else { "DESC" };
        query.push_str(&format!(" ORDER BY {} {}", col, direction));
    }

    // Add TOP
    if rng.next_bool() {
        let limit = rng.next_range(10) + 1;
        query = format!("SELECT TOP {} {}", limit, &query[7..]);
    }

    query
}

/// Generate a random INSERT query
fn generate_insert(rng: &mut SimpleRng, table: &str, columns: &[String]) -> String {
    let num_cols = rng.next_range(columns.len()) + 1;
    let mut selected_cols = Vec::new();
    let mut values = Vec::new();

    for _ in 0..num_cols {
        let col = &columns[rng.next_range(columns.len())];
        if !selected_cols.contains(col) {
            selected_cols.push(col.clone());
            values.push(rng.next_range(1000).to_string());
        }
    }

    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        selected_cols.join(", "),
        values.join(", ")
    )
}

/// Generate a random UPDATE query
fn generate_update(rng: &mut SimpleRng, table: &str, columns: &[String]) -> String {
    let set_col = &columns[rng.next_range(columns.len())];
    let set_val = rng.next_range(1000);

    let mut query = format!("UPDATE {} SET {} = {}", table, set_col, set_val);

    if rng.next_bool() {
        let where_col = &columns[rng.next_range(columns.len())];
        let where_val = rng.next_range(100);
        query.push_str(&format!(" WHERE {} = {}", where_col, where_val));
    }

    query
}

/// Generate a random DELETE query
fn generate_delete(rng: &mut SimpleRng, table: &str, columns: &[String]) -> String {
    let mut query = format!("DELETE FROM {}", table);

    if rng.next_bool() {
        let col = &columns[rng.next_range(columns.len())];
        let val = rng.next_range(100);
        query.push_str(&format!(" WHERE {} = {}", col, val));
    }

    query
}

/// Test random query generation doesn't crash
#[test]
fn test_phase8_random_queries() {
    let mut engine = Engine::new();
    let mut rng = SimpleRng::new(12345);

    // Create test table
    exec(
        &mut engine,
        "CREATE TABLE test_table (id INT, value INT, name VARCHAR(50))",
    );

    let columns = vec![
        "id".to_string(),
        "value".to_string(),
        "name".to_string(),
    ];

    // Insert some initial data
    for i in 0..20 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO test_table (id, value, name) VALUES ({}, {}, 'name_{}')",
                i,
                rng.next_range(1000),
                i
            ),
        );
    }

    // Generate and execute random queries
    let mut query_count = 0;
    let mut success_count = 0;

    for _ in 0..100 {
        let query_type = rng.next_range(4);
        let sql = match query_type {
            0 => generate_select(&mut rng, "test_table", &columns),
            1 => generate_insert(&mut rng, "test_table", &columns),
            2 => generate_update(&mut rng, "test_table", &columns),
            _ => generate_delete(&mut rng, "test_table", &columns),
        };

        query_count += 1;

        // Query should not panic
        let result = engine.execute(parse_sql(&sql).unwrap());
        if result.is_ok() {
            success_count += 1;
        }
    }

    // Most queries should succeed (some may have logic errors like empty result sets)
    assert!(
        success_count > query_count / 2,
        "At least half of random queries should succeed: {}/{}",
        success_count,
        query_count
    );
}

/// Test random aggregation queries
#[test]
fn test_phase8_random_aggregates() {
    let mut engine = Engine::new();
    let mut rng = SimpleRng::new(54321);

    exec(
        &mut engine,
        "CREATE TABLE agg_test (category VARCHAR(10), subcategory VARCHAR(10), value INT)",
    );

    // Insert test data
    let categories = ["A", "B", "C"];
    let subcategories = ["X", "Y"];

    for cat in &categories {
        for sub in &subcategories {
            for _ in 0..10 {
                exec(
                    &mut engine,
                    &format!(
                        "INSERT INTO agg_test (category, subcategory, value) VALUES ('{}', '{}', {})",
                        cat,
                        sub,
                        rng.next_range(100)
                    ),
                );
            }
        }
    }

    // Generate random aggregate queries
    let agg_functions = ["COUNT(*)", "SUM(value)", "AVG(value)", "MIN(value)", "MAX(value)"];

    for _ in 0..50 {
        let agg = agg_functions[rng.next_range(agg_functions.len())];
        let group_col = if rng.next_bool() {
            "category"
        } else {
            "subcategory"
        };

        let sql = format!(
            "SELECT {}, {} AS result FROM agg_test GROUP BY {}",
            group_col, agg, group_col
        );

        let parsed = parse_sql(&sql);
        assert!(parsed.is_ok(), "Aggregate query should parse: {}", sql);

        let result = engine.execute(parsed.unwrap());
        assert!(
            result.is_ok(),
            "Aggregate query should execute: {}",
            sql
        );
    }
}

/// Test random JOIN queries
#[test]
fn test_phase8_random_joins() {
    let mut engine = Engine::new();
    let mut rng = SimpleRng::new(98765);

    // Create two tables
    exec(
        &mut engine,
        "CREATE TABLE users (id INT, name VARCHAR(50))",
    );
    exec(
        &mut engine,
        "CREATE TABLE orders (id INT, user_id INT, amount INT)",
    );

    // Insert data
    for i in 0..10 {
        exec(
            &mut engine,
            &format!("INSERT INTO users (id, name) VALUES ({}, 'user_{}')", i, i),
        );
    }

    for i in 0..20 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO orders (id, user_id, amount) VALUES ({}, {}, {})",
                i,
                rng.next_range(10),
                rng.next_range(500)
            ),
        );
    }

    // Generate random JOIN queries
    let join_types = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"];

    for _ in 0..30 {
        let join_type = join_types[rng.next_range(join_types.len())];
        let sql = format!(
            "SELECT u.name, o.amount FROM users u {} orders o ON u.id = o.user_id",
            join_type
        );

        let parsed = parse_sql(&sql);
        assert!(parsed.is_ok(), "JOIN query should parse: {}", sql);

        let result = engine.execute(parsed.unwrap());
        assert!(result.is_ok(), "JOIN query should execute: {}", sql);
    }
}

/// Test random subquery generation
#[test]
fn test_phase8_random_subqueries() {
    let mut engine = Engine::new();
    let mut rng = SimpleRng::new(11111);

    exec(
        &mut engine,
        "CREATE TABLE products (id INT, category VARCHAR(10), price INT)",
    );

    for i in 0..20 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO products (id, category, price) VALUES ({}, '{}', {})",
                i,
                if i % 2 == 0 { "A" } else { "B" },
                rng.next_range(100)
            ),
        );
    }

    // Scalar subquery
    for _ in 0..10 {
        let sql = "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)";
        let result = engine.execute(parse_sql(sql).unwrap());
        assert!(result.is_ok(), "Scalar subquery should execute");
    }

    // IN subquery
    for _ in 0..10 {
        let sql = "SELECT * FROM products WHERE category IN (SELECT DISTINCT category FROM products WHERE price > 50)";
        let result = engine.execute(parse_sql(sql).unwrap());
        assert!(result.is_ok(), "IN subquery should execute");
    }

    // EXISTS subquery
    for _ in 0..10 {
        let sql = "SELECT * FROM products p1 WHERE EXISTS (SELECT 1 FROM products p2 WHERE p2.category = p1.category AND p2.price > p1.price)";
        let result = engine.execute(parse_sql(sql).unwrap());
        assert!(result.is_ok(), "EXISTS subquery should execute");
    }
}

/// Test random expression generation
#[test]
fn test_phase8_random_expressions() {
    let mut engine = Engine::new();
    let mut rng = SimpleRng::new(22222);

    // Test various expression combinations
    for _ in 0..100 {
        let a = rng.next_range(100) as i64;
        let b = rng.next_range(100) as i64 + 1; // Avoid division by zero
        let c = rng.next_range(100) as i64;

        let expr = match rng.next_range(6) {
            0 => format!("{} + {} * {}", a, b, c),
            1 => format!("({} + {}) * {}", a, b, c),
            2 => format!("{} - {} / {}", a, b, c),
            3 => format!("{} % {}", a, b),
            4 => format!("{} + {} - {}", a, b, c),
            _ => format!("({} - {}) / {}", a, b, c),
        };

        let sql = format!("SELECT {}", expr);
        let parsed = parse_sql(&sql);
        assert!(parsed.is_ok(), "Expression should parse: {}", sql);

        let result = engine.execute(parsed.unwrap());
        assert!(result.is_ok(), "Expression should execute: {}", sql);
    }
}

/// Test random WHERE clause generation
#[test]
fn test_phase8_random_where_clauses() {
    let mut engine = Engine::new();
    let mut rng = SimpleRng::new(33333);

    exec(
        &mut engine,
        "CREATE TABLE test (id INT, a INT, b INT, c VARCHAR(10))",
    );

    for i in 0..50 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO test (id, a, b, c) VALUES ({}, {}, {}, 'val_{}')",
                i,
                rng.next_range(100),
                rng.next_range(100),
                i % 5
            ),
        );
    }

    // Generate various WHERE clause patterns
    for _ in 0..50 {
        let col = ["a", "b"][rng.next_range(2)];
        let val = rng.next_range(100);

        let where_clause = match rng.next_range(8) {
            0 => format!("{} = {}", col, val),
            1 => format!("{} > {}", col, val),
            2 => format!("{} < {}", col, val),
            3 => format!("{} >= {}", col, val),
            4 => format!("{} <= {}", col, val),
            5 => format!("{} <> {}", col, val),
            6 => format!("{} BETWEEN {} AND {}", col, val, val + 50),
            _ => format!("{} IN ({}, {}, {})", col, val, val + 1, val + 2),
        };

        let sql = format!("SELECT * FROM test WHERE {}", where_clause);
        let parsed = parse_sql(&sql);
        assert!(parsed.is_ok(), "WHERE clause should parse: {}", sql);

        let result = engine.execute(parsed.unwrap());
        assert!(result.is_ok(), "WHERE clause should execute: {}", sql);
    }
}

/// Test random CTE generation
#[test]
fn test_phase8_random_ctes() {
    let mut engine = Engine::new();
    let mut rng = SimpleRng::new(44444);

    exec(
        &mut engine,
        "CREATE TABLE data (id INT, value INT)",
    );

    for i in 0..30 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO data (id, value) VALUES ({}, {})",
                i,
                rng.next_range(100)
            ),
        );
    }

    // Generate random CTE queries
    for _ in 0..20 {
        let threshold = rng.next_range(50) + 25;
        let sql = format!(
            "WITH filtered AS (SELECT * FROM data WHERE value > {}) SELECT COUNT(*) FROM filtered",
            threshold
        );

        let parsed = parse_sql(&sql);
        assert!(parsed.is_ok(), "CTE query should parse: {}", sql);

        let result = engine.execute(parsed.unwrap());
        assert!(result.is_ok(), "CTE query should execute: {}", sql);
    }

    // Multiple CTEs
    for _ in 0..10 {
        let sql = r#"
            WITH 
                cte1 AS (SELECT * FROM data WHERE value > 30),
                cte2 AS (SELECT * FROM data WHERE value < 70)
            SELECT COUNT(*) FROM cte1
            UNION ALL
            SELECT COUNT(*) FROM cte2
        "#;

        let parsed = parse_sql(sql);
        assert!(parsed.is_ok(), "Multiple CTEs should parse");

        let result = engine.execute(parsed.unwrap());
        if let Err(ref e) = result {
            println!("CTE error: {:?}", e);
        }
        assert!(result.is_ok(), "Multiple CTEs should execute");
    }
}

/// Test query result consistency
#[test]
fn test_phase8_query_consistency() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE consistency (id INT, value INT)",
    );

    for i in 0..20 {
        exec(
            &mut engine,
            &format!("INSERT INTO consistency (id, value) VALUES ({}, {})", i, i * 10),
        );
    }

    // Same query should return same results
    let sql = "SELECT * FROM consistency WHERE value > 50 ORDER BY id";

    let result1 = query(&mut engine, sql);
    let result2 = query(&mut engine, sql);

    assert_eq!(result1.rows.len(), result2.rows.len());
    assert_eq!(result1.columns, result2.columns);

    for (row1, row2) in result1.rows.iter().zip(result2.rows.iter()) {
        assert_eq!(row1, row2);
    }
}
