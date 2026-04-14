use iridium_core::{parse_sql, Database, Engine};
use std::time::Instant;

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_sql(sql).expect("parse")).expect("exec");
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

/// Performance baseline: parsing speed
#[test]
fn test_phase8_perf_parse_speed() {
    let sql = "SELECT id, name, value FROM users WHERE id > 100 ORDER BY name DESC";
    let iterations = 1000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = parse_sql(sql);
    }
    let elapsed = start.elapsed();

    let per_parse = elapsed.as_micros() as f64 / iterations as f64;
    println!(
        "Parse speed: {:.2} µs/parse ({} iterations in {:?})",
        per_parse, iterations, elapsed
    );

    // Baseline: parsing should be under 1ms per statement
    assert!(
        per_parse < 1000.0,
        "Parsing should be under 1ms, got {:.2} µs",
        per_parse
    );
}

/// Performance baseline: simple SELECT execution
#[test]
fn test_phase8_perf_select_speed() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE perf_test (id INT, value INT, name VARCHAR(50))",
    );

    // Insert test data
    for i in 0..1000 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO perf_test (id, value, name) VALUES ({}, {}, 'name_{}')",
                i, i, i
            ),
        );
    }

    let iterations = 100;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = query(&mut engine, "SELECT * FROM perf_test");
    }
    let elapsed = start.elapsed();

    let per_query = elapsed.as_micros() as f64 / iterations as f64;
    println!(
        "SELECT speed: {:.2} µs/query (1000 rows, {} iterations in {:?})",
        per_query, iterations, elapsed
    );

    // Baseline: simple SELECT should be under 10ms
    assert!(
        per_query < 10000.0,
        "SELECT should be under 10ms, got {:.2} µs",
        per_query
    );
}

/// Performance baseline: filtered SELECT
#[test]
fn test_phase8_perf_filtered_select() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE filter_test (id INT, category VARCHAR(10), value INT)",
    );

    // Insert test data
    for i in 0..1000 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO filter_test (id, category, value) VALUES ({}, '{}', {})",
                i,
                if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" },
                i * 10
            ),
        );
    }

    let iterations = 100;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = query(
            &mut engine,
            "SELECT * FROM filter_test WHERE category = 'A' AND value > 500",
        );
    }
    let elapsed = start.elapsed();

    let per_query = elapsed.as_micros() as f64 / iterations as f64;
    println!(
        "Filtered SELECT speed: {:.2} µs/query ({} iterations in {:?})",
        per_query, iterations, elapsed
    );
}

/// Performance baseline: aggregate queries
#[test]
fn test_phase8_perf_aggregates() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE agg_perf (category VARCHAR(10), value INT)",
    );

    // Insert test data
    for i in 0..1000 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO agg_perf (category, value) VALUES ('{}', {})",
                match i % 5 {
                    0 => "A",
                    1 => "B",
                    2 => "C",
                    3 => "D",
                    _ => "E",
                },
                i
            ),
        );
    }

    let iterations = 100;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = query(
            &mut engine,
            "SELECT category, COUNT(*) AS cnt, SUM(value) AS total, AVG(value) AS avg_val FROM agg_perf GROUP BY category",
        );
    }
    let elapsed = start.elapsed();

    let per_query = elapsed.as_micros() as f64 / iterations as f64;
    println!(
        "Aggregate speed: {:.2} µs/query ({} iterations in {:?})",
        per_query, iterations, elapsed
    );
}

/// Performance baseline: JOIN queries
#[test]
fn test_phase8_perf_join() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE users_perf (id INT, name VARCHAR(50))",
    );
    exec(
        &mut engine,
        "CREATE TABLE orders_perf (id INT, user_id INT, amount INT)",
    );

    // Insert test data
    for i in 0..100 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO users_perf (id, name) VALUES ({}, 'user_{}')",
                i, i
            ),
        );
    }

    for i in 0..500 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO orders_perf (id, user_id, amount) VALUES ({}, {}, {})",
                i,
                i % 100,
                i * 10
            ),
        );
    }

    let iterations = 50;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = query(
            &mut engine,
            "SELECT u.name, SUM(o.amount) FROM users_perf u INNER JOIN orders_perf o ON u.id = o.user_id GROUP BY u.name",
        );
    }
    let elapsed = start.elapsed();

    let per_query = elapsed.as_micros() as f64 / iterations as f64;
    println!(
        "JOIN speed: {:.2} µs/query ({} iterations in {:?})",
        per_query, iterations, elapsed
    );
}

/// Performance baseline: batch execution
#[test]
fn test_phase8_perf_batch() {
    let _engine = Engine::new();

    let batch = r#"
        CREATE TABLE batch_test (id INT, value INT);
        INSERT INTO batch_test (id, value) VALUES (1, 10);
        INSERT INTO batch_test (id, value) VALUES (2, 20);
        INSERT INTO batch_test (id, value) VALUES (3, 30);
        SELECT * FROM batch_test;
        UPDATE batch_test SET value = 100 WHERE id = 1;
        DELETE FROM batch_test WHERE id = 3;
        SELECT * FROM batch_test;
    "#;

    let iterations = 50;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = iridium_core::parse_batch(batch);
    }
    let elapsed = start.elapsed();

    let per_batch = elapsed.as_micros() as f64 / iterations as f64;
    println!(
        "Batch parse speed: {:.2} µs/batch ({} iterations in {:?})",
        per_batch, iterations, elapsed
    );
}

/// Performance baseline: INSERT throughput
#[test]
fn test_phase8_perf_insert_throughput() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE insert_test (id INT, value INT, name VARCHAR(50))",
    );

    let count = 1000;
    let start = Instant::now();
    for i in 0..count {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO insert_test (id, value, name) VALUES ({}, {}, 'name_{}')",
                i, i, i
            ),
        );
    }
    let elapsed = start.elapsed();

    let per_insert = elapsed.as_micros() as f64 / count as f64;
    let inserts_per_sec = 1_000_000.0 / per_insert;
    println!(
        "INSERT throughput: {:.2} µs/insert ({:.0} inserts/sec, {} total in {:?})",
        per_insert, inserts_per_sec, count, elapsed
    );

    // Baseline: INSERT should be under 5ms each
    assert!(
        per_insert < 5000.0,
        "INSERT should be under 5ms, got {:.2} µs",
        per_insert
    );
}

/// Performance baseline: memory efficiency
#[test]
fn test_phase8_perf_memory() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE mem_test (id INT, data VARCHAR(100))",
    );

    // Insert data and track approximate memory usage
    let count = 1000;
    for i in 0..count {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO mem_test (id, data) VALUES ({}, '{}')",
                i,
                "x".repeat(50)
            ),
        );
    }

    // Query to verify data is accessible
    let result = query(&mut engine, "SELECT COUNT(*) FROM mem_test");
    assert_eq!(result.rows[0][0], iridium_core::types::Value::BigInt(count));

    println!("Memory test: {} rows with 50-byte strings inserted", count);
}

/// Performance baseline: transaction overhead
#[test]
fn test_phase8_perf_transaction() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE tx_test (id INT, value INT)",
    );

    let iterations = 100;
    let start = Instant::now();
    for i in 0..iterations {
        exec(&mut engine, "BEGIN TRANSACTION");
        exec(
            &mut engine,
            &format!("INSERT INTO tx_test (id, value) VALUES ({}, {})", i, i),
        );
        exec(&mut engine, "COMMIT");
    }
    let elapsed = start.elapsed();

    let per_tx = elapsed.as_micros() as f64 / iterations as f64;
    println!(
        "Transaction overhead: {:.2} µs/tx ({} iterations in {:?})",
        per_tx, iterations, elapsed
    );
}

/// Performance baseline: complex query
#[test]
fn test_phase8_perf_complex_query() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE sales (id INT, product VARCHAR(20), category VARCHAR(20), amount INT, sale_date DATE)",
    );

    // Insert test data
    for i in 0..500 {
        exec(
            &mut engine,
            &format!(
                "INSERT INTO sales (id, product, category, amount, sale_date) VALUES ({}, 'product_{}', '{}', {}, '2024-01-01')",
                i,
                i % 20,
                if i % 3 == 0 { "Electronics" } else if i % 3 == 1 { "Clothing" } else { "Food" },
                (i % 100) * 10
            ),
        );
    }

    let complex_query = r#"
        WITH category_totals AS (
            SELECT category, SUM(amount) AS total
            FROM sales
            GROUP BY category
        ),
        top_categories AS (
            SELECT category, total
            FROM category_totals
            WHERE total > 1000
        )
        SELECT tc.category, tc.total, COUNT(*) AS sale_count
        FROM top_categories tc
        INNER JOIN sales s ON tc.category = s.category
        GROUP BY tc.category, tc.total
        ORDER BY tc.total DESC
    "#;

    let iterations = 20;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = query(&mut engine, complex_query);
    }
    let elapsed = start.elapsed();

    let per_query = elapsed.as_micros() as f64 / iterations as f64;
    println!(
        "Complex query speed: {:.2} µs/query ({} iterations in {:?})",
        per_query, iterations, elapsed
    );
}

/// Performance baseline: multi-session
#[test]
fn test_phase8_perf_multi_session() {
    let db = Database::new();

    // Create table in first session
    let sid0 = db.create_session();
    db.execute_session(
        sid0,
        parse_sql("CREATE TABLE t (id INT PRIMARY KEY)").unwrap(),
    )
    .unwrap();

    let session_count = 10;
    let mut sessions = Vec::new();

    for _ in 0..session_count {
        sessions.push(db.create_session());
    }

    let start = Instant::now();
    for (i, sid) in sessions.iter().enumerate() {
        // Each session inserts a row
        db.execute_session(
            *sid,
            parse_sql(&format!("INSERT INTO t (id) VALUES ({})", i)).unwrap(),
        )
        .unwrap();
    }
    let elapsed = start.elapsed();

    let per_session = elapsed.as_micros() as f64 / session_count as f64;
    println!(
        "Multi-session insert: {:.2} µs/session ({} sessions in {:?})",
        per_session, session_count, elapsed
    );
}

