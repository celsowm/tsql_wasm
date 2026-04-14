use iridium_core::{parse_sql, types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

fn setup_sales(engine: &mut Engine) {
    exec(
        engine,
        "CREATE TABLE sales (region VARCHAR(50), quarter VARCHAR(10), amount INT)",
    );
    exec(
        engine,
        "INSERT INTO sales VALUES ('East', 'Q1', 1000), ('East', 'Q2', 2000), ('East', 'Q3', 1500), ('East', 'Q4', 3000)",
    );
    exec(
        engine,
        "INSERT INTO sales VALUES ('West', 'Q1', 500), ('West', 'Q2', 1500), ('West', 'Q3', 1000), ('West', 'Q4', 2000)",
    );
    exec(
        engine,
        "INSERT INTO sales VALUES ('North', 'Q1', 800), ('North', 'Q2', 1200), ('North', 'Q3', 900), ('North', 'Q4', 1100)",
    );
}

fn setup_employees(engine: &mut Engine) {
    exec(
        engine,
        "CREATE TABLE employees (dept VARCHAR(50), emp VARCHAR(50), sales INT)",
    );
    exec(
        engine,
        "INSERT INTO employees VALUES ('Sales', 'Alice', 100), ('Sales', 'Bob', 200), ('Sales', 'Carol', 150)",
    );
    exec(
        engine,
        "INSERT INTO employees VALUES ('Marketing', 'Dave', 80), ('Marketing', 'Eve', 120)",
    );
}

// ─── Basic PIVOT with SUM ────────────────────────────────────────────────

#[test]
fn test_pivot_basic_sum() {
    let mut e = Engine::new();
    setup_sales(&mut e);

    let r = query(
        &mut e,
        "SELECT * FROM (SELECT region, quarter, amount FROM sales) AS s \
         PIVOT (SUM(amount) FOR quarter IN (Q1, Q2, Q3, Q4)) AS p \
         ORDER BY region",
    );

    assert_eq!(r.columns.len(), 5);
    assert_eq!(r.rows.len(), 3);

    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(1000));
    assert_eq!(r.rows[0][2], Value::Int(2000));
    assert_eq!(r.rows[0][3], Value::Int(1500));
    assert_eq!(r.rows[0][4], Value::Int(3000));

    assert_eq!(r.rows[1][0], Value::VarChar("North".to_string()));
    assert_eq!(r.rows[1][1], Value::Int(800));
    assert_eq!(r.rows[1][4], Value::Int(1100));

    assert_eq!(r.rows[2][0], Value::VarChar("West".to_string()));
    assert_eq!(r.rows[2][1], Value::Int(500));
    assert_eq!(r.rows[2][4], Value::Int(2000));
}

#[test]
fn test_pivot_basic_count() {
    let mut e = Engine::new();
    setup_sales(&mut e);

    let r = query(
        &mut e,
        "SELECT region, Q1, Q2, Q3, Q4 FROM (SELECT region, quarter, amount FROM sales) AS s \
         PIVOT (COUNT(amount) FOR quarter IN (Q1, Q2, Q3, Q4)) AS p \
         ORDER BY region",
    );

    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Value::Int(1));
    assert_eq!(r.rows[0][2], Value::Int(1));
    assert_eq!(r.rows[0][3], Value::Int(1));
    assert_eq!(r.rows[0][4], Value::Int(1));
}

#[test]
fn test_pivot_basic_avg() {
    let mut e = Engine::new();
    setup_sales(&mut e);

    let r = query(
        &mut e,
        "SELECT region, Q1, Q2 FROM (SELECT region, quarter, amount FROM sales) AS s \
         PIVOT (AVG(amount) FOR quarter IN (Q1, Q2)) AS p \
         ORDER BY region",
    );

    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::BigInt(1000));
    assert_eq!(r.rows[0][2], Value::BigInt(2000));
    assert_eq!(r.rows[1][1], Value::BigInt(800));
    assert_eq!(r.rows[1][2], Value::BigInt(1200));
}

#[test]
fn test_pivot_basic_min_max() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t (category VARCHAR(50), period VARCHAR(10), val INT)",
    );
    exec(
        &mut e,
        "INSERT INTO t VALUES ('A', 'P1', 100), ('A', 'P1', 200), ('A', 'P2', 150)",
    );
    exec(
        &mut e,
        "INSERT INTO t VALUES ('B', 'P1', 50), ('B', 'P2', 75)",
    );

    let r = query(
        &mut e,
        "SELECT category, P1, P2 FROM (SELECT category, period, val FROM t) AS s \
         PIVOT (MIN(val) FOR period IN (P1, P2)) AS p",
    );

    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("A".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(100));
    assert_eq!(r.rows[0][2], Value::Int(150));

    let r2 = query(
        &mut e,
        "SELECT category, P1, P2 FROM (SELECT category, period, val FROM t) AS s \
         PIVOT (MAX(val) FOR period IN (P1, P2)) AS p",
    );

    assert_eq!(r2.rows[0][1], Value::Int(200));
    assert_eq!(r2.rows[0][2], Value::Int(150));
}

// ─── PIVOT with ORDER BY ─────────────────────────────────────────────────

#[test]
fn test_pivot_with_order_by() {
    let mut e = Engine::new();
    setup_sales(&mut e);

    let r = query(
        &mut e,
        "SELECT * FROM (SELECT region, quarter, amount FROM sales) AS s \
         PIVOT (SUM(amount) FOR quarter IN (Q1, Q2, Q3, Q4)) AS p \
         ORDER BY region DESC",
    );

    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::VarChar("West".to_string()));
    assert_eq!(r.rows[2][0], Value::VarChar("East".to_string()));
}

// ─── PIVOT with TOP ──────────────────────────────────────────────────────

#[test]
fn test_pivot_with_top() {
    let mut e = Engine::new();
    setup_sales(&mut e);

    let r = query(
        &mut e,
        "SELECT TOP 2 * FROM (SELECT region, quarter, amount FROM sales) AS s \
         PIVOT (SUM(amount) FOR quarter IN (Q1, Q2, Q3, Q4)) AS p \
         ORDER BY region",
    );

    assert_eq!(r.rows.len(), 2);
}

// ─── PIVOT with missing pivot values (returns NULL) ─────────────────────

#[test]
fn test_pivot_missing_values_null() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (cat VARCHAR(10), val INT)");
    exec(&mut e, "INSERT INTO t VALUES ('A', 100), ('B', 200)");

    let r = query(
        &mut e,
        "SELECT * FROM (SELECT cat, val FROM t) AS s \
         PIVOT (SUM(val) FOR cat IN (A, B, C)) AS p",
    );

    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(100));
    assert_eq!(r.rows[0][1], Value::Int(200));
    assert_eq!(r.rows[0][2], Value::Null);
}

// ─── PIVOT with multiple grouping columns ───────────────────────────────

#[test]
fn test_pivot_multiple_grouping_columns() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE sales2 (region VARCHAR(50), country VARCHAR(50), product VARCHAR(50), qty INT)",
    );
    exec(
        &mut e,
        "INSERT INTO sales2 VALUES ('East', 'USA', 'Widget', 100), ('East', 'USA', 'Gadget', 200)",
    );
    exec(
        &mut e,
        "INSERT INTO sales2 VALUES ('East', 'Canada', 'Widget', 50), ('East', 'Canada', 'Gadget', 75)",
    );
    exec(
        &mut e,
        "INSERT INTO sales2 VALUES ('West', 'USA', 'Widget', 150), ('West', 'USA', 'Gadget', 250)",
    );

    let r = query(
        &mut e,
        "SELECT region, country, Widget, Gadget FROM (SELECT region, country, product, qty FROM sales2) AS s \
         PIVOT (SUM(qty) FOR product IN (Widget, Gadget)) AS p \
         ORDER BY region, country",
    );

    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("Canada".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(50));
    assert_eq!(r.rows[0][3], Value::Int(75));

    assert_eq!(r.rows[1][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("USA".to_string()));
    assert_eq!(r.rows[1][2], Value::Int(100));
    assert_eq!(r.rows[1][3], Value::Int(200));
}

// ─── PIVOT with NULLs in aggregate column ───────────────────────────────

#[test]
fn test_pivot_nulls_in_aggregate() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (cat VARCHAR(10), val INT)");
    exec(
        &mut e,
        "INSERT INTO t VALUES ('A', 100), ('A', NULL), ('A', 200), ('B', NULL)",
    );

    let r = query(
        &mut e,
        "SELECT * FROM (SELECT cat, val FROM t) AS s \
         PIVOT (SUM(val) FOR cat IN (A, B)) AS p",
    );

    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::BigInt(300));
    assert_eq!(r.rows[0][1], Value::Null);
}

// ─── PIVOT COUNT_BIG ────────────────────────────────────────────────────

#[test]
fn test_pivot_count_big() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (cat VARCHAR(10), val INT)");
    exec(
        &mut e,
        "INSERT INTO t VALUES ('A', 100), ('A', 200), ('A', NULL)",
    );

    let r = query(
        &mut e,
        "SELECT A FROM (SELECT cat, val FROM t) AS s \
         PIVOT (COUNT_BIG(val) FOR cat IN (A)) AS p",
    );

    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::BigInt(2));
}

// ─── PIVOT STRING_AGG ───────────────────────────────────────────────────

#[test]
fn test_pivot_string_agg() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE t (cat VARCHAR(10), val VARCHAR(10))");
    exec(
        &mut e,
        "INSERT INTO t VALUES ('A', 'x'), ('A', 'y'), ('A', 'z')",
    );

    let r = query(
        &mut e,
        "SELECT A FROM (SELECT cat, val FROM t) AS s \
         PIVOT (STRING_AGG(val) FOR cat IN (A)) AS p",
    );

    assert_eq!(r.rows.len(), 1);
    let aggregated = r.rows[0][0].to_string_value();
    assert!(aggregated.contains('x'));
    assert!(aggregated.contains('y'));
    assert!(aggregated.contains('z'));
}

// ─── PIVOT with subquery ────────────────────────────────────────────────

#[test]
fn test_pivot_with_subquery() {
    let mut e = Engine::new();
    setup_sales(&mut e);

    let r = query(
        &mut e,
        "SELECT * FROM (SELECT region, quarter, amount FROM sales WHERE region = 'East') AS s \
         PIVOT (SUM(amount) FOR quarter IN (Q1, Q2, Q3, Q4)) AS p",
    );

    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(1000));
    assert_eq!(r.rows[0][2], Value::Int(2000));
    assert_eq!(r.rows[0][3], Value::Int(1500));
    assert_eq!(r.rows[0][4], Value::Int(3000));
}

// ─── PIVOT: STDEV aggregate (now supported) ──────────────────────────────────

#[test]
fn test_pivot_stdev_aggregate() {
    let mut e = Engine::new();
    setup_sales(&mut e);

    let r = query(
        &mut e,
        "SELECT * FROM (SELECT region, quarter, amount FROM sales) AS s \
         PIVOT (STDEV(amount) FOR quarter IN (Q1)) AS p",
    );
    assert_eq!(r.rows.len(), 3);
    // STDEV with sample of 1 should return NULL
    assert!(r
        .rows
        .iter()
        .any(|row| row[1] == Value::Null || matches!(row[1], Value::Float(_))));
}

// ─── PIVOT with non-matching pivot column values ────────────────────────

#[test]
fn test_pivot_with_nonexistent_pivot_values() {
    let mut e = Engine::new();
    setup_sales(&mut e);

    let r = query(
        &mut e,
        "SELECT * FROM (SELECT region, quarter, amount FROM sales) AS s \
         PIVOT (SUM(amount) FOR quarter IN (Q1, Q999)) AS p",
    );

    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][1], Value::Int(1000));
    assert_eq!(r.rows[0][2], Value::Null);
}

// ─── PIVOT on empty source ──────────────────────────────────────────────

#[test]
fn test_pivot_empty_source() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE empty_t (cat VARCHAR(10), val INT)");

    let r = query(
        &mut e,
        "SELECT * FROM (SELECT cat, val FROM empty_t) AS s \
         PIVOT (SUM(val) FOR cat IN (A, B)) AS p",
    );

    assert_eq!(r.rows.len(), 0);
}

// ─── PIVOT with aliased aggregate ──────────────────────────────────────

#[test]
fn test_pivot_with_region_example() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE regions (category VARCHAR(50), region VARCHAR(50), sales INT)",
    );
    exec(&mut e, "INSERT INTO regions VALUES ('Sales', 'East', 300), ('Sales', 'West', 150), ('Marketing', 'East', 80), ('Marketing', 'West', 120)");

    let r = query(
        &mut e,
        "SELECT category, East, West FROM (SELECT category, region, sales FROM regions) AS s \
         PIVOT (SUM(sales) FOR region IN (East, West)) AS p \
         ORDER BY category",
    );

    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("Marketing".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(80));
    assert_eq!(r.rows[0][2], Value::Int(120));

    assert_eq!(r.rows[1][0], Value::VarChar("Sales".to_string()));
    assert_eq!(r.rows[1][1], Value::Int(300));
    assert_eq!(r.rows[1][2], Value::Int(150));
}

