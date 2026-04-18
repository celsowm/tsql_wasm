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

fn setup_pivot_source(engine: &mut Engine) {
    exec(
        engine,
        "CREATE TABLE pivot_source (region VARCHAR(50), Q1 INT, Q2 INT, Q3 INT, Q4 INT)",
    );
    exec(
        engine,
        "INSERT INTO pivot_source VALUES ('East', 1000, 2000, 1500, 3000)",
    );
    exec(
        engine,
        "INSERT INTO pivot_source VALUES ('West', 500, 1500, 1000, 2000)",
    );
    exec(
        engine,
        "INSERT INTO pivot_source VALUES ('North', 800, 1200, 900, 1100)",
    );
}

// ─── Basic UNPIVOT ──────────────────────────────────────────────────────

#[test]
fn test_unpivot_basic() {
    let mut e = Engine::new();
    setup_pivot_source(&mut e);

    let r = query(
        &mut e,
        "SELECT region, quarter, amount FROM pivot_source \
         UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3, Q4)) AS u \
         ORDER BY region, quarter",
    );

    assert_eq!(r.columns.len(), 3);
    assert_eq!(r.rows.len(), 12);

    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("Q1".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(1000));

    assert_eq!(r.rows[1][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("Q2".to_string()));
    assert_eq!(r.rows[1][2], Value::Int(2000));

    assert_eq!(r.rows[4][0], Value::VarChar("North".to_string()));
    assert_eq!(r.rows[4][1], Value::VarChar("Q1".to_string()));
    assert_eq!(r.rows[4][2], Value::Int(800));

    assert_eq!(r.rows[11][0], Value::VarChar("West".to_string()));
    assert_eq!(r.rows[11][1], Value::VarChar("Q4".to_string()));
    assert_eq!(r.rows[11][2], Value::Int(2000));
}

// ─── UNPIVOT with ORDER BY ──────────────────────────────────────────────

#[test]
fn test_unpivot_with_order_by() {
    let mut e = Engine::new();
    setup_pivot_source(&mut e);

    let r = query(
        &mut e,
        "SELECT region, quarter, amount FROM pivot_source \
         UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3, Q4)) AS u \
         ORDER BY amount DESC, region",
    );

    assert_eq!(r.rows.len(), 12);
    assert_eq!(r.rows[0][2], Value::Int(3000));
    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
}

// ─── UNPIVOT with subset of columns ────────────────────────────────────

#[test]
fn test_unpivot_subset_columns() {
    let mut e = Engine::new();
    setup_pivot_source(&mut e);

    let r = query(
        &mut e,
        "SELECT region, quarter, amount FROM pivot_source \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u \
         ORDER BY region, quarter",
    );

    assert_eq!(r.rows.len(), 6);

    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("Q1".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(1000));

    assert_eq!(r.rows[1][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("Q2".to_string()));
    assert_eq!(r.rows[1][2], Value::Int(2000));
}

// ─── UNPIVOT with NULL values (should be skipped) ───────────────────────

#[test]
fn test_unpivot_skips_nulls() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t (region VARCHAR(50), Q1 INT, Q2 INT, Q3 INT)",
    );
    exec(&mut e, "INSERT INTO t VALUES ('East', 1000, NULL, 1500)");
    exec(&mut e, "INSERT INTO t VALUES ('West', NULL, NULL, 2000)");

    let r = query(
        &mut e,
        "SELECT region, quarter, amount FROM t \
         UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3)) AS u \
         ORDER BY region, quarter",
    );

    assert_eq!(r.rows.len(), 3);

    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("Q1".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(1000));

    assert_eq!(r.rows[1][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[1][1], Value::VarChar("Q3".to_string()));
    assert_eq!(r.rows[1][2], Value::Int(1500));

    assert_eq!(r.rows[2][0], Value::VarChar("West".to_string()));
    assert_eq!(r.rows[2][1], Value::VarChar("Q3".to_string()));
    assert_eq!(r.rows[2][2], Value::Int(2000));
}

// ─── UNPIVOT with all NULLs for a row ───────────────────────────────────

#[test]
fn test_unpivot_all_nulls_row() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t (region VARCHAR(50), Q1 INT, Q2 INT)",
    );
    exec(&mut e, "INSERT INTO t VALUES ('Empty', NULL, NULL)");

    let r = query(
        &mut e,
        "SELECT region, quarter, amount FROM t \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u",
    );

    assert_eq!(r.rows.len(), 0);
}

// ─── UNPIVOT on empty source ────────────────────────────────────────────

#[test]
fn test_unpivot_empty_source() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE empty_t (region VARCHAR(50), Q1 INT, Q2 INT)",
    );

    let r = query(
        &mut e,
        "SELECT region, quarter, amount FROM empty_t \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u",
    );

    assert_eq!(r.rows.len(), 0);
}

// ─── UNPIVOT error: non-existent column ────────────────────────────

#[test]
fn test_unpivot_error_nonexistent_column() {
    let mut e = Engine::new();
    setup_pivot_source(&mut e);

    let stmt = parse_sql(
        "SELECT region, quarter, amount FROM pivot_source \
         UNPIVOT (amount FOR quarter IN (Q1, nonexistent)) AS u",
    );
    let result = e.execute(stmt.expect("parse failed"));
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("nonexistent") || err.to_string().contains("not found"));
}

// ─── UNPIVOT with TOP ──────────────────────────────────────────────────

#[test]
fn test_unpivot_with_top() {
    let mut e = Engine::new();
    setup_pivot_source(&mut e);

    let r = query(
        &mut e,
        "SELECT TOP 3 region, quarter, amount FROM pivot_source \
         UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3, Q4)) AS u \
         ORDER BY region, quarter",
    );

    assert_eq!(r.rows.len(), 3);
}

// ─── UNPIVOT preserves original columns ────────────────────────────────

#[test]
fn test_unpivot_preserves_columns() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE t (id INT PRIMARY KEY, A INT, B INT, C INT)",
    );
    exec(&mut e, "INSERT INTO t VALUES (1, 100, 200, 300)");
    exec(&mut e, "INSERT INTO t VALUES (2, 150, 250, 350)");

    let r = query(
        &mut e,
        "SELECT id, col, val FROM t \
         UNPIVOT (val FOR col IN (A, B, C)) AS u \
         ORDER BY id, col",
    );

    assert_eq!(r.rows.len(), 6);

    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("A".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(100));

    assert_eq!(r.rows[1][0], Value::Int(1));
    assert_eq!(r.rows[1][1], Value::VarChar("B".to_string()));
    assert_eq!(r.rows[1][2], Value::Int(200));

    assert_eq!(r.rows[2][0], Value::Int(1));
    assert_eq!(r.rows[2][1], Value::VarChar("C".to_string()));
    assert_eq!(r.rows[2][2], Value::Int(300));

    assert_eq!(r.rows[3][0], Value::Int(2));
    assert_eq!(r.rows[3][1], Value::VarChar("A".to_string()));
    assert_eq!(r.rows[3][2], Value::Int(150));
}

// ─── Round-trip PIVOT then UNPIVOT ─────────────────────────────────────

#[test]
fn test_pivot_unpivot_roundtrip() {
    let mut e = Engine::new();

    exec(
        &mut e,
        "CREATE TABLE original (region VARCHAR(50), quarter VARCHAR(10), amount INT)",
    );
    exec(
        &mut e,
        "INSERT INTO original VALUES ('East', 'Q1', 1000), ('East', 'Q2', 2000), ('West', 'Q1', 500), ('West', 'Q2', 1500)",
    );

    let pivoted = query(
        &mut e,
        "SELECT * FROM (SELECT region, quarter, amount FROM original) AS s \
         PIVOT (SUM(amount) FOR quarter IN (Q1, Q2)) AS p",
    );

    assert_eq!(pivoted.rows.len(), 2);

    exec(
        &mut e,
        "CREATE TABLE pivoted_result (region VARCHAR(50), Q1 INT, Q2 INT)",
    );
    for row in &pivoted.rows {
        let region = match &row[0] {
            Value::VarChar(s) => format!("'{}'", s),
            _ => panic!("expected VarChar"),
        };
        let q1 = match &row[1] {
            Value::Int(i) => i.to_string(),
            Value::Null => "NULL".to_string(),
            _ => panic!("expected Int or Null"),
        };
        let q2 = match &row[2] {
            Value::Int(i) => i.to_string(),
            Value::Null => "NULL".to_string(),
            _ => panic!("expected Int or Null"),
        };
        exec(
            &mut e,
            &format!(
                "INSERT INTO pivoted_result VALUES ({}, {}, {})",
                region, q1, q2
            ),
        );
    }

    let unpivoted = query(
        &mut e,
        "SELECT region, quarter, amount FROM pivoted_result \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u \
         ORDER BY region, quarter",
    );

    assert_eq!(unpivoted.rows.len(), 4);
}

// ─── UNPIVOT with join ──────────────────────────────────────────────────

#[test]
fn test_unpivot_with_cross_join() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE src_data (name VARCHAR(50), Q1 INT, Q2 INT)",
    );
    exec(&mut e, "INSERT INTO src_data VALUES ('Alice', 100, 200)");
    exec(&mut e, "INSERT INTO src_data VALUES ('Bob', 150, 250)");

    exec(&mut e, "CREATE TABLE mult (factor INT)");
    exec(&mut e, "INSERT INTO mult VALUES (10)");

    let r = query(
        &mut e,
        "SELECT name, quarter, amount, factor FROM src_data \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u \
         CROSS JOIN mult \
         ORDER BY name, quarter",
    );

    assert_eq!(r.rows.len(), 4);
    assert_eq!(r.rows[0][0], Value::VarChar("Alice".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(100));
    assert_eq!(r.rows[1][2], Value::Int(200));
}

// ─── UNPIVOT with source alias (qualified column reference) ────────────

#[test]
fn test_unpivot_source_alias_qualified() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE src (name VARCHAR(50), Q1 INT, Q2 INT, Q3 INT)",
    );
    exec(&mut e, "INSERT INTO src VALUES ('East', 1000, 2000, 1500)");

    let r = query(
        &mut e,
        "SELECT d.name, u.quarter, u.amount FROM src AS d \
         UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3)) AS u \
         ORDER BY u.quarter",
    );

    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("Q1".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(1000));
    assert_eq!(r.rows[1][1], Value::VarChar("Q2".to_string()));
    assert_eq!(r.rows[1][2], Value::Int(2000));
    assert_eq!(r.rows[2][1], Value::VarChar("Q3".to_string()));
    assert_eq!(r.rows[2][2], Value::Int(1500));
}

#[test]
fn test_unpivot_source_alias_unpivoted_alias() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE src (region VARCHAR(50), Q1 INT, Q2 INT)",
    );
    exec(&mut e, "INSERT INTO src VALUES ('North', 800, 1200)");

    let r = query(
        &mut e,
        "SELECT u.region, u.quarter, u.amount FROM src AS d \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u \
         ORDER BY u.quarter",
    );

    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("North".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("Q1".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(800));
}

#[test]
fn test_unpivot_source_alias_wildcard() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE src (region VARCHAR(50), Q1 INT, Q2 INT)",
    );
    exec(&mut e, "INSERT INTO src VALUES ('North', 800, 1200)");

    let r = query(
        &mut e,
        "SELECT d.* FROM src AS d \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u",
    );

    assert_eq!(r.columns.len(), 3);
    assert_eq!(r.columns[0], "region");
    assert_eq!(r.columns[1], "quarter");
    assert_eq!(r.columns[2], "amount");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("North".to_string()));
}

#[test]
fn test_unpivot_unpivoted_alias_wildcard() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE src (region VARCHAR(50), Q1 INT, Q2 INT)",
    );
    exec(&mut e, "INSERT INTO src VALUES ('North', 800, 1200)");

    let r = query(
        &mut e,
        "SELECT u.* FROM src AS d \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u",
    );

    assert_eq!(r.columns.len(), 3);
    assert_eq!(r.columns[0], "region");
    assert_eq!(r.columns[1], "quarter");
    assert_eq!(r.columns[2], "amount");
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("North".to_string()));
}

#[test]
fn test_unpivot_no_source_alias() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE pivot_source (region VARCHAR(50), Q1 INT, Q2 INT)",
    );
    exec(
        &mut e,
        "INSERT INTO pivot_source VALUES ('East', 1000, 2000)",
    );

    let r = query(
        &mut e,
        "SELECT pivot_source.region, u.quarter FROM pivot_source \
         UNPIVOT (amount FOR quarter IN (Q1, Q2)) AS u \
         ORDER BY u.quarter",
    );

    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0], Value::VarChar("East".to_string()));
    assert_eq!(r.rows[0][1], Value::VarChar("Q1".to_string()));
}
