use iridium_core::parse_sql;

fn assert_parses(sql: &str) {
    parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse: {}\n  error: {}", sql, e));
}

// ── ROW_NUMBER ──────────────────────────────────────────────────

#[test]
fn row_number_order_by() {
    assert_parses("SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t");
}

#[test]
fn row_number_partition_order() {
    assert_parses("SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM emp");
}

// ── RANK / DENSE_RANK ───────────────────────────────────────────

#[test]
fn rank_order_desc() {
    assert_parses("SELECT RANK() OVER (ORDER BY score DESC) AS rnk FROM scores");
}

#[test]
fn dense_rank_partition() {
    assert_parses("SELECT DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS dr FROM emp");
}

// ── NTILE ───────────────────────────────────────────────────────

#[test]
fn ntile_basic() {
    assert_parses("SELECT NTILE(4) OVER (ORDER BY id) AS bucket FROM items");
}

// ── LAG / LEAD ──────────────────────────────────────────────────

#[test]
fn lag_with_offset() {
    assert_parses("SELECT LAG(val, 2) OVER (ORDER BY ts) AS prev FROM data");
}

#[test]
fn lead_with_default() {
    assert_parses("SELECT LEAD(val, 1, 0) OVER (ORDER BY ts) AS nxt FROM data");
}

// ── FIRST_VALUE / LAST_VALUE ────────────────────────────────────

#[test]
fn first_value_frame() {
    assert_parses(
        "SELECT FIRST_VALUE(price) OVER (PARTITION BY product ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fv FROM prices",
    );
}

#[test]
fn last_value_full_frame() {
    assert_parses(
        "SELECT LAST_VALUE(price) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv FROM prices",
    );
}

// ── Frame clauses: ROWS ─────────────────────────────────────────

#[test]
fn rows_unbounded_preceding() {
    assert_parses(
        "SELECT SUM(amount) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS running FROM sales",
    );
}

#[test]
fn rows_between_n_preceding_and_current() {
    assert_parses(
        "SELECT AVG(val) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS mavg FROM t",
    );
}

#[test]
fn rows_between_n_preceding_and_n_following() {
    assert_parses(
        "SELECT SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving FROM t",
    );
}

#[test]
fn rows_current_row() {
    assert_parses(
        "SELECT COUNT(*) OVER (ORDER BY id ROWS CURRENT ROW) AS cnt FROM t",
    );
}

// ── Frame clauses: RANGE ────────────────────────────────────────

#[test]
fn range_unbounded_preceding() {
    assert_parses(
        "SELECT SUM(val) OVER (ORDER BY id RANGE UNBOUNDED PRECEDING) AS r FROM t",
    );
}

#[test]
fn range_between_unbounded_and_current() {
    assert_parses(
        "SELECT SUM(val) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM t",
    );
}

// ── Frame clauses: GROUPS ───────────────────────────────────────

#[test]
fn groups_between_unbounded_preceding_and_current() {
    assert_parses(
        "SELECT SUM(val) OVER (ORDER BY id GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r FROM t",
    );
}

#[test]
fn groups_between_1_preceding_and_1_following() {
    assert_parses(
        "SELECT SUM(val) OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS r FROM t",
    );
}

// ── Aggregate window functions ──────────────────────────────────

#[test]
fn count_over_partition() {
    assert_parses("SELECT COUNT(*) OVER (PARTITION BY dept) AS dept_count FROM emp");
}

#[test]
fn sum_over_order() {
    assert_parses("SELECT SUM(amount) OVER (ORDER BY id) AS cumsum FROM sales");
}

#[test]
fn avg_over_partition_order() {
    assert_parses("SELECT AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date) AS avg_sal FROM emp");
}

#[test]
fn min_max_over() {
    assert_parses("SELECT MIN(val) OVER (ORDER BY id) AS mn, MAX(val) OVER (ORDER BY id) AS mx FROM t");
}

// ── Multiple window functions in single SELECT ──────────────────

#[test]
fn multiple_window_functions() {
    assert_parses(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn, RANK() OVER (ORDER BY val DESC) AS rnk, SUM(val) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS running FROM t",
    );
}

// ── ORDER BY with explicit ASC ──────────────────────────────────

#[test]
fn window_order_by_asc() {
    assert_parses("SELECT ROW_NUMBER() OVER (ORDER BY id ASC) AS rn FROM t");
}

#[test]
fn window_order_by_multiple_directions() {
    assert_parses(
        "SELECT ROW_NUMBER() OVER (ORDER BY dept ASC, salary DESC) AS rn FROM emp",
    );
}

