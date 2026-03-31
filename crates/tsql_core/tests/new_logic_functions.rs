include!("new_functions/helpers.rs");

// ─── IIF ──────────────────────────────────────────────────────────────────

#[test]
fn test_iif_true() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT IIF(1 = 1, 'yes', 'no') AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("yes".to_string()));
}

#[test]
fn test_iif_false() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT IIF(1 = 2, 'yes', 'no') AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("no".to_string()));
}

#[test]
fn test_iif_with_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT IIF(NULL, 'yes', 'no') AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("no".to_string()));
}

#[test]
fn test_iif_nested() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT IIF(1 = 1, IIF(2 = 2, 'both', 'first'), 'none') AS v",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("both".to_string()));
}

#[test]
fn test_iif_with_column() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (val INT)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (10)");
    exec(&mut engine, "INSERT INTO dbo.t (val) VALUES (-5)");
    let r = query(
        &mut engine,
        "SELECT IIF(val >= 0, 'positive', 'negative') AS sign FROM dbo.t ORDER BY val DESC",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("positive".to_string()));
    assert_eq!(r.rows[1][0], Value::VarChar("negative".to_string()));
}

#[test]
fn test_iif_with_concat() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CONCAT(IIF(1 = 1, 'yes', 'no'), ' - ', 'result') AS v",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("yes - result".to_string()));
}

// ─── NULLIF ───────────────────────────────────────────────────────────────

#[test]
fn test_nullif_equal() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT NULLIF(42, 42) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_nullif_not_equal() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT NULLIF(42, 0) AS v");
    assert_eq!(r.rows[0][0], Value::Int(42));
}

#[test]
fn test_nullif_strings() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT NULLIF('hello', 'hello') AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_nullif_with_divide_by_zero() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (numerator INT, denominator INT)");
    exec(&mut engine, "INSERT INTO dbo.t (numerator, denominator) VALUES (10, 2)");
    exec(&mut engine, "INSERT INTO dbo.t (numerator, denominator) VALUES (5, 0)");
    let r = query(
        &mut engine,
        "SELECT numerator / NULLIF(denominator, 0) AS result FROM dbo.t ORDER BY numerator DESC",
    );
    assert_eq!(r.rows[0][0].to_integer_i64().unwrap(), 5);
    assert!(r.rows[1][0].is_null());
}

#[test]
fn test_nullif_prevents_division() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.orders (amount INT, qty INT)");
    exec(&mut engine, "INSERT INTO dbo.orders (amount, qty) VALUES (100, 5)");
    exec(&mut engine, "INSERT INTO dbo.orders (amount, qty) VALUES (50, 0)");
    let r = query(
        &mut engine,
        "SELECT ISNULL(amount / NULLIF(qty, 0), 0) AS unit_price FROM dbo.orders ORDER BY amount",
    );
    assert_eq!(r.rows[0][0].to_integer_i64().unwrap(), 0);
    assert_eq!(r.rows[1][0].to_integer_i64().unwrap(), 20);
}

// ─── CHOOSE ───────────────────────────────────────────────────────────────

#[test]
fn test_choose_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CHOOSE(2, 'a', 'b', 'c') AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("b".to_string()));
}

#[test]
fn test_choose_first() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CHOOSE(1, 'first', 'second') AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("first".to_string()));
}

#[test]
fn test_choose_out_of_range() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CHOOSE(0, 'a', 'b') AS v");
    assert!(r.rows[0][0].is_null());

    let r = query(&mut engine, "SELECT CHOOSE(5, 'a', 'b') AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_choose_with_column() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (day_num INT)");
    exec(&mut engine, "INSERT INTO dbo.t (day_num) VALUES (1)");
    exec(&mut engine, "INSERT INTO dbo.t (day_num) VALUES (3)");
    let r = query(
        &mut engine,
        "SELECT CHOOSE(day_num, 'Mon', 'Tue', 'Wed', 'Thu', 'Fri') AS day_name FROM dbo.t ORDER BY day_num",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("Mon".to_string()));
    assert_eq!(r.rows[1][0], Value::VarChar("Wed".to_string()));
}

#[test]
fn test_choose_with_datepart() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CHOOSE(3, 'Q1', 'Q2', 'Q3', 'Q4') AS quarter",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("Q3".to_string()));
}
