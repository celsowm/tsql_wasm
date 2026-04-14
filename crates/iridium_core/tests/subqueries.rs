use iridium_core::types::Value;
use iridium_core::{parse_batch, parse_sql, Engine, QueryResult};

fn setup_engine() -> Engine {
    let engine = Engine::new();

    let batch = parse_batch(
        "CREATE TABLE departments (id INT PRIMARY KEY, name NVARCHAR(100) NOT NULL);
         CREATE TABLE employees (
             id INT PRIMARY KEY,
             name NVARCHAR(100) NOT NULL,
             department_id INT NOT NULL,
             salary INT NOT NULL
         );
         INSERT INTO departments VALUES (1, 'Engineering');
         INSERT INTO departments VALUES (2, 'Sales');
         INSERT INTO departments VALUES (3, 'Marketing');
         INSERT INTO departments VALUES (4, 'HR');
         INSERT INTO employees VALUES (1, 'Alice', 1, 100000);
         INSERT INTO employees VALUES (2, 'Bob', 1, 90000);
         INSERT INTO employees VALUES (3, 'Charlie', 2, 80000);
         INSERT INTO employees VALUES (4, 'Diana', 2, 85000);
         INSERT INTO employees VALUES (5, 'Eve', 3, 70000);",
    )
    .unwrap();

    for stmt in batch {
        engine.execute(stmt).unwrap();
    }

    engine
}

fn query(engine: &mut Engine, sql: &str) -> QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

fn col_str(row: &[Value], idx: usize) -> String {
    match &row[idx] {
        Value::VarChar(s) => s.clone(),
        Value::NVarChar(s) => s.clone(),
        Value::Char(s) => s.clone(),
        Value::NChar(s) => s.clone(),
        _ => panic!("expected string at index {}, got {:?}", idx, &row[idx]),
    }
}

fn col_num(row: &[Value], idx: usize) -> i64 {
    match &row[idx] {
        Value::TinyInt(n) => *n as i64,
        Value::SmallInt(n) => *n as i64,
        Value::Int(n) => *n as i64,
        Value::BigInt(n) => *n,
        Value::Decimal(raw, scale) => {
            let divisor = 10i128.pow(*scale as u32);
            (*raw / divisor) as i64
        }
        _ => panic!("expected number at index {}, got {:?}", idx, &row[idx]),
    }
}

// ─── Scalar Subqueries ────────────────────────────────────────────────────

#[test]
fn test_scalar_subquery_select() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT (SELECT MAX(salary) FROM employees) AS max_salary",
    );

    assert_eq!(result.columns, vec!["max_salary"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_num(&result.rows[0], 0), 100000);
}

#[test]
fn test_scalar_subquery_in_where() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name, salary FROM employees WHERE salary > (SELECT MIN(salary) FROM employees WHERE salary > 80000) ORDER BY salary DESC",
    );

    assert_eq!(result.rows.len(), 2);
    assert_eq!(col_str(&result.rows[0], 0), "Alice");
    assert_eq!(col_str(&result.rows[1], 0), "Bob");
}

#[test]
fn test_scalar_subquery_returns_null_when_empty() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM employees WHERE salary = (SELECT salary FROM employees WHERE id = 999)",
    );
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn test_scalar_subquery_with_min() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM employees WHERE salary = (SELECT MIN(salary) FROM employees)",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_str(&result.rows[0], 0), "Eve");
}

#[test]
fn test_scalar_subquery_with_count() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM departments WHERE id = (SELECT COUNT(*) FROM employees)",
    );
    assert_eq!(result.rows.len(), 0);
}

// ─── EXISTS Subqueries ────────────────────────────────────────────────────

#[test]
fn test_exists_correlated() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department_id = d.id) ORDER BY name",
    );
    assert_eq!(result.rows.len(), 3);
    assert_eq!(col_str(&result.rows[0], 0), "Engineering");
    assert_eq!(col_str(&result.rows[1], 0), "Marketing");
    assert_eq!(col_str(&result.rows[2], 0), "Sales");
}

#[test]
fn test_not_exists_correlated() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.department_id = d.id)",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_str(&result.rows[0], 0), "HR");
}

#[test]
fn test_exists_non_correlated() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM departments WHERE EXISTS (SELECT 1 FROM employees WHERE salary > 90000) ORDER BY name",
    );
    assert_eq!(result.rows.len(), 4);
}

#[test]
fn test_exists_empty_result() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM departments WHERE EXISTS (SELECT 1 FROM employees WHERE salary > 200000)",
    );
    assert_eq!(result.rows.len(), 0);
}

// ─── IN Subquery ──────────────────────────────────────────────────────────

#[test]
fn test_in_subquery() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM employees WHERE department_id IN (SELECT id FROM departments WHERE name = 'Engineering') ORDER BY name",
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(col_str(&result.rows[0], 0), "Alice");
    assert_eq!(col_str(&result.rows[1], 0), "Bob");
}

#[test]
fn test_in_subquery_multiple_values() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM employees WHERE department_id IN (SELECT id FROM departments WHERE name IN ('Engineering', 'Sales')) ORDER BY name",
    );
    assert_eq!(result.rows.len(), 4);
    assert_eq!(col_str(&result.rows[0], 0), "Alice");
    assert_eq!(col_str(&result.rows[1], 0), "Bob");
    assert_eq!(col_str(&result.rows[2], 0), "Charlie");
    assert_eq!(col_str(&result.rows[3], 0), "Diana");
}

#[test]
fn test_not_in_subquery() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM employees WHERE department_id NOT IN (SELECT id FROM departments WHERE name IN ('Engineering', 'Sales')) ORDER BY name",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_str(&result.rows[0], 0), "Eve");
}

#[test]
fn test_in_subquery_empty_result() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM employees WHERE department_id IN (SELECT id FROM departments WHERE name = 'NonExistent')",
    );
    assert_eq!(result.rows.len(), 0);
}

// ─── Correlated Subqueries ────────────────────────────────────────────────

#[test]
fn test_correlated_scalar_subquery() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department_id = d.id AND e.salary > 85000) ORDER BY d.name",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_str(&result.rows[0], 0), "Engineering");
}

#[test]
fn test_correlated_exists() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department_id = d.id AND e.salary > 85000) ORDER BY d.name",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_str(&result.rows[0], 0), "Engineering");
}

#[test]
fn test_correlated_in() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT e.name FROM employees e WHERE e.department_id IN (SELECT d.id FROM departments d WHERE d.id = e.department_id) ORDER BY e.name",
    );
    assert_eq!(result.rows.len(), 5);
}

#[test]
fn test_join_group_in_parentheses() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT e.name AS employee_name, d.name AS department_name FROM (employees e JOIN departments d ON e.department_id = d.id) ORDER BY e.name",
    );

    assert_eq!(result.rows.len(), 5);
    assert_eq!(result.columns, vec!["employee_name", "department_name"]);
    assert_eq!(col_str(&result.rows[0], 0), "Alice");
    assert_eq!(col_str(&result.rows[0], 1), "Engineering");
}

#[test]
fn test_join_group_with_alias() {
    let mut engine = Engine::new();
    engine
        .exec("CREATE TABLE left_t (lid INT, lval NVARCHAR(10))")
        .unwrap();
    engine
        .exec("CREATE TABLE right_t (rid INT, rval NVARCHAR(10))")
        .unwrap();
    engine.exec("INSERT INTO left_t VALUES (1, 'A')").unwrap();
    engine.exec("INSERT INTO right_t VALUES (1, 'B')").unwrap();
    let result = query(
        &mut engine,
        "SELECT g.lid, g.lval, g.rid, g.rval FROM (left_t l JOIN right_t r ON l.lid = r.rid) g",
    );

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns, vec!["lid", "lval", "rid", "rval"]);
    assert_eq!(col_num(&result.rows[0], 0), 1);
    assert_eq!(col_str(&result.rows[0], 1), "A");
    assert_eq!(col_num(&result.rows[0], 2), 1);
    assert_eq!(col_str(&result.rows[0], 3), "B");
}

#[test]
fn test_derived_table_set_op_subquery() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT x.v FROM (SELECT 1 AS v UNION ALL SELECT 2 AS v) x ORDER BY x.v",
    );

    assert_eq!(result.columns, vec!["v"]);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(col_num(&result.rows[0], 0), 1);
    assert_eq!(col_num(&result.rows[1], 0), 2);
}

// ─── Subqueries with comparison operators ─────────────────────────────────

#[test]
fn test_subquery_with_greater_than() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM employees WHERE salary > (SELECT salary FROM employees WHERE name = 'Bob') ORDER BY name",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_str(&result.rows[0], 0), "Alice");
}

#[test]
fn test_subquery_with_less_than() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM employees WHERE salary < (SELECT salary FROM employees WHERE name = 'Diana') ORDER BY name",
    );
    assert_eq!(result.rows.len(), 2);
    assert_eq!(col_str(&result.rows[0], 0), "Charlie");
    assert_eq!(col_str(&result.rows[1], 0), "Eve");
}

#[test]
fn test_subquery_with_equals() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM departments WHERE id = (SELECT department_id FROM employees WHERE name = 'Eve')",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_str(&result.rows[0], 0), "Marketing");
}

// ─── Complex subqueries ───────────────────────────────────────────────────

#[test]
fn test_subquery_in_projection_with_correlated() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT d.name, (SELECT COUNT(*) FROM employees e WHERE e.department_id = d.id) AS emp_count FROM departments d ORDER BY d.name",
    );
    assert_eq!(result.rows.len(), 4);
    assert_eq!(col_str(&result.rows[0], 0), "Engineering");
    assert_eq!(col_num(&result.rows[0], 1), 2);
    assert_eq!(col_str(&result.rows[1], 0), "HR");
    assert_eq!(col_num(&result.rows[1], 1), 0);
    assert_eq!(col_str(&result.rows[2], 0), "Marketing");
    assert_eq!(col_num(&result.rows[2], 1), 1);
    assert_eq!(col_str(&result.rows[3], 0), "Sales");
    assert_eq!(col_num(&result.rows[3], 1), 2);
}

#[test]
fn test_subquery_with_join() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT d.name, e.name, e.salary FROM departments d INNER JOIN employees e ON e.department_id = d.id WHERE e.salary = (SELECT MAX(e2.salary) FROM employees e2 WHERE e2.department_id = d.id) ORDER BY d.name",
    );
    assert_eq!(result.rows.len(), 3);
    assert_eq!(col_str(&result.rows[0], 0), "Engineering");
    assert_eq!(col_str(&result.rows[0], 1), "Alice");
    assert_eq!(col_str(&result.rows[1], 0), "Marketing");
    assert_eq!(col_str(&result.rows[1], 1), "Eve");
    assert_eq!(col_str(&result.rows[2], 0), "Sales");
    assert_eq!(col_str(&result.rows[2], 1), "Diana");
}

#[test]
fn test_subquery_in_select_without_from() {
    let mut engine = setup_engine();
    let result = query(&mut engine, "SELECT (SELECT COUNT(*) FROM employees)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(col_num(&result.rows[0], 0), 5);
}

#[test]
fn test_multiple_subqueries_in_where() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name, salary FROM employees WHERE salary > (SELECT MIN(salary) FROM employees) AND salary < (SELECT MAX(salary) FROM employees) ORDER BY salary DESC",
    );
    assert_eq!(result.rows.len(), 3);
    assert_eq!(col_str(&result.rows[0], 0), "Bob");
    assert_eq!(col_str(&result.rows[1], 0), "Diana");
    assert_eq!(col_str(&result.rows[2], 0), "Charlie");
}

#[test]
fn test_not_exists_non_correlated_false() {
    let mut engine = setup_engine();
    let result = query(
        &mut engine,
        "SELECT name FROM departments WHERE NOT EXISTS (SELECT 1 FROM employees WHERE salary > 90000) ORDER BY name",
    );
    assert_eq!(result.rows.len(), 0);
}

#[test]
fn test_avg_is_numeric() {
    let mut engine = setup_engine();
    let result = query(&mut engine, "SELECT AVG(salary) as avg_sal FROM employees");
    assert_eq!(result.rows.len(), 1);

    match &result.rows[0][0] {
        Value::Int(val) => {
            assert_eq!(*val, 85000);
        }
        _ => panic!("Expected int, got {:?}", result.rows[0][0]),
    }
}

#[test]
fn test_nested_correlated_subqueries() {
    let mut engine = setup_engine();
    // Level 0: departments d
    // Level 1: employees e (correlated with d)
    // Level 2: employees e2 (correlated with d)
    let result = query(
        &mut engine,
        "SELECT d.name FROM departments d
         WHERE EXISTS (
             SELECT 1 FROM employees e
             WHERE e.department_id = d.id
             AND e.salary > (
                 SELECT MIN(e2.salary) FROM employees e2
                 WHERE e2.department_id = d.id
             )
         )
         ORDER BY d.name",
    );
    // Engineering has Alice (100k) and Bob (90k). Min is 90k. Alice > 90k. So Engineering matches.
    // Sales has Charlie (80k) and Diana (85k). Min is 80k. Diana > 80k. So Sales matches.
    // Marketing has only Eve (70k). Min is 70k. Nothing > 70k. So Marketing does NOT match.
    // HR has no employees. So HR does NOT match.
    assert_eq!(result.rows.len(), 2);
    assert_eq!(col_str(&result.rows[0], 0), "Engineering");
    assert_eq!(col_str(&result.rows[1], 0), "Sales");
}

