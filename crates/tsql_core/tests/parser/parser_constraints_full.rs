use tsql_core::parse_sql;

fn assert_parses(sql: &str) {
    parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse: {}\n  error: {}", sql, e));
}

// ── Column-level constraints ────────────────────────────────────

#[test]
fn column_primary_key() {
    assert_parses("CREATE TABLE t (id INT PRIMARY KEY)");
}

#[test]
fn column_not_null() {
    assert_parses("CREATE TABLE t (id INT NOT NULL)");
}

#[test]
fn column_null() {
    assert_parses("CREATE TABLE t (name VARCHAR(50) NULL)");
}

#[test]
fn column_unique() {
    assert_parses("CREATE TABLE t (email VARCHAR(100) UNIQUE)");
}

#[test]
fn column_default_literal() {
    assert_parses("CREATE TABLE t (status INT DEFAULT 0)");
}

#[test]
fn column_default_string() {
    assert_parses("CREATE TABLE t (name VARCHAR(50) DEFAULT 'unknown')");
}

#[test]
fn column_check() {
    assert_parses("CREATE TABLE t (age INT CHECK (age > 0))");
}

#[test]
fn column_identity() {
    assert_parses("CREATE TABLE t (id INT IDENTITY(1,1))");
}

#[test]
fn column_references() {
    assert_parses("CREATE TABLE orders (customer_id INT REFERENCES customers(id))");
}

#[test]
fn column_combined_constraints() {
    assert_parses("CREATE TABLE t (id INT NOT NULL PRIMARY KEY IDENTITY(1,1))");
}

#[test]
fn column_named_default_constraint() {
    assert_parses("CREATE TABLE t (val INT CONSTRAINT DF_val DEFAULT 42)");
}

#[test]
fn column_named_check_constraint() {
    assert_parses("CREATE TABLE t (val INT CONSTRAINT CK_val CHECK (val > 0))");
}

// ── Table-level named constraints (CONSTRAINT keyword) ──────────

#[test]
fn table_constraint_primary_key() {
    assert_parses(
        "CREATE TABLE t (id INT, name VARCHAR(50), CONSTRAINT PK_t PRIMARY KEY (id))",
    );
}

#[test]
fn table_constraint_composite_primary_key() {
    assert_parses(
        "CREATE TABLE t (a INT, b INT, CONSTRAINT PK_t PRIMARY KEY (a, b))",
    );
}

#[test]
fn table_constraint_unique() {
    assert_parses(
        "CREATE TABLE t (id INT, email VARCHAR(100), CONSTRAINT UQ_email UNIQUE (email))",
    );
}

#[test]
fn table_constraint_composite_unique() {
    assert_parses(
        "CREATE TABLE t (a INT, b INT, CONSTRAINT UQ_ab UNIQUE (a, b))",
    );
}

#[test]
fn table_constraint_check() {
    assert_parses(
        "CREATE TABLE t (a INT, b INT, CONSTRAINT CK_t CHECK (a < b))",
    );
}

#[test]
fn table_constraint_default_for() {
    assert_parses(
        "CREATE TABLE t (id INT, val INT, CONSTRAINT DF_val DEFAULT 9 FOR val)",
    );
}

#[test]
fn table_constraint_foreign_key() {
    assert_parses(
        "CREATE TABLE orders (id INT, cust_id INT, CONSTRAINT FK_cust FOREIGN KEY (cust_id) REFERENCES customers(id))",
    );
}

// ── Table-level unnamed constraints ─────────────────────────────

#[test]
fn unnamed_primary_key() {
    assert_parses("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a))");
}

#[test]
fn unnamed_composite_primary_key() {
    assert_parses("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))");
}

#[test]
fn unnamed_unique() {
    assert_parses("CREATE TABLE t (a INT, email VARCHAR(100), UNIQUE (email))");
}

// ── Multiple constraints in one table ───────────────────────────

#[test]
fn multiple_table_constraints() {
    assert_parses(
        "CREATE TABLE orders (
            id INT NOT NULL IDENTITY(1,1),
            customer_id INT NOT NULL,
            amount INT DEFAULT 0,
            CONSTRAINT PK_orders PRIMARY KEY (id),
            CONSTRAINT FK_customer FOREIGN KEY (customer_id) REFERENCES customers(id),
            CONSTRAINT CK_amount CHECK (amount >= 0)
        )",
    );
}

#[test]
fn mixed_column_and_table_constraints() {
    assert_parses(
        "CREATE TABLE employees (
            id INT PRIMARY KEY IDENTITY(1,1),
            email VARCHAR(200) NOT NULL UNIQUE,
            dept_id INT REFERENCES departments(id),
            salary INT CHECK (salary > 0),
            CONSTRAINT UQ_email UNIQUE (email)
        )",
    );
}

// ── Computed columns ────────────────────────────────────────────

#[test]
fn computed_column() {
    assert_parses("CREATE TABLE t (a INT, b INT, total AS (a + b))");
}
