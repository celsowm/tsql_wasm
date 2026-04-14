use iridium_core::{types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect(sql);
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    engine.query(sql).expect(sql)
}

#[test]
fn migration_alter_table_add_column() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.users (id INT PRIMARY KEY, name NVARCHAR(100))",
    );
    exec(&mut e, "INSERT INTO dbo.users VALUES (1, N'Alice')");

    exec(&mut e, "ALTER TABLE dbo.users ADD email NVARCHAR(200) NULL");

    exec(
        &mut e,
        "INSERT INTO dbo.users (id, name, email) VALUES (2, N'Bob', N'bob@test.com')",
    );

    let r = query(&mut e, "SELECT id, name, email FROM dbo.users ORDER BY id");
    assert_eq!(r.rows.len(), 2);
    assert!(r.rows[0][2].is_null());
    assert_eq!(r.rows[1][2].to_string_value(), "bob@test.com");
}

#[test]
fn migration_alter_table_drop_column() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.items (id INT PRIMARY KEY, name NVARCHAR(50), extra INT)",
    );
    exec(&mut e, "INSERT INTO dbo.items VALUES (1, N'Widget', 42)");

    exec(&mut e, "ALTER TABLE dbo.items DROP COLUMN extra");

    let r = query(
        &mut e,
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'items' ORDER BY ORDINAL_POSITION",
    );
    let col_names: Vec<String> = r.rows.iter().map(|row| row[0].to_string_value()).collect();
    assert_eq!(col_names, vec!["id", "name"]);
}

#[test]
fn migration_create_index_on_existing_table() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.orders (id INT PRIMARY KEY, customer_id INT, order_date DATE, total DECIMAL(10,2))",
    );
    exec(
        &mut e,
        "INSERT INTO dbo.orders VALUES (1, 100, '2025-01-15', 50.00)",
    );
    exec(
        &mut e,
        "INSERT INTO dbo.orders VALUES (2, 100, '2025-02-20', 75.00)",
    );
    exec(
        &mut e,
        "INSERT INTO dbo.orders VALUES (3, 200, '2025-01-10', 30.00)",
    );

    exec(
        &mut e,
        "CREATE INDEX ix_orders_customer ON dbo.orders (customer_id)",
    );

    let r = query(
        &mut e,
        "SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.orders') AND name = 'ix_orders_customer'",
    );
    assert_eq!(r.rows.len(), 1);
}

#[test]
fn migration_add_unique_constraint() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.users (id INT PRIMARY KEY, email NVARCHAR(200))",
    );
    exec(
        &mut e,
        "INSERT INTO dbo.users VALUES (1, N'alice@test.com')",
    );

    exec(
        &mut e,
        "ALTER TABLE dbo.users ADD CONSTRAINT UQ_users_email UNIQUE (email)",
    );

    let err = e.exec("INSERT INTO dbo.users VALUES (2, N'alice@test.com')");
    assert!(
        err.is_err(),
        "UNIQUE constraint should reject duplicate email"
    );
}

#[test]
fn migration_drop_check_constraint() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.t (id INT, val INT)");
    exec(
        &mut e,
        "ALTER TABLE dbo.t ADD CONSTRAINT CK_t_val CHECK (val > 0)",
    );

    exec(&mut e, "ALTER TABLE dbo.t DROP CONSTRAINT CK_t_val");

    let r = query(
        &mut e,
        "SELECT name FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID('dbo.t')",
    );
    assert_eq!(r.rows.len(), 0);
}

#[test]
fn migration_add_check_constraint() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.accounts (id INT PRIMARY KEY, balance DECIMAL(10,2))",
    );
    exec(&mut e, "INSERT INTO dbo.accounts VALUES (1, 100.00)");

    exec(
        &mut e,
        "ALTER TABLE dbo.accounts ADD CONSTRAINT CK_balance_positive CHECK (balance >= 0)",
    );

    let err = e.exec("INSERT INTO dbo.accounts VALUES (2, -50.00)");
    assert!(
        err.is_err(),
        "CHECK constraint should reject negative balance"
    );

    exec(&mut e, "INSERT INTO dbo.accounts VALUES (2, 0.00)");
    let r = query(&mut e, "SELECT COUNT(*) FROM dbo.accounts");
    assert_eq!(r.rows[0][0], Value::BigInt(2));
}

#[test]
fn migration_add_primary_key() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.temp (id INT, name NVARCHAR(50))");
    exec(&mut e, "INSERT INTO dbo.temp VALUES (1, N'Alice')");
    exec(&mut e, "INSERT INTO dbo.temp VALUES (2, N'Bob')");

    exec(
        &mut e,
        "ALTER TABLE dbo.temp ADD CONSTRAINT PK_temp PRIMARY KEY (id)",
    );

    let r = query(
        &mut e,
        "SELECT name FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID('dbo.temp') AND type_desc = 'PRIMARY_KEY_CONSTRAINT'",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string_value(), "PK_temp");
}

#[test]
fn migration_add_foreign_key() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.categories (id INT PRIMARY KEY, name NVARCHAR(50))",
    );
    exec(
        &mut e,
        "INSERT INTO dbo.categories VALUES (1, N'Electronics')",
    );

    exec(
        &mut e,
        "CREATE TABLE dbo.products (id INT PRIMARY KEY, category_id INT, name NVARCHAR(100))",
    );
    exec(&mut e, "INSERT INTO dbo.products VALUES (1, 1, N'Laptop')");

    exec(
        &mut e,
        "ALTER TABLE dbo.products ADD CONSTRAINT FK_products_categories FOREIGN KEY (category_id) REFERENCES dbo.categories(id)",
    );

    let r = query(
        &mut e,
        "SELECT name FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('dbo.products')",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_string_value(), "FK_products_categories");
}

#[test]
fn migration_multi_step_schema_evolution() {
    let mut e = Engine::new();

    exec(
        &mut e,
        "CREATE TABLE dbo.employees (id INT PRIMARY KEY, name NVARCHAR(100))",
    );
    exec(
        &mut e,
        "INSERT INTO dbo.employees VALUES (1, N'Alice'), (2, N'Bob')",
    );

    exec(
        &mut e,
        "ALTER TABLE dbo.employees ADD department NVARCHAR(50) NULL",
    );
    exec(
        &mut e,
        "ALTER TABLE dbo.employees ADD salary DECIMAL(10,2) NULL",
    );

    exec(
        &mut e,
        "UPDATE dbo.employees SET department = N'Engineering', salary = 100000 WHERE id = 1",
    );
    exec(
        &mut e,
        "UPDATE dbo.employees SET department = N'Sales', salary = 80000 WHERE id = 2",
    );

    let r = query(
        &mut e,
        "SELECT id, name, department, salary FROM dbo.employees ORDER BY id",
    );
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][2].to_string_value(), "Engineering");
    assert_eq!(r.rows[1][2].to_string_value(), "Sales");
}

#[test]
fn migration_drop_foreign_key() {
    let mut e = Engine::new();
    exec(&mut e, "CREATE TABLE dbo.parent (id INT PRIMARY KEY)");
    exec(
        &mut e,
        "CREATE TABLE dbo.child (id INT PRIMARY KEY, parent_id INT)",
    );
    exec(
        &mut e,
        "ALTER TABLE dbo.child ADD CONSTRAINT FK_child_parent FOREIGN KEY (parent_id) REFERENCES dbo.parent(id)",
    );

    exec(
        &mut e,
        "ALTER TABLE dbo.child DROP CONSTRAINT FK_child_parent",
    );

    let r = query(
        &mut e,
        "SELECT name FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('dbo.child')",
    );
    assert_eq!(r.rows.len(), 0);
}

