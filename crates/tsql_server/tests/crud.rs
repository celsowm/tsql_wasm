use tsql_server_test_support::*;

#[tokio::test]
async fn test_create_table_and_insert() {
    let port = start_server().await;
    let mut client = connect(port).await;

    exec_sql(
        &mut client,
        "CREATE TABLE test_users (id INT, name NVARCHAR(50), active BIT)",
    )
    .await;

    exec_sql(
        &mut client,
        "INSERT INTO test_users VALUES (1, N'Alice', 1)",
    )
    .await;
    exec_sql(&mut client, "INSERT INTO test_users VALUES (2, N'Bob', 0)").await;

    let (cols, rows) = query_sql(&mut client, "SELECT id, name FROM test_users ORDER BY id").await;
    assert_eq!(cols.len(), 2);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[1][1], "Bob");
}

#[tokio::test]
async fn test_join() {
    let port = start_server().await;
    let mut client = connect(port).await;

    exec_sql(
        &mut client,
        "CREATE TABLE t_orders (order_id INT, customer_id INT, amount INT)",
    )
    .await;
    exec_sql(
        &mut client,
        "CREATE TABLE t_customers (customer_id INT, name NVARCHAR(50))",
    )
    .await;

    exec_sql(
        &mut client,
        "INSERT INTO t_customers VALUES (1, N'Alice'), (2, N'Bob')",
    )
    .await;
    exec_sql(
        &mut client,
        "INSERT INTO t_orders VALUES (100, 1, 50), (101, 1, 75), (102, 2, 30)",
    )
    .await;

    let (_, rows) = query_sql(
        &mut client,
        "SELECT c.name, SUM(o.amount) as total \
         FROM t_customers c \
         INNER JOIN t_orders o ON c.customer_id = o.customer_id \
         GROUP BY c.name \
         ORDER BY total DESC",
    )
    .await;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "Alice");
    assert_eq!(rows[0][1], "125");
}

#[tokio::test]
async fn test_identity() {
    let port = start_server().await;
    let mut client = connect(port).await;

    exec_sql(
        &mut client,
        "CREATE TABLE t_id (id INT IDENTITY(1,1) PRIMARY KEY, val NVARCHAR(20))",
    )
    .await;

    exec_sql(&mut client, "INSERT INTO t_id (val) VALUES (N'first')").await;
    exec_sql(&mut client, "INSERT INTO t_id (val) VALUES (N'second')").await;

    let (_, rows) = query_sql(&mut client, "SELECT id, val FROM t_id ORDER BY id").await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[1][0], "2");
}
