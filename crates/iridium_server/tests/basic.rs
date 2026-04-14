mod common;
use common::*;

#[tokio::test]
async fn test_prelogin_and_login() {
    let port = start_server().await;
    let mut client = tokio::time::timeout(std::time::Duration::from_secs(10), connect(port))
        .await
        .expect("Connection timed out");

    let (cols, rows) = query_sql(&mut client, "SELECT 1 as n").await;
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], "n");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");
}

#[tokio::test]
async fn test_select_string() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let (cols, rows) = query_sql(&mut client, "SELECT 'hello' as greeting").await;
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], "greeting");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "hello");
}

#[tokio::test]
async fn test_select_multiple_columns() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let (cols, rows) = query_sql(&mut client, "SELECT 42 as num, 'test' as str").await;
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0], "num");
    assert_eq!(cols[1], "str");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "42");
    assert_eq!(rows[0][1], "test");
}

#[tokio::test]
async fn test_error_handling() {
    let port = start_server().await;
    let mut client = connect(port).await;

    let result = client.query("SELECT * FROM nonexistent_table", &[]).await;
    if let Ok(stream) = result {
        let first_result = stream.into_first_result().await;
        assert!(first_result.is_err());
    }
}

