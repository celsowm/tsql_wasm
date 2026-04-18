use iridium_core::parser::parse_batch;

#[test]
fn test_ssms_batch_reproduction() {
    let sql = "
select host_platform from sys.dm_os_host_info
if @edition = N'SQL Azure'
    select 'TCP' as ConnectionProtocol
else
    exec ('select CONVERT(nvarchar(40),CONNECTIONPROPERTY(''net_transport'')) as ConnectionProtocol')
";
    let stmts = parse_batch(sql).expect("Should parse SSMS batch");
    for (i, stmt) in stmts.iter().enumerate() {
        println!("Statement {}: {:?}", i, stmt);
    }
    assert_eq!(stmts.len(), 2);
}

#[test]
fn test_nested_exec_quotes() {
    let sql = "exec ('select ''select ''''a''''''')";
    let stmts = parse_batch(sql).expect("Should parse nested exec quotes");
    println!("Nested Statement: {:?}", stmts[0]);
    assert_eq!(stmts.len(), 1);
}

#[test]
fn test_unterminated_string() {
    let sql = "SELECT 'abc";
    let res = parse_batch(sql);
    println!("Unterminated result: {:?}", res);
    // It should probably be an error, but let's see what it does now.
}
