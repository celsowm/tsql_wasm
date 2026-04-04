include!("new_functions/helpers.rs");

// ─── DB_NAME / DB_ID ─────────────────────────────────────────────────────

#[test]
fn test_db_name() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DB_NAME() AS v");
    assert!(matches!(r.rows[0][0], Value::NVarChar(_)));
}

#[test]
fn test_db_id() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DB_ID() AS v");
    assert_eq!(r.rows[0][0], Value::Int(1));
}

// ─── SUSER / USER functions ──────────────────────────────────────────────

#[test]
fn test_suser_sname() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SUSER_SNAME() AS v");
    assert!(matches!(r.rows[0][0], Value::NVarChar(_)));
}

#[test]
fn test_user_name() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT USER_NAME() AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("dbo".to_string()));
}

#[test]
fn test_app_name() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT APP_NAME() AS v");
    assert!(matches!(r.rows[0][0], Value::NVarChar(_)));
}

#[test]
fn test_host_name() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT HOST_NAME() AS v");
    assert!(matches!(r.rows[0][0], Value::NVarChar(_)));
}

#[test]
fn test_system_user_via_suser() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SUSER_SNAME() AS v");
    assert!(matches!(r.rows[0][0], Value::NVarChar(_)));
}

#[test]
fn test_system_functions_in_select() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DB_NAME() AS db, USER_NAME() AS usr, DB_ID() AS dbid, SUSER_SNAME() AS login",
    );
    assert_eq!(r.columns.len(), 4);
    assert!(matches!(r.rows[0][0], Value::NVarChar(_)));
    assert_eq!(r.rows[0][1], Value::NVarChar("dbo".to_string()));
    assert_eq!(r.rows[0][2], Value::Int(1));
    assert!(matches!(r.rows[0][3], Value::NVarChar(_)));
}

#[test]
fn test_sql_server_handshake_probe_functions() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT SERVERPROPERTY('Edition') AS edition, SERVERPROPERTY('EngineEdition') AS engine_edition, SERVERPROPERTY('ProductVersion') AS product_version, @@MICROSOFTVERSION AS microsoft_version, CONNECTIONPROPERTY('net_transport') AS transport",
    );
    assert!(matches!(r.rows[0][0], Value::NVarChar(_)));
    assert_eq!(r.rows[0][1], Value::Int(3));
    assert_eq!(r.rows[0][2], Value::NVarChar("16.0.1000.6".to_string()));
    assert!(matches!(r.rows[0][3], Value::Int(_)));
    assert_eq!(r.rows[0][4], Value::NVarChar("TCP".to_string()));

    let r = query(&mut engine, "SELECT host_platform FROM sys.dm_os_host_info");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("Windows".to_string()));
}

#[test]
fn test_bitwise_and_in_sql_server_probe_expression() {
    parse_sql("SELECT @@MICROSOFTVERSION & 0xffff AS v").expect("parse failed");
}

// ─── HASHBYTES ────────────────────────────────────────────────────────────

#[test]
fn test_hashbytes_md5() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT HASHBYTES('MD5', 'test') AS v");
    assert!(matches!(r.rows[0][0], Value::VarBinary(_)));
    if let Value::VarBinary(bytes) = &r.rows[0][0] {
        assert_eq!(bytes.len(), 16);
    }
}

#[test]
fn test_hashbytes_sha256() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT HASHBYTES('SHA2_256', 'test') AS v");
    assert!(matches!(r.rows[0][0], Value::VarBinary(_)));
    if let Value::VarBinary(bytes) = &r.rows[0][0] {
        assert_eq!(bytes.len(), 32);
    }
}

#[test]
fn test_hashbytes_deterministic() {
    let mut engine = Engine::new();
    let r1 = query(&mut engine, "SELECT HASHBYTES('MD5', 'hello') AS v");
    let r2 = query(&mut engine, "SELECT HASHBYTES('MD5', 'hello') AS v");
    assert_eq!(r1.rows[0][0], r2.rows[0][0]);
}

#[test]
fn test_hashbytes_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT HASHBYTES('MD5', NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_hashbytes_in_where() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CASE WHEN HASHBYTES('MD5', 'test') IS NOT NULL THEN 'has_hash' ELSE 'no_hash' END AS v",
    );
    assert_eq!(r.rows[0][0], Value::VarChar("has_hash".to_string()));
}

// ─── PARSENAME ────────────────────────────────────────────────────────────

#[test]
fn test_parsename_object() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PARSENAME('server.db.dbo.table', 1) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("table".to_string()));
}

#[test]
fn test_parsename_schema() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PARSENAME('server.db.dbo.table', 2) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("dbo".to_string()));
}

#[test]
fn test_parsename_database() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PARSENAME('server.db.dbo.table', 3) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("db".to_string()));
}

#[test]
fn test_parsename_server() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PARSENAME('server.db.dbo.table', 4) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("server".to_string()));
}

#[test]
fn test_parsename_simple() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PARSENAME('dbo.table', 1) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("table".to_string()));
}

#[test]
fn test_parsename_invalid_part() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PARSENAME('dbo.table', 5) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_parsename_in_query() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.objects (full_name VARCHAR(100))");
    exec(&mut engine, "INSERT INTO dbo.objects (full_name) VALUES ('server1.mydb.dbo.users')");
    exec(&mut engine, "INSERT INTO dbo.objects (full_name) VALUES ('server2.otherdb.dbo.orders')");
    let r = query(
        &mut engine,
        "SELECT PARSENAME(full_name, 3) AS database_name FROM dbo.objects ORDER BY full_name",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("mydb".to_string()));
    assert_eq!(r.rows[1][0], Value::NVarChar("otherdb".to_string()));
}

// ─── QUOTENAME ────────────────────────────────────────────────────────────

#[test]
fn test_quotename_default() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT QUOTENAME('my table') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("[my table]".to_string()));
}

#[test]
fn test_quotename_custom_char() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT QUOTENAME('hello', '\"') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("\"hello\"".to_string()));
}

#[test]
fn test_quotename_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT QUOTENAME(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── CHECKSUM ─────────────────────────────────────────────────────────────

#[test]
fn test_checksum_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CHECKSUM('hello') AS v");
    assert!(matches!(r.rows[0][0], Value::Int(_)));
}

#[test]
fn test_checksum_deterministic() {
    let mut engine = Engine::new();
    let r1 = query(&mut engine, "SELECT CHECKSUM('test') AS v");
    let r2 = query(&mut engine, "SELECT CHECKSUM('test') AS v");
    assert_eq!(r1.rows[0][0], r2.rows[0][0]);
}

#[test]
fn test_checksum_different_inputs() {
    let mut engine = Engine::new();
    let r1 = query(&mut engine, "SELECT CHECKSUM('abc') AS v");
    let r2 = query(&mut engine, "SELECT CHECKSUM('xyz') AS v");
    assert_ne!(r1.rows[0][0], r2.rows[0][0]);
}
