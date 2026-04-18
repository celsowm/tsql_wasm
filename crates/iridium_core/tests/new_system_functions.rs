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

#[test]
fn test_db_name_and_id_with_argument() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DB_NAME(1) AS dbname, DB_ID('master') AS dbid",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("master".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(1));
}

#[test]
fn test_db_name_and_id_system_databases() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT DB_NAME(2) AS tempdb_name, DB_ID('tempdb') AS tempdb_id, DB_NAME(3) AS model_name, DB_ID('model') AS model_id, DB_NAME(4) AS msdb_name, DB_ID('msdb') AS msdb_id",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("tempdb".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(2));
    assert_eq!(r.rows[0][2], Value::NVarChar("model".to_string()));
    assert_eq!(r.rows[0][3], Value::Int(3));
    assert_eq!(r.rows[0][4], Value::NVarChar("msdb".to_string()));
    assert_eq!(r.rows[0][5], Value::Int(4));
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
        "SELECT SERVERPROPERTY('Edition') AS edition, SERVERPROPERTY('EngineEdition') AS engine_edition, SERVERPROPERTY('ProductVersion') AS product_version, SERVERPROPERTY('IsSingleUser') AS is_single_user, FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') AS is_fulltext_installed, @@MICROSOFTVERSION AS microsoft_version, CONNECTIONPROPERTY('net_transport') AS transport",
    );
    assert!(matches!(r.rows[0][0], Value::NVarChar(_)));
    assert_eq!(r.rows[0][1], Value::Int(3));
    assert_eq!(r.rows[0][2], Value::NVarChar("16.0.1000.6".to_string()));
    assert_eq!(r.rows[0][3], Value::Int(0));
    assert_eq!(r.rows[0][4], Value::Int(0));
    assert!(matches!(r.rows[0][5], Value::Int(_)));
    assert_eq!(r.rows[0][6], Value::NVarChar("TCP".to_string()));

    let r = query(
        &mut engine,
        "SELECT host_platform, host_sku FROM sys.dm_os_host_info",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("Windows".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(7));
}

#[test]
fn test_serverproperty_is_hadr_enabled_probe() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CAST(SERVERPROPERTY('IsHadrEnabled') AS bit) AS [IsHadrEnabled]",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Bit(false));
}

#[test]
fn test_contained_ag_session_probe_batch_returns_zero() {
    let engine = Engine::new();
    let batch = iridium_core::parse_batch(
        "IF OBJECT_ID(N'sys.sp_MSIsContainedAGSession', N'P') IS NOT NULL BEGIN DECLARE @x int; EXECUTE @x = sys.sp_MSIsContainedAGSession; SELECT @x END ELSE SELECT 0",
    )
    .expect("parse batch failed");
    let r = engine
        .execute_batch(batch)
        .expect("execute batch failed")
        .expect("expected result");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(0));
}

#[test]
fn test_xp_msver_is_available_for_object_explorer_probes() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "EXEC master.dbo.xp_msver");
    assert_eq!(r.columns, vec!["ID", "Name", "Internal_Value", "Value"]);
    assert!(r.rows.len() >= 4);
    assert!(r
        .rows
        .iter()
        .any(|row| matches!(&row[1], Value::NVarChar(name) if name.eq_ignore_ascii_case("ProcessorCount"))));
    assert!(r
        .rows
        .iter()
        .any(|row| matches!(&row[1], Value::NVarChar(name) if name.eq_ignore_ascii_case("PhysicalMemory"))));
}

#[test]
fn test_xp_qv_alwayson_probe_sets_exec_return_variable() {
    let engine = Engine::new();
    let batch = iridium_core::parse_batch(
        "DECLARE @alwayson INT; EXECUTE @alwayson = master.dbo.xp_qv N'3641190370', @@SERVICENAME; SELECT ISNULL(@alwayson, -1) AS [AlwaysOn]",
    )
    .expect("parse batch failed");
    let r = engine
        .execute_batch(batch)
        .expect("execute batch failed")
        .expect("expected result");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(-1));
}

#[test]
fn test_exec_return_variable_captures_user_procedure_return_code() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE PROCEDURE dbo.return_seven AS BEGIN RETURN 7 END",
    );
    let batch = iridium_core::parse_batch(
        "DECLARE @rc INT = 0; EXEC @rc = dbo.return_seven; SELECT @rc AS rc",
    )
    .expect("parse batch failed");
    let r = engine
        .execute_batch(batch)
        .expect("execute batch failed")
        .expect("expected result");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(7));
}

#[test]
fn test_sys_databases_master_is_visible() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT name, database_id, state_desc FROM sys.databases WHERE name = 'master'",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("master".to_string()));
    assert_eq!(r.rows[0][1], Value::Int(1));
    assert_eq!(r.rows[0][2], Value::VarChar("ONLINE".to_string()));
}

#[test]
fn test_sys_configurations_object_explorer_probe() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "select value_in_use from sys.configurations where configuration_id = 16384",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(0));
}

#[test]
fn test_syspolicy_configuration_object_explorer_probe() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT \
            CAST((SELECT current_value FROM msdb.dbo.syspolicy_configuration WHERE name = 'Enabled') AS bit) AS Enabled, \
            CAST((SELECT current_value FROM msdb.dbo.syspolicy_configuration WHERE name = 'HistoryRetentionInDays') AS int) AS HistoryRetentionInDays, \
            CAST((SELECT current_value FROM msdb.dbo.syspolicy_configuration WHERE name = 'LogOnSuccess') AS bit) AS LogOnSuccess",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Bit(true));
    assert_eq!(r.rows[0][1], Value::Int(90));
    assert_eq!(r.rows[0][2], Value::Bit(false));
}

#[test]
fn test_syspolicy_health_state_probe() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CASE WHEN 1 = msdb.dbo.fn_syspolicy_is_automation_enabled() AND EXISTS (SELECT * FROM msdb.dbo.syspolicy_system_health_state WHERE target_query_expression_with_id LIKE 'Server%') THEN 1 ELSE 0 END AS PolicyHealthState",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::Int(1));
}

#[test]
fn test_designer_table_filegroup_probe() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.DesignerCategories (CategoryId INT NOT NULL, Name NVARCHAR(100) NOT NULL)",
    );
    let r = query(
        &mut engine,
        "SELECT Fg.name AS TableFg, dsp.name AS TexImageFg, FtCat.name AS FulltextCatalog FROM sys.tables tbl LEFT OUTER JOIN sys.change_tracking_tables AS ctt ON ctt.object_id = tbl.object_id LEFT OUTER JOIN sys.data_spaces dsp ON dsp.data_space_id = tbl.lob_data_space_id LEFT OUTER JOIN (sys.fulltext_indexes fti INNER JOIN sys.fulltext_catalogs FtCat ON FtCat.fulltext_catalog_id = fti.fulltext_catalog_id) ON fti.object_id = tbl.object_id INNER JOIN (sys.indexes idx INNER JOIN sys.data_spaces Fg ON (idx.index_id = 0 OR idx.index_id = 1) AND Fg.data_space_id = idx.data_space_id) ON idx.object_id = tbl.object_id AND (idx.index_id = 0 OR idx.index_id = 1) WHERE tbl.object_id = object_id(N'dbo.DesignerCategories')",
    );
    assert!(!r.rows.is_empty());
    assert!(r
        .rows
        .iter()
        .all(|row| row[0] == Value::VarChar("PRIMARY".to_string())));
    assert!(r.rows.iter().all(|row| row[1].is_null()));
    assert!(r.rows.iter().all(|row| row[2].is_null()));
}

#[test]
fn test_designer_partition_scheme_filegroups_probe_no_fg_error() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "select sch.name, sch.data_space_id, sch.function_id, dest.destination_id, fg.name as fg_name, fg.type from sys.partition_schemes sch inner join (sys.destination_data_spaces dest inner join sys.filegroups fg on fg.data_space_id = dest.data_space_id) on dest.partition_scheme_id = sch.data_space_id order by sch.data_space_id, dest.destination_id",
    );
    assert_eq!(
        r.columns,
        vec![
            "name",
            "data_space_id",
            "function_id",
            "destination_id",
            "fg_name",
            "TYPE",
        ]
    );
}

#[test]
fn test_ssms_database_info_probe_with_master_files_df_alias() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT dtb.name AS [Name], ISNULL(df.physical_name, N'') AS [PrimaryFilePath] FROM master.sys.databases AS dtb LEFT OUTER JOIN sys.master_files AS df ON df.database_id = dtb.database_id and 1=df.data_space_id and 1 = df.file_id WHERE dtb.name = N'iridium_sql'",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("iridium_sql".to_string()));
}

#[test]
fn test_ssms_database_info_probe_with_dtb_containment_and_ledger() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT dtb.name AS [Database_Name], dtb.containment AS [Database_ContainmentType], CAST(ISNULL(dtb.is_ledger_on, 0) AS bit) AS [Database_IsLedger] FROM master.sys.databases AS dtb WHERE dtb.name = N'master'",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("master".to_string()));
    assert_eq!(r.rows[0][1], Value::TinyInt(0));
    assert_eq!(r.rows[0][2], Value::Bit(false));
}
#[test]
fn test_is_srvrolemember_object_explorer_probe() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "select is_srvrolemember('sysadmin') * 1 +is_srvrolemember('serveradmin') * 2 +is_srvrolemember('setupadmin') * 4 +is_srvrolemember('securityadmin') * 8 +is_srvrolemember('processadmin') * 16 +is_srvrolemember('dbcreator') * 32 +is_srvrolemember('diskadmin') * 64+ is_srvrolemember('bulkadmin') * 128",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].to_integer_i64(), Some(1));
}

#[test]
fn test_has_dbaccess_for_master() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT HAS_DBACCESS('master') AS has_master, HAS_DBACCESS('msdb') AS has_msdb, HAS_DBACCESS('iridium_sql') AS has_iridium_sql",
    );
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Int(1));
    assert_eq!(r.rows[0][2], Value::Int(1));
}

#[test]
fn test_has_perms_by_name_probe() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW ANY DATABASE') AS null_case, HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') AS server_state_case, HAS_PERMS_BY_NAME('SERVER', 'SERVER', 'VIEW ANY DATABASE') AS server_case, HAS_PERMS_BY_NAME('master', 'DATABASE', 'CONNECT') AS db_case",
    );
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::Int(1));
    assert_eq!(r.rows[0][2], Value::Int(1));
    assert_eq!(r.rows[0][3], Value::Int(1));
}

#[test]
fn test_sys_sysdatabases_compat_view() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT name, dbid, cmptlevel FROM sys.sysdatabases WHERE name = 'master'",
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], Value::VarChar("master".to_string()));
    assert_eq!(r.rows[0][1], Value::SmallInt(1));
    assert_eq!(r.rows[0][2], Value::TinyInt(160));
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
    let r = query(
        &mut engine,
        "SELECT PARSENAME('server.db.dbo.table', 1) AS v",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("table".to_string()));
}

#[test]
fn test_parsename_schema() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT PARSENAME('server.db.dbo.table', 2) AS v",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("dbo".to_string()));
}

#[test]
fn test_parsename_database() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT PARSENAME('server.db.dbo.table', 3) AS v",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("db".to_string()));
}

#[test]
fn test_parsename_server() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT PARSENAME('server.db.dbo.table', 4) AS v",
    );
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
    exec(
        &mut engine,
        "CREATE TABLE dbo.objects (full_name VARCHAR(100))",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.objects (full_name) VALUES ('server1.mydb.dbo.users')",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.objects (full_name) VALUES ('server2.otherdb.dbo.orders')",
    );
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

// ─── IDENTITY METADATA ────────────────────────────────────────────────────

#[test]
fn test_ident_seed_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE t1 (id INT IDENTITY(10, 5))");
    let r = query(&mut engine, "SELECT IDENT_SEED('t1') AS v");
    assert_eq!(r.rows[0][0], Value::BigInt(10));
}

#[test]
fn test_ident_incr_basic() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE t1 (id INT IDENTITY(10, 5))");
    let r = query(&mut engine, "SELECT IDENT_INCR('t1') AS v");
    assert_eq!(r.rows[0][0], Value::BigInt(5));
}

#[test]
fn test_ident_metadata_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT IDENT_SEED('nonexistent') AS v");
    assert!(r.rows[0][0].is_null());
}

