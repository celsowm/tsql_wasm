use tiberius::{Client, Config, Row};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use iridium_core::{parse_sql, types::Value, Engine};

/// Helper to convert engine Value to a string representation that matches SQL Server's TDS output
fn engine_val_to_string(val: &Value) -> String {
    match val {
        Value::Null => "NULL".to_string(),
        Value::Bit(v) => (if *v { "1" } else { "0" }).to_string(),
        Value::Money(v) => {
            // SQL Server TDS doesn't include the $ prefix
            iridium_core::types::format_decimal(*v, 4)
        }
        Value::SmallMoney(v) => iridium_core::types::format_decimal(*v as i128, 4),
        _ => val.to_string_value(),
    }
}

/// Helper to convert Tiberius Row to a vector of strings
fn tiberius_row_to_strings(row: &Row) -> Vec<String> {
    (0..row.len())
        .map(|i| {
            if let Ok(Some(v)) = row.try_get::<&str, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<i32, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<i64, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<i16, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<u8, _>(i) {
                return v.to_string();
            }
            if let Ok(Some(v)) = row.try_get::<f64, _>(i) {
                // Formatting float to match engine's format_float
                return iridium_core::types::format_float(v);
            }
            if let Ok(Some(v)) = row.try_get::<bool, _>(i) {
                return if v { "1".to_string() } else { "0".to_string() };
            }
            if let Ok(Some(v)) = row.try_get::<tiberius::numeric::Numeric, _>(i) {
                return v.to_string();
            }
            "NULL".to_string()
        })
        .collect()
}

async fn get_sqlserver_client() -> Client<tokio_util::compat::Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(11433);
    config.authentication(tiberius::AuthMethod::sql_server("sa", "Test@12345"));
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("Failed to connect to Podman SQL Server");
    tcp.set_nodelay(true).unwrap();

    Client::connect(config, tcp.compat_write())
        .await
        .expect("Failed to connect TDS")
}

async fn sqlserver_supports_vector() -> bool {
    let mut client = get_sqlserver_client().await;
    let stream = client
        .query("SELECT CASE WHEN TYPE_ID('vector') IS NULL THEN 0 ELSE 1 END", &[])
        .await
        .expect("Failed to probe SQL Server VECTOR support");
    let rows = stream
        .into_first_result()
        .await
        .expect("Failed to read VECTOR support probe");
    rows.first()
        .and_then(|row| row.try_get::<i32, _>(0).ok().flatten())
        == Some(1)
}

async fn compare(sql: &str) {
    let engine = Engine::new();
    let mut client = get_sqlserver_client().await;

    // Run on iridium_core
    let stmt = parse_sql(sql).expect("Failed to parse SQL for engine");
    let engine_res = engine
        .execute(stmt)
        .expect("Engine execution failed")
        .expect("Expected result from engine");
    let engine_rows: Vec<Vec<String>> = engine_res
        .rows
        .iter()
        .map(|r| r.iter().map(engine_val_to_string).collect())
        .collect();

    // Run on SQL Server
    let stream = client
        .query(sql, &[])
        .await
        .expect("SQL Server query failed");
    let ss_rows_raw = stream
        .into_first_result()
        .await
        .expect("Failed to get results from SQL Server");
    let ss_rows: Vec<Vec<String>> = ss_rows_raw.iter().map(tiberius_row_to_strings).collect();

    assert_eq!(engine_rows, ss_rows, "Mismatch for SQL: {}", sql);
    println!("Success comparing: {}", sql);
}

async fn compare_after_setup(setup_sqls: &[&str], sql: &str) {
    let engine = Engine::new();
    let mut client = get_sqlserver_client().await;

    for setup_sql in setup_sqls {
        engine.exec(setup_sql).expect(setup_sql);
        client
            .execute(*setup_sql, &[])
            .await
            .expect(setup_sql);
    }

    let stmt = parse_sql(sql).expect("Failed to parse SQL for engine");
    let engine_res = engine
        .execute(stmt)
        .expect("Engine execution failed")
        .expect("Expected result from engine");
    let engine_rows: Vec<Vec<String>> = engine_res
        .rows
        .iter()
        .map(|r| r.iter().map(engine_val_to_string).collect())
        .collect();

    let stream = client
        .query(sql, &[])
        .await
        .expect("SQL Server query failed");
    let ss_rows_raw = stream
        .into_first_result()
        .await
        .expect("Failed to get results from SQL Server");
    let ss_rows: Vec<Vec<String>> = ss_rows_raw.iter().map(tiberius_row_to_strings).collect();

    assert_eq!(engine_rows, ss_rows, "Mismatch for SQL after setup: {}", sql);
    println!("Success comparing after setup: {}", sql);
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos()
}

async fn compare_after_setup_owned(setup_sqls: &[String], sql: &str) {
    let setup_refs: Vec<&str> = setup_sqls.iter().map(|s| s.as_str()).collect();
    compare_after_setup(&setup_refs, sql).await;
}

#[tokio::test]
#[ignore] // Skip by default as it requires a running Podman container
async fn test_compare_basic_math() {
    compare("SELECT 1 + 1").await;
    compare("SELECT 10 * 3").await;
    compare("SELECT 100 / 4").await;
    compare("SELECT ABS(-42)").await;
}

#[tokio::test]
#[ignore]
async fn test_compare_strings() {
    compare("SELECT 'hello' + ' world'").await;
    compare("SELECT UPPER('rust')").await;
    compare("SELECT LOWER('SQL')").await;
    compare("SELECT LEN('test')").await;
}

#[tokio::test]
#[ignore]
async fn test_compare_logic() {
    compare("SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END").await;
    compare("SELECT CASE WHEN 1=0 THEN 'yes' ELSE 'no' END").await;
}

#[tokio::test]
#[ignore]
async fn test_compare_throw_catch() {
    compare(
        "BEGIN TRY THROW 50001, 'boom', 1 END TRY BEGIN CATCH SELECT ERROR_NUMBER() AS n, ERROR_SEVERITY() AS s, ERROR_STATE() AS st, ERROR_MESSAGE() AS msg END CATCH",
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn test_compare_greatest_least() {
    compare("SELECT GREATEST(1, 5, 3) AS g, LEAST(1, 5, 3) AS l").await;
}

#[tokio::test]
#[ignore]
async fn test_compare_string_split_with_ordinal() {
    compare(
        "SELECT value, ordinal FROM STRING_SPLIT('a,b,c', ',', 1) ORDER BY ordinal",
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn test_compare_alter_column() {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    let table_name = format!("AlterColumnTest_{}", suffix);
    let full_table_name = format!("dbo.{}", table_name);

    let create_sql = format!("CREATE TABLE {} (v INT NOT NULL)", full_table_name);
    let insert_sql = format!("INSERT INTO {} VALUES (1)", full_table_name);
    let alter_sql = format!(
        "ALTER TABLE {} ALTER COLUMN v BIGINT NOT NULL",
        full_table_name
    );
    let select_sql = format!(
        "SELECT DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{}' AND COLUMN_NAME = 'v'",
        table_name
    );

    compare_after_setup(&[&create_sql, &insert_sql, &alter_sql], &select_sql).await;
}

#[tokio::test]
#[ignore]
async fn test_compare_vector_cast_and_distance() {
    if !sqlserver_supports_vector().await {
        println!("Skipping vector parity test: SQL Server does not support VECTOR");
        return;
    }
    compare("SELECT DATALENGTH(CAST('[1,2,3]' AS VECTOR(3))) AS bytes").await;
    compare(
        "SELECT VECTOR_DISTANCE('euclidean', CAST('[1,0]' AS VECTOR(2)), CAST('[0,0]' AS VECTOR(2))) AS e, VECTOR_DISTANCE('cosine', CAST('[1,0]' AS VECTOR(2)), CAST('[0,1]' AS VECTOR(2))) AS c, VECTOR_DISTANCE('dot', CAST('[1,2]' AS VECTOR(2)), CAST('[3,4]' AS VECTOR(2))) AS d",
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn test_compare_vector_metadata() {
    if !sqlserver_supports_vector().await {
        println!("Skipping vector parity test: SQL Server does not support VECTOR");
        return;
    }
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    let table_name_1 = format!("VectorMetadataTestInfo_{}", suffix);
    let full_table_name_1 = format!("dbo.{}", table_name_1);
    let table_name_2 = format!("VectorMetadataTestSys_{}", suffix);
    let full_table_name_2 = format!("dbo.{}", table_name_2);

    let create_sql_1 = format!(
        "CREATE TABLE {} (id INT NOT NULL, embedding VECTOR(3) NOT NULL)",
        full_table_name_1
    );

    let info_schema_sql = format!(
        "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{}' AND COLUMN_NAME = 'embedding'",
        table_name_1
    );
    compare_after_setup(&[&create_sql_1], &info_schema_sql).await;

    let create_sql_2 = format!(
        "CREATE TABLE {} (id INT NOT NULL, embedding VECTOR(3) NOT NULL)",
        full_table_name_2
    );

    let sys_columns_sql = format!(
        "SELECT system_type_id, max_length, vector_dimensions, vector_base_type, vector_base_type_desc FROM sys.columns WHERE object_id = OBJECT_ID('{}') AND name = 'embedding'",
        full_table_name_2
    );
    compare_after_setup(&[&create_sql_2], &sys_columns_sql).await;
}
#[tokio::test]
#[ignore]
async fn test_compare_designer_table_list_probe() {
    let suffix = unique_suffix();
    let table_name = format!("DesignerCustomers_{}", suffix);
    let full_table_name = format!("dbo.{}", table_name);

    let setup_sqls = vec![format!(
        "CREATE TABLE {} (CustomerId INT NOT NULL, Name NVARCHAR(50) NOT NULL, CONSTRAINT PK_{} PRIMARY KEY CLUSTERED (CustomerId))",
        full_table_name, table_name
    )];

    let sql = format!(
        "SELECT tbl.name AS [Name], SCHEMA_NAME(tbl.schema_id) AS [Schema], CAST(tbl.is_memory_optimized AS bit) AS [IsMemoryOptimized], CAST(CASE idx.type WHEN 5 THEN 1 ELSE 0 END AS bit) AS [HasClusteredColumnStoreIndex], CAST(CASE WHEN 'PS' = dsidx.type THEN 1 ELSE 0 END AS bit) AS [IsPartitioned], CAST(ISNULL((SELECT distinct 1 FROM sys.all_columns WHERE object_id = tbl.object_id AND is_sparse = 1), 0) AS bit) AS [HasSparseColumn], CAST(CASE WHEN (SELECT major_id FROM sys.extended_properties WHERE major_id = tbl.object_id AND minor_id = 0 AND class = 1 AND name = N'microsoft_database_tools_support') IS NOT NULL THEN 1 ELSE 0 END AS bit) AS [HasMdtSupport] FROM sys.tables AS tbl LEFT JOIN sys.indexes AS idx ON idx.object_id = tbl.object_id AND (idx.index_id < 2 OR (tbl.is_memory_optimized = 1 AND idx.index_id = (SELECT MIN(index_id) FROM sys.indexes WHERE object_id = tbl.object_id))) LEFT OUTER JOIN sys.data_spaces AS dsidx ON dsidx.data_space_id = idx.data_space_id WHERE CAST(ISNULL(tbl.ledger_type, 0) AS int) = 0 AND tbl.is_filetable = 0 AND CAST(tbl.is_memory_optimized AS bit) = 0 AND tbl.temporal_type = 0 AND CAST(tbl.is_external AS bit) = 0 AND CAST(tbl.is_node AS bit) = 0 AND CAST(tbl.is_edge AS bit) = 0 AND tbl.is_ms_shipped = 0 AND tbl.name = '{}' AND SCHEMA_NAME(tbl.schema_id) = N'dbo' ORDER BY [Schema], [Name]",
        table_name
    );

    compare_after_setup_owned(&setup_sqls, &sql).await;
}

#[tokio::test]
#[ignore]
async fn test_compare_designer_table_columns_probe() {
    let suffix = unique_suffix();
    let table_name = format!("DesignerColumns_{}", suffix);
    let full_table_name = format!("dbo.{}", table_name);

    let setup_sqls = vec![format!(
        "CREATE TABLE {} (CustomerId INT NOT NULL, Name NVARCHAR(50) NOT NULL, CONSTRAINT PK_{} PRIMARY KEY CLUSTERED (CustomerId))",
        full_table_name, table_name
    )];

    let sql = format!(
        "SELECT clmns.name AS [Name], CAST(ISNULL(cik.index_column_id, 0) AS bit) AS [InPrimaryKey], CAST(ISNULL((select TOP 1 1 from sys.foreign_key_columns AS colfk where colfk.parent_column_id = clmns.column_id and colfk.parent_object_id = clmns.object_id), 0) AS bit) AS [IsForeignKey], ISNULL(usrt.name, baset.name) AS [DataType], ISNULL(baset.name, N'') AS [SystemType], CAST(CASE WHEN baset.name IN (N'nchar', N'nvarchar') AND clmns.max_length <> -1 THEN clmns.max_length/2 ELSE clmns.max_length END AS int) AS [Length], CAST(clmns.precision AS int) AS [NumericPrecision], CAST(clmns.scale AS int) AS [NumericScale], clmns.is_nullable AS [Nullable], clmns.is_computed AS [Computed], ISNULL(s2clmns.name, N'') AS [XmlSchemaNamespaceSchema], ISNULL(xscclmns.name, N'') AS [XmlSchemaNamespace], ISNULL((case clmns.is_xml_document when 1 then 2 else 1 end), 0) AS [XmlDocumentConstraint], CAST(clmns.is_sparse AS bit) AS [IsSparse], CAST(clmns.is_column_set AS bit) AS [IsColumnSet], clmns.column_id AS [ID], CAST(clmns.is_dropped_ledger_column AS bit) AS [IsDroppedLedgerColumn] FROM sys.tables AS tbl INNER JOIN sys.all_columns AS clmns ON clmns.object_id = tbl.object_id LEFT OUTER JOIN sys.indexes AS ik ON ik.object_id = clmns.object_id and 1 = ik.is_primary_key LEFT OUTER JOIN sys.index_columns AS cik ON cik.index_id = ik.index_id and cik.column_id = clmns.column_id and cik.object_id = clmns.object_id and 0 = cik.is_included_column LEFT OUTER JOIN sys.types AS usrt ON usrt.user_type_id = clmns.user_type_id LEFT OUTER JOIN sys.types AS baset ON (baset.user_type_id = clmns.system_type_id and baset.user_type_id = baset.system_type_id) or ((baset.system_type_id = clmns.system_type_id) and (baset.user_type_id = clmns.user_type_id) and (baset.is_user_defined = 0) and (baset.is_assembly_type = 1)) LEFT OUTER JOIN sys.xml_schema_collections AS xscclmns ON xscclmns.xml_collection_id = clmns.xml_collection_id LEFT OUTER JOIN sys.schemas AS s2clmns ON s2clmns.schema_id = xscclmns.schema_id WHERE (CAST(clmns.is_dropped_ledger_column AS bit) = 0) and ((tbl.name = N'{}' and SCHEMA_NAME(tbl.schema_id) = N'dbo')) ORDER BY [ID] ASC",
        table_name
    );

    compare_after_setup_owned(&setup_sqls, &sql).await;
}

#[tokio::test]
#[ignore]
async fn test_compare_designer_table_indexes_probe() {
    let suffix = unique_suffix();
    let table_name = format!("DesignerIndexes_{}", suffix);
    let full_table_name = format!("dbo.{}", table_name);

    let setup_sqls = vec![format!(
        "CREATE TABLE {} (CustomerId INT NOT NULL, Name NVARCHAR(50) NOT NULL, CONSTRAINT PK_{} PRIMARY KEY CLUSTERED (CustomerId))",
        full_table_name, table_name
    )];

    let sql = format!(
        "SELECT i.name, i.type_desc, i.is_unique, i.is_primary_key FROM sys.indexes i INNER JOIN sys.tables t ON i.object_id = t.object_id WHERE t.name = '{}' AND t.schema_id = SCHEMA_ID('dbo')",
        table_name
    );

    compare_after_setup_owned(&setup_sqls, &sql).await;
}

#[tokio::test]
#[ignore]
async fn test_compare_designer_table_foreign_keys_probe() {
    let suffix = unique_suffix();
    let customers_table = format!("DesignerCustomersFk_{}", suffix);
    let orders_table = format!("DesignerOrdersFk_{}", suffix);
    let full_customers_table = format!("dbo.{}", customers_table);
    let full_orders_table = format!("dbo.{}", orders_table);

    let setup_sqls = vec![
        format!(
            "CREATE TABLE {} (CustomerId INT NOT NULL, Name NVARCHAR(50) NOT NULL, CONSTRAINT PK_{} PRIMARY KEY CLUSTERED (CustomerId))",
            full_customers_table, customers_table
        ),
        format!(
            "CREATE TABLE {} (OrderId INT NOT NULL, CustomerId INT NOT NULL, CONSTRAINT PK_{} PRIMARY KEY (OrderId), CONSTRAINT FK_{}_Customers FOREIGN KEY (CustomerId) REFERENCES {}(CustomerId))",
            full_orders_table, orders_table, orders_table, full_customers_table
        ),
    ];

    let sql = format!(
        "SELECT cstr.name AS [Name] FROM sys.tables AS tbl INNER JOIN sys.foreign_keys AS cstr ON cstr.parent_object_id = tbl.object_id LEFT OUTER JOIN sys.indexes AS ki ON ki.index_id = cstr.key_index_id and ki.object_id = cstr.referenced_object_id WHERE (CAST(CASE WHEN ((SELECT o.type FROM sys.objects o WHERE o.object_id = ki.object_id) = 'U') THEN CASE WHEN ((SELECT tbl.is_memory_optimized FROM sys.tables tbl WHERE tbl.object_id = ki.object_id) = 1) THEN 1 ELSE 0 END ELSE CASE WHEN ((SELECT tt.is_memory_optimized FROM sys.table_types tt WHERE tt.type_table_object_id = ki.object_id) = 1) THEN 1 ELSE 0 END END AS bit) = 0) and ((tbl.name = '{}' and SCHEMA_NAME(tbl.schema_id) = N'dbo')) ORDER BY [Name] ASC",
        orders_table
    );

    compare_after_setup_owned(&setup_sqls, &sql).await;
}

#[tokio::test]
#[ignore]
async fn test_compare_designer_partition_schemes_filegroups_probe() {
    compare(
        "select sch.name, sch.data_space_id, sch.function_id, dest.destination_id, fg.name as fg_name, fg.type from sys.partition_schemes sch inner join (sys.destination_data_spaces dest inner join sys.filegroups fg on fg.data_space_id = dest.data_space_id) on dest.partition_scheme_id = sch.data_space_id order by sch.data_space_id, dest.destination_id",
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn test_compare_database_identity_functions() {
    compare(
        "SELECT DB_NAME() AS current_db, DB_ID() AS current_db_id, \
                DB_NAME(1) AS master_name, DB_ID('master') AS master_id, \
                DB_NAME(2) AS tempdb_name, DB_ID('tempdb') AS tempdb_id, \
                DB_NAME(3) AS model_name, DB_ID('model') AS model_id, \
                DB_NAME(4) AS msdb_name, DB_ID('msdb') AS msdb_id",
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn test_compare_database_metadata_probes() {
    compare(
        "SELECT \
            name, database_id, state_desc, compatibility_level, recovery_model_desc \
         FROM sys.databases \
         WHERE name IN ('master', 'tempdb', 'model', 'msdb') \
         ORDER BY database_id",
    )
    .await;

    compare(
        "SELECT \
            CAST(DATABASEPROPERTYEX('master', 'Status') AS nvarchar(20)) AS master_status, \
            CAST(DATABASEPROPERTYEX('master', 'Recovery') AS nvarchar(20)) AS master_recovery, \
            CAST(DATABASEPROPERTYEX('tempdb', 'Recovery') AS nvarchar(20)) AS tempdb_recovery, \
            CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS nvarchar(128)) AS current_collation",
    )
    .await;

    compare(
        "SELECT \
            HAS_DBACCESS('master') AS has_master, \
            HAS_DBACCESS('tempdb') AS has_tempdb, \
            HAS_DBACCESS('model') AS has_model, \
            HAS_DBACCESS('msdb') AS has_msdb",
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn test_compare_session_and_connection_probes() {
    compare(
        "SELECT database_id, authenticating_database_id \
         FROM sys.dm_exec_sessions \
         WHERE session_id = @@SPID",
    )
    .await;

    compare(
        "SELECT status, database_id \
         FROM sys.dm_exec_requests \
         WHERE session_id = @@SPID",
    )
    .await;
}

#[tokio::test]
#[ignore]
async fn test_compare_contained_auth_probe_after_use() {
    compare_after_setup(
        &["USE iridium_sql"],
        "SELECT CASE WHEN authenticating_database_id = 1 THEN 0 ELSE 1 END \
         FROM sys.dm_exec_sessions \
         WHERE session_id = @@SPID",
    )
    .await;
}
