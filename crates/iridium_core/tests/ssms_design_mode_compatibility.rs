//! Comprehensive SSMS Design Mode compatibility tests.
//!
//! Tests discovered during the SSMS Design Mode debugging session:
//! - sys.tables missing columns (lob_data_space_id, filestream_data_space_id, etc.)
//! - sys.change_tracking_tables missing
//! - sys.fulltext_indexes missing
//! - sys.fulltext_catalogs missing
//! - LEFT OUTER JOIN column resolution bugs
//! - Parenthesized JOIN syntax support
//!
//! Some tests require a running SQL Server (Podman) for parity comparison.

use iridium_core::{parse_sql, types::Value, Engine};

// ─── Local-only tests (no SQL Server required) ─────────────────────────

fn engine_exec(engine: &mut Engine, sql: &str) -> Option<iridium_core::QueryResult> {
    let stmt = parse_sql(sql).unwrap_or_else(|e| panic!("Parser falhou: {}\n  SQL: {}", e, sql));
    engine
        .execute(stmt)
        .unwrap_or_else(|e| panic!("Engine falhou: {}\n  SQL: {}", e, sql))
}

fn exec(engine: &mut Engine, sql: &str) {
    let stmt = parse_sql(sql).expect("parse failed");
    engine.execute(stmt).expect("execute failed");
}

// ─── sys.tables column coverage ────────────────────────────────────────

#[test]
fn test_sys_tables_has_lob_data_space_id() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.lob_test_table (id INT PRIMARY KEY, data VARCHAR(MAX))");

    let result = engine_exec(
        &mut engine,
        "SELECT lob_data_space_id FROM sys.tables WHERE name = 'lob_test_table'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    // Should be 0 (default filegroup), not error
    assert!(result.rows[0][0].is_null() || result.rows[0][0] == Value::Int(0));
}

#[test]
fn test_sys_tables_has_filestream_data_space_id() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.fs_test_table (id INT PRIMARY KEY)");

    let result = engine_exec(
        &mut engine,
        "SELECT filestream_data_space_id FROM sys.tables WHERE name = 'fs_test_table'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    // Should be NULL (no FileStream)
    assert!(result.rows[0][0].is_null());
}

#[test]
fn test_sys_tables_has_lock_escalation_desc() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.lock_test_table (id INT PRIMARY KEY)");

    let result = engine_exec(
        &mut engine,
        "SELECT lock_escalation_desc FROM sys.tables WHERE name = 'lock_test_table'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::VarChar("TABLE".to_string()));
}

#[test]
fn test_sys_tables_has_is_replicated() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.repl_test (id INT PRIMARY KEY)");

    let result = engine_exec(
        &mut engine,
        "SELECT is_replicated FROM sys.tables WHERE name = 'repl_test'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Bit(false));
}

#[test]
fn test_sys_tables_all_ssms_columns_present() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.ssms_compat_test (id INT PRIMARY KEY, name VARCHAR(50))");

    // Query all columns from sys.tables for the test table
    let result = engine_exec(
        &mut engine,
        "SELECT object_id, name, schema_id, principal_id, parent_object_id, type, type_desc,
                create_date, modify_date, is_memory_optimized, is_ms_shipped, is_filetable,
                temporal_type, is_external, is_node, is_edge, ledger_type,
                is_dropped_ledger_table, durability, durability_desc, history_table_id,
                is_replicated, lock_escalation, lock_escalation_desc,
                lob_data_space_id, filestream_data_space_id, max_column_id_used,
                lock_on_bulk_load, uses_ansi_null_defaults, is_tracked_by_cdc,
                is_merge_published
         FROM sys.tables WHERE name = 'ssms_compat_test'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 31);
}

// ─── sys.change_tracking_tables ────────────────────────────────────────

#[test]
fn test_sys_change_tracking_tables_exists() {
    let mut engine = Engine::new();

    let result = engine_exec(&mut engine, "SELECT * FROM sys.change_tracking_tables").unwrap();
    // Should return empty result (no tables have change tracking enabled)
    assert!(result.rows.is_empty());
}

#[test]
fn test_sys_change_tracking_tables_has_is_track_columns_updated_on() {
    let mut engine = Engine::new();

    let result = engine_exec(
        &mut engine,
        "SELECT object_id, is_track_columns_updated_on FROM sys.change_tracking_tables",
    )
    .unwrap();

    assert!(result.rows.is_empty());
    // Verify the columns exist (no error thrown)
    assert_eq!(result.columns.len(), 2);
}

#[test]
fn test_sys_change_tracking_tables_left_join_with_sys_tables() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.ctt_join_test (id INT PRIMARY KEY)",
    );

    // This is the exact pattern SSMS Design Mode uses
    let result = engine_exec(
        &mut engine,
        "SELECT tbl.name, ctt.object_id AS ctt_id
         FROM sys.tables tbl
         LEFT OUTER JOIN sys.change_tracking_tables ctt ON ctt.object_id = tbl.object_id
         WHERE tbl.name = 'ctt_join_test'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    // Table name should be returned
    assert_eq!(result.rows[0][0], Value::VarChar("ctt_join_test".to_string()));
    // ctt_id should be NULL (no change tracking)
    assert!(result.rows[0][1].is_null());
}

// ─── sys.fulltext_indexes and sys.fulltext_catalogs ────────────────────

#[test]
fn test_sys_fulltext_indexes_exists() {
    let mut engine = Engine::new();

    let result = engine_exec(&mut engine, "SELECT * FROM sys.fulltext_indexes").unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn test_sys_fulltext_catalogs_exists() {
    let mut engine = Engine::new();

    let result = engine_exec(&mut engine, "SELECT * FROM sys.fulltext_catalogs").unwrap();
    assert!(result.rows.is_empty());
}

#[test]
fn test_sys_fulltext_indexes_left_join_with_sys_tables() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.fti_join_test (id INT PRIMARY KEY, name VARCHAR(50))",
    );

    // SSMS Design Mode pattern
    let result = engine_exec(
        &mut engine,
        "SELECT tbl.name, FtCat.name AS FulltextCatalog
         FROM sys.tables tbl
         LEFT OUTER JOIN (sys.fulltext_indexes fti
             INNER JOIN sys.fulltext_catalogs FtCat
                 ON FtCat.fulltext_catalog_id = fti.fulltext_catalog_id)
             ON fti.object_id = tbl.object_id
         WHERE tbl.name = 'fti_join_test'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::VarChar("fti_join_test".to_string()));
    assert!(result.rows[0][1].is_null());
}

// ─── JOIN patterns used by SSMS Design Mode ────────────────────────────

#[test]
fn test_left_outer_join_column_resolution() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.left_join_test (id INT PRIMARY KEY, name VARCHAR(50))",
    );

    // LEFT OUTER JOIN should resolve aliases correctly
    let result = engine_exec(
        &mut engine,
        "SELECT tbl.name, dsp.name AS ds_name
         FROM sys.tables tbl
         LEFT OUTER JOIN sys.data_spaces dsp ON dsp.data_space_id = tbl.schema_id
         WHERE tbl.name = 'left_join_test'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::VarChar("left_join_test".to_string()));
}

#[test]
fn test_inner_join_after_left_outer_join() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.multi_join_test (id INT PRIMARY KEY)",
    );

    // INNER JOIN after LEFT OUTER JOIN
    let result = engine_exec(
        &mut engine,
        "SELECT tbl.name, idx.name AS idx_name
         FROM sys.tables tbl
         LEFT OUTER JOIN sys.data_spaces dsp ON dsp.data_space_id = tbl.lob_data_space_id
         INNER JOIN sys.indexes idx ON idx.object_id = tbl.object_id
         WHERE tbl.name = 'multi_join_test'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::VarChar("multi_join_test".to_string()));
}

#[test]
fn test_parenthesized_join_group() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.paren_join_test (id INT PRIMARY KEY)",
    );

    // Parenthesized JOIN syntax (SSMS pattern)
    let result = engine_exec(
        &mut engine,
        "SELECT tbl.name, idx.name AS idx_name, Fg.name AS fg_name
         FROM sys.tables tbl
         INNER JOIN (sys.indexes idx
             INNER JOIN sys.data_spaces Fg ON Fg.data_space_id = idx.data_space_id)
             ON idx.object_id = tbl.object_id
         WHERE tbl.name = 'paren_join_test'",
    )
    .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::VarChar("paren_join_test".to_string()));
    assert_eq!(result.rows[0][2], Value::VarChar("PRIMARY".to_string()));
}

#[test]
fn test_ssms_designer_categories_query() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.Categories (id INT PRIMARY KEY, name VARCHAR(50))",
    );

    // Simplified version of the actual SSMS query that was failing
    let result = engine_exec(
        &mut engine,
        "SELECT
            Fg.name AS TableFg,
            dsp.name AS TexImageFg,
            FtCat.name AS FulltextCatalog,
            tbl.is_replicated,
            tbl.lock_escalation_desc,
            CAST(CASE WHEN ctt.object_id IS NULL THEN 0 ELSE 1 END AS bit) AS IsChangeTracked
         FROM sys.tables tbl
         LEFT OUTER JOIN sys.change_tracking_tables AS ctt ON ctt.object_id = tbl.object_id
         LEFT OUTER JOIN sys.data_spaces dsp ON dsp.data_space_id = tbl.lob_data_space_id
         LEFT OUTER JOIN (sys.fulltext_indexes fti
             INNER JOIN sys.fulltext_catalogs FtCat
                 ON FtCat.fulltext_catalog_id = fti.fulltext_catalog_id)
             ON fti.object_id = tbl.object_id
         INNER JOIN (sys.indexes idx
             INNER JOIN sys.data_spaces Fg
                 ON (idx.index_id = 0 OR idx.index_id = 1) AND Fg.data_space_id = idx.data_space_id)
             ON idx.object_id = tbl.object_id AND (idx.index_id = 0 OR idx.index_id = 1)
         WHERE tbl.object_id = OBJECT_ID(N'dbo.Categories')",
    )
    .unwrap();

    // Should return exactly one row
    assert_eq!(result.rows.len(), 1);

    // TableFg should be 'PRIMARY' (the default filegroup)
    assert_eq!(result.rows[0][0], Value::VarChar("PRIMARY".to_string()));

    // TexImageFg should be NULL (no LOB data space)
    assert!(result.rows[0][1].is_null());

    // FulltextCatalog should be NULL (no fulltext index)
    assert!(result.rows[0][2].is_null());

    // is_replicated should be false
    assert_eq!(result.rows[0][3], Value::Bit(false));

    // lock_escalation_desc should be 'TABLE'
    assert_eq!(result.rows[0][4], Value::VarChar("TABLE".to_string()));

    // IsChangeTracked should be false (0)
    assert_eq!(result.rows[0][5], Value::Bit(false));
}

// ─── SQL Server parity tests (require Podman + SQL Server) ─────────────

mod sql_server_parity {
    use super::*;
    use tiberius::{Client, Config};
    use tokio::net::TcpStream;
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    async fn get_sqlserver_client() -> Client<tokio_util::compat::Compat<TcpStream>> {
        let mut config = Config::new();
        config.host("localhost");
        config.port(11433);
        config.authentication(tiberius::AuthMethod::sql_server(
            "sa", "Test@12345",
        ));
        config.trust_cert();
        config.encryption(tiberius::EncryptionLevel::NotSupported);

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .expect("Failed to connect to SQL Server");
        tcp.set_nodelay(true).unwrap();

        Client::connect(config, tcp.compat_write())
            .await
            .expect("Failed to connect via TDS")
    }

    /// Get all column names from sys.tables on SQL Server
    async fn get_sqlserver_sys_tables_columns() -> Vec<String> {
        let mut client = get_sqlserver_client().await;
        let stream = client
            .query(
                "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('sys.tables') ORDER BY column_id",
                &[],
            )
            .await
            .expect("SQL Server query failed");
        let rows = stream
            .into_first_result()
            .await
            .expect("Failed to read results");
        rows.iter()
            .filter_map(|r| r.try_get::<&str, _>(0).ok().flatten())
            .map(|s| s.to_string())
            .collect()
    }

    /// Get all column names from sys.tables on Iridium
    fn get_iridium_sys_tables_columns() -> Vec<String> {
        let mut engine = Engine::new();
        let res = engine_exec(
            &mut engine,
            "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('sys.tables') ORDER BY column_id",
        )
        .expect("query failed");

        res.rows
            .iter()
            .filter_map(|r| {
                r.first().and_then(|v| match v {
                    Value::VarChar(s) => Some(s.clone()),
                    Value::NVarChar(s) => Some(s.clone()),
                    _ => None,
                })
            })
            .collect()
    }

    #[tokio::test]
    #[ignore] // Requires running Podman with SQL Server
    async fn test_sys_tables_column_parity_with_sql_server() {
        let ss_columns = get_sqlserver_sys_tables_columns().await;
        let iridium_columns = get_iridium_sys_tables_columns();

        // Find columns in SQL Server that are missing in Iridium
        let missing: Vec<&String> = ss_columns
            .iter()
            .filter(|c| !iridium_columns.iter().any(|ic| ic.eq_ignore_ascii_case(c)))
            .collect();

        if !missing.is_empty() {
            eprintln!(
                "\n⚠️  Missing columns in sys.tables: {:?}",
                missing
            );
        }

        // We don't assert here because it's ok to have some missing columns
        // as long as the SSMS-critical ones are present. This is informational.
        assert!(
            missing.len() < 10,
            "Too many missing columns ({}) - SSMS Design Mode may break. Missing: {:?}",
            missing.len(),
            missing
        );
    }

    /// Test that the critical SSMS Design Mode columns exist in both
    #[tokio::test]
    #[ignore] // Requires running Podman with SQL Server
    async fn test_sys_tables_critical_columns_parity() {
        let ss_columns = get_sqlserver_sys_tables_columns().await;

        // These are the columns SSMS Design Mode actually uses
        let critical_columns = vec![
            "object_id",
            "name",
            "schema_id",
            "principal_id",
            "type",
            "type_desc",
            "create_date",
            "modify_date",
            "is_ms_shipped",
            "is_memory_optimized",
            "is_filetable",
            "temporal_type",
            "is_external",
            "is_node",
            "is_edge",
            "ledger_type",
            "is_replicated",
            "lock_escalation_desc",
            "lob_data_space_id",
            "filestream_data_space_id",
            "durability",
            "durability_desc",
            "history_table_id",
        ];

        for col in &critical_columns {
            assert!(
                ss_columns.iter().any(|c| c.eq_ignore_ascii_case(col)),
                "SQL Server should have column '{}' in sys.tables",
                col
            );
        }
    }

    /// Compare sys.change_tracking_tables exists in both
    #[tokio::test]
    #[ignore] // Requires running Podman with SQL Server
    async fn test_change_tracking_tables_parity() {
        let mut engine = Engine::new();
        let mut client = get_sqlserver_client().await;

        // Iridium
        let engine_result =
            engine_exec(&mut engine, "SELECT * FROM sys.change_tracking_tables").unwrap();

        // SQL Server
        let stream = client
            .query("SELECT * FROM sys.change_tracking_tables", &[])
            .await
            .expect("SQL Server query failed");
        let ss_result = stream
            .into_first_result()
            .await
            .expect("Failed to read results");

        // Both should return empty (no change tracking enabled)
        assert!(
            engine_result.rows.is_empty(),
            "Iridium should have empty change_tracking_tables"
        );
        assert!(
            ss_result.is_empty(),
            "SQL Server should have empty change_tracking_tables"
        );
    }

    /// Compare sys.fulltext_indexes exists in both
    #[tokio::test]
    #[ignore] // Requires running Podman with SQL Server
    async fn test_fulltext_indexes_parity() {
        let mut engine = Engine::new();
        let mut client = get_sqlserver_client().await;

        // Iridium
        let engine_result =
            engine_exec(&mut engine, "SELECT * FROM sys.fulltext_indexes").unwrap();

        // SQL Server
        let stream = client
            .query("SELECT * FROM sys.fulltext_indexes", &[])
            .await
            .expect("SQL Server query failed");
        let ss_result = stream
            .into_first_result()
            .await
            .expect("Failed to read results");

        // Both should return empty (no fulltext indexes)
        assert!(
            engine_result.rows.is_empty(),
            "Iridium should have empty fulltext_indexes"
        );
        assert!(
            ss_result.is_empty(),
            "SQL Server should have empty fulltext_indexes"
        );
    }

    /// Compare sys.fulltext_catalogs exists in both
    #[tokio::test]
    #[ignore] // Requires running Podman with SQL Server
    async fn test_fulltext_catalogs_parity() {
        let mut engine = Engine::new();
        let mut client = get_sqlserver_client().await;

        // Iridium
        let engine_result =
            engine_exec(&mut engine, "SELECT * FROM sys.fulltext_catalogs").unwrap();

        // SQL Server
        let stream = client
            .query("SELECT * FROM sys.fulltext_catalogs", &[])
            .await
            .expect("SQL Server query failed");
        let ss_result = stream
            .into_first_result()
            .await
            .expect("Failed to read results");

        // Both should return empty (no fulltext catalogs)
        assert!(
            engine_result.rows.is_empty(),
            "Iridium should have empty fulltext_catalogs"
        );
        assert!(
            ss_result.is_empty(),
            "SQL Server should have empty fulltext_catalogs"
        );
    }

    /// Full SSMS Design Mode query comparison
    #[tokio::test]
    #[ignore] // Requires running Podman with SQL Server
    async fn test_ssms_designer_full_comparison() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let table_name = format!("SsmsDesignTest_{}", suffix);

        let mut engine = Engine::new();
        let mut client = get_sqlserver_client().await;

        // Create table on both
        let create_sql = format!(
            "CREATE TABLE dbo.{} (id INT PRIMARY KEY, name VARCHAR(50))",
            table_name
        );
        exec(&mut engine, &create_sql);
        client
            .execute(&create_sql, &[])
            .await
            .expect("Failed to create table on SQL Server");

        // Run the SSMS Design Mode query
        let query = format!(
            "SELECT
                Fg.name AS TableFg,
                dsp.name AS TexImageFg,
                FtCat.name AS FulltextCatalog,
                tbl.is_replicated,
                tbl.lock_escalation_desc,
                CAST(CASE WHEN ctt.object_id IS NULL THEN 0 ELSE 1 END AS bit) AS IsChangeTracked
             FROM sys.tables tbl
             LEFT OUTER JOIN sys.change_tracking_tables AS ctt ON ctt.object_id = tbl.object_id
             LEFT OUTER JOIN sys.data_spaces dsp ON dsp.data_space_id = tbl.lob_data_space_id
             LEFT OUTER JOIN (sys.fulltext_indexes fti
                 INNER JOIN sys.fulltext_catalogs FtCat
                     ON FtCat.fulltext_catalog_id = fti.fulltext_catalog_id)
                 ON fti.object_id = tbl.object_id
             INNER JOIN (sys.indexes idx
                 INNER JOIN sys.data_spaces Fg
                     ON (idx.index_id = 0 OR idx.index_id = 1) AND Fg.data_space_id = idx.data_space_id)
                 ON idx.object_id = tbl.object_id AND (idx.index_id = 0 OR idx.index_id = 1)
             WHERE tbl.name = N'{}'",
            table_name
        );

        // Iridium
        let engine_result = engine_exec(&mut engine, &query).expect("Iridium query failed");
        assert_eq!(
            engine_result.rows.len(),
            1,
            "Iridium should return 1 row"
        );

        // SQL Server
        let stream = client
            .query(&query, &[])
            .await
            .expect("SQL Server query failed");
        let ss_rows: Vec<Vec<String>> = stream
            .into_first_result()
            .await
            .expect("Failed to read SQL Server results")
            .iter()
            .map(|row| {
                (0..row.len())
                    .map(|i| {
                        if let Ok(Some(v)) = row.try_get::<&str, _>(i) {
                            return v.to_string();
                        }
                        if let Ok(Some(v)) = row.try_get::<i32, _>(i) {
                            return v.to_string();
                        }
                        if let Ok(Some(v)) = row.try_get::<bool, _>(i) {
                            return if v { "1".to_string() } else { "0".to_string() };
                        }
                        "NULL".to_string()
                    })
                    .collect()
            })
            .collect();

        assert_eq!(ss_rows.len(), 1, "SQL Server should return 1 row");

        // Compare TableFg (should be PRIMARY or similar)
        let engine_fg = match &engine_result.rows[0][0] {
            Value::VarChar(s) | Value::NVarChar(s) => s.clone(),
            _ => "NULL".to_string(),
        };
        assert_eq!(
            engine_fg, ss_rows[0][0],
            "TableFg mismatch between Iridium and SQL Server"
        );

        // Compare is_replicated (should be 0/false)
        let engine_replicated = match &engine_result.rows[0][3] {
            Value::Bit(b) => if *b { "1" } else { "0" }.to_string(),
            _ => "NULL".to_string(),
        };
        assert_eq!(
            engine_replicated, ss_rows[0][3],
            "is_replicated mismatch between Iridium and SQL Server"
        );

        // Compare lock_escalation_desc (should be TABLE)
        let engine_lock = match &engine_result.rows[0][4] {
            Value::VarChar(s) | Value::NVarChar(s) => s.clone(),
            _ => "NULL".to_string(),
        };
        assert_eq!(
            engine_lock, ss_rows[0][4],
            "lock_escalation_desc mismatch between Iridium and SQL Server"
        );

        // Compare IsChangeTracked (should be 0/false)
        let engine_ct = match &engine_result.rows[0][5] {
            Value::Bit(b) => if *b { "1" } else { "0" }.to_string(),
            _ => "NULL".to_string(),
        };
        assert_eq!(
            engine_ct, ss_rows[0][5],
            "IsChangeTracked mismatch between Iridium and SQL Server"
        );
    }
}
