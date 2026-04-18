//! Discovery test: queries SQL Server for all sys.* view columns
//! and compares with Iridium's implementation.
//!
//! Run with: cargo test -p iridium_core test_sys_views_column_discovery -- --ignored

mod discovery {
    use iridium_core::{parse_sql, types::Value, Engine};
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

    /// Get all columns for a given sys view from SQL Server
    async fn get_ss_columns(view_name: &str) -> Vec<(String, String)> {
        let mut client = get_sqlserver_client().await;
        let query = format!(
            "SELECT c.name, t.name AS type_name
             FROM sys.columns c
             JOIN sys.types t ON c.user_type_id = t.user_type_id
             WHERE c.object_id = OBJECT_ID('sys.{}')
             ORDER BY c.column_id",
            view_name
        );
        let stream = client
            .query(&query, &[])
            .await
            .expect("SQL Server query failed");
        let rows = stream
            .into_first_result()
            .await
            .expect("Failed to read results");

        rows.iter()
            .filter_map(|r| {
                let col_name = r.try_get::<&str, _>(0).ok().flatten()?.to_string();
                let type_name = r.try_get::<&str, _>(1).ok().flatten()?.to_string();
                Some((col_name, type_name))
            })
            .collect()
    }

    /// Get all columns for a given sys view from Iridium
    fn get_iridium_columns(view_name: &str) -> Vec<(String, String)> {
        let engine = Engine::new();
        let query2 = format!(
            "SELECT c.name, ISNULL(CAST(t.name AS VARCHAR(30)), 'unknown') AS type_name
             FROM sys.columns c
             LEFT JOIN sys.types t ON c.system_type_id = t.system_type_id
             WHERE c.object_id = OBJECT_ID('sys.{}')
             ORDER BY c.column_id",
            view_name
        );

        let stmt = parse_sql(&query2).expect("parse failed");
        let result = engine
            .execute(stmt)
            .expect("execute failed")
            .expect("expected result");

        result
            .rows
            .iter()
            .filter_map(|r| {
                let col_name = match &r[0] {
                    Value::VarChar(s) | Value::NVarChar(s) => s.clone(),
                    _ => return None,
                };
                let type_name = match &r[1] {
                    Value::VarChar(s) | Value::NVarChar(s) => s.clone(),
                    Value::Null => "unknown".to_string(),
                    _ => "unknown".to_string(),
                };
                Some((col_name, type_name))
            })
            .collect()
    }

    /// Full list of sys.* views to compare
    const SYS_VIEWS: &[&str] = &[
        "tables",
        "columns",
        "all_columns",
        "indexes",
        "index_columns",
        "foreign_keys",
        "foreign_key_columns",
        "objects",
        "all_objects",
        "schemas",
        "types",
        "data_spaces",
        "filegroups",
        "partitions",
        "allocation_units",
        "partition_functions",
        "partition_schemes",
        "destination_data_spaces",
        "stats",
        "stats_columns",
        "change_tracking_tables",
        "fulltext_indexes",
        "fulltext_catalogs",
        "extended_properties",
        "parameters",
        "procedures",
        "functions",
        "routines",
        "sql_modules",
        "views",
        "view_columns",
        "triggers",
        "trigger_events",
        "synonyms",
        "sequences",
        "table_types",
        "identity_columns",
        "computed_columns",
        "check_constraints",
        "key_constraints",
        "default_constraints",
        "database_principals",
        "database_permissions",
        "database_role_members",
        "server_principals",
        "databases",
        "sysdatabases",
        "configurations",
        "master_files",
        "database_files",
        "database_mirroring",
        "availability_replicas",
        "availability_groups",
        "dm_hadr_database_replica_states",
        "dm_exec_sessions",
        "dm_exec_requests",
        "dm_exec_connections",
        "dm_os_host_info",
        "dm_os_sys_info",
        "dm_db_index_usage_stats",
        "dm_db_partition_stats",
        "dm_db_index_physical_stats",
        "system_views",
        "sql_expression_dependencies",
    ];

    #[tokio::test]
    #[ignore] // Requires running Podman with SQL Server
    async fn test_sys_views_column_discovery() {
        println!("\n{:=<80}", "");
        println!("  SYS VIEWS COLUMN DISCOVERY REPORT");
        println!("{:=<80}\n", "");

        let mut total_ss = 0;
        let mut total_iridium = 0;
        let mut total_missing = 0;
        let mut total_extra = 0;

        for view in SYS_VIEWS {
            let ss_cols = get_ss_columns(view).await;
            let iridium_cols = get_iridium_columns(view);

            // Skip if Iridium doesn't have the view
            if iridium_cols.is_empty() && ss_cols.is_empty() {
                continue;
            }

            let ss_names: std::collections::HashSet<_> =
                ss_cols.iter().map(|(n, _)| n.to_lowercase()).collect();
            let iridium_names: std::collections::HashSet<_> = iridium_cols
                .iter()
                .map(|(n, _)| n.to_lowercase())
                .collect();

            let missing: Vec<_> = ss_names.difference(&iridium_names).collect();
            let extra: Vec<_> = iridium_names.difference(&ss_names).collect();

            total_ss += ss_cols.len();
            total_iridium += iridium_cols.len();
            total_missing += missing.len();
            total_extra += extra.len();

            if !missing.is_empty() || !extra.is_empty() {
                println!("\n--- sys.{} ---", view);
                println!(
                    "  SQL Server: {} columns | Iridium: {} columns",
                    ss_cols.len(),
                    iridium_cols.len()
                );

                if !missing.is_empty() {
                    println!("  MISSING in Iridium:");
                    for m in &missing {
                        let ss_type = ss_cols
                            .iter()
                            .find(|(n, _)| n.eq_ignore_ascii_case(m))
                            .map(|(_, t)| t.as_str())
                            .unwrap_or("?");
                        println!("    - {} ({})", m, ss_type);
                    }
                }

                if !extra.is_empty() {
                    println!("  EXTRA in Iridium:");
                    for e in &extra {
                        let ir_type = iridium_cols
                            .iter()
                            .find(|(n, _)| n.eq_ignore_ascii_case(e))
                            .map(|(_, t)| t.as_str())
                            .unwrap_or("?");
                        println!("    + {} ({})", e, ir_type);
                    }
                }
            }
        }

        println!("\n{:=<80}", "");
        println!("  SUMMARY");
        println!("{:=<80}", "");
        println!("  Total SQL Server columns: {}", total_ss);
        println!("  Total Iridium columns:    {}", total_iridium);
        println!("  Missing in Iridium:       {}", total_missing);
        println!("  Extra in Iridium:         {}", total_extra);
        println!("{:=<80}\n", "");
    }

    /// Specifically check the columns used in the SSMS Design Mode query
    #[tokio::test]
    #[ignore] // Requires running Podman with SQL Server
    async fn test_ssms_design_columns_exist() {
        // These are the exact columns the SSMS Design Mode query references
        let design_queries: Vec<(&str, Vec<&str>)> = vec![
            (
                "tables",
                vec![
                    "object_id",
                    "name",
                    "schema_id",
                    "principal_id",
                    "parent_object_id",
                    "type",
                    "type_desc",
                    "create_date",
                    "modify_date",
                    "is_memory_optimized",
                    "is_ms_shipped",
                    "is_filetable",
                    "temporal_type",
                    "is_external",
                    "is_node",
                    "is_edge",
                    "ledger_type",
                    "is_dropped_ledger_table",
                    "durability",
                    "durability_desc",
                    "history_table_id",
                    "is_replicated",
                    "lock_escalation_desc",
                    "lob_data_space_id",
                    "filestream_data_space_id",
                ],
            ),
            (
                "change_tracking_tables",
                vec![
                    "object_id",
                    "is_track_columns_updated_on",
                ],
            ),
            (
                "data_spaces",
                vec![
                    "data_space_id",
                    "name",
                    "type",
                ],
            ),
            (
                "fulltext_indexes",
                vec![
                    "object_id",
                    "fulltext_catalog_id",
                ],
            ),
            (
                "fulltext_catalogs",
                vec![
                    "fulltext_catalog_id",
                    "name",
                ],
            ),
            (
                "indexes",
                vec![
                    "object_id",
                    "name",
                    "index_id",
                    "type",
                    "type_desc",
                    "is_unique",
                    "is_primary_key",
                    "data_space_id",
                ],
            ),
        ];

        println!("\n{:=<80}", "");
        println!("  SSMS DESIGN MODE COLUMN CHECK");
        println!("{:=<80}\n", "");

        let mut all_pass = true;

        for (view, columns) in &design_queries {
            let ss_cols = get_ss_columns(view).await;
            let ss_names: std::collections::HashSet<_> =
                ss_cols.iter().map(|(n, _)| n.to_lowercase()).collect();

            for col in columns {
                let exists = ss_names.contains(&col.to_lowercase());
                let status = if exists { "✅" } else { "❌" };
                println!("  {} sys.{}.{}", status, view, col);
                if !exists {
                    all_pass = false;
                }
            }
        }

        println!("\n{:=<80}\n", "");

        assert!(all_pass, "Some SSMS Design Mode columns are missing in SQL Server");
    }
}
