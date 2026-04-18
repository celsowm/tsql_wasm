#[cfg(test)]
mod tests {
    use iridium_core::{parse_sql, types::Value, Engine};

    #[test]
    fn test_dm_exec_sessions() {
        let engine = Engine::new();

        let sql = "SELECT session_id, login_name, status FROM sys.dm_exec_sessions";
        let stmt = parse_sql(sql).unwrap();
        let result = engine.execute(stmt).unwrap().expect("Expected result");

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        // Session ID in Engine::new() defaults to 1 for the first connection
        assert_eq!(row[0], Value::SmallInt(1));
        assert_eq!(row[1], Value::NVarChar("sa".to_string()));
        assert_eq!(row[2], Value::NVarChar("running".to_string()));
    }

    #[test]
    fn test_dm_exec_sessions_reports_master_database_ids() {
        let engine = Engine::new();

        let sql = "SELECT database_id, authenticating_database_id FROM sys.dm_exec_sessions WHERE session_id = @@SPID";
        let stmt = parse_sql(sql).unwrap();
        let result = engine.execute(stmt).unwrap().expect("Expected result");

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row[0], Value::SmallInt(1));
        assert_eq!(row[1], Value::Int(1));
    }

    #[test]
    fn test_db_identity_resolves_from_shared_catalog() {
        let engine = Engine::new();

        let stmt = parse_sql("SELECT DB_NAME(5) AS db_name, DB_ID('tempdb') AS db_id").unwrap();
        let result = engine.execute(stmt).unwrap().expect("Expected result");

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row[0], Value::NVarChar("iridium_sql".to_string()));
        assert_eq!(row[1], Value::Int(2));
    }

    #[test]
    fn test_dm_exec_connections() {
        let engine = Engine::new();

        let sql = "SELECT session_id, net_transport, auth_scheme FROM sys.dm_exec_connections";
        let stmt = parse_sql(sql).unwrap();
        let result = engine.execute(stmt).unwrap().expect("Expected result");

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row[0], Value::Int(1));
        assert_eq!(row[1], Value::NVarChar("TCP".to_string()));
        assert_eq!(row[2], Value::NVarChar("SQL".to_string()));
    }
}
