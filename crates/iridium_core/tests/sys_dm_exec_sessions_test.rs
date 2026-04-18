#[cfg(test)]
mod tests {
    use iridium_core::{parse_sql, Engine, types::Value};

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

    #[test]
    fn test_use_switches_current_database_without_changing_auth_db() {
        let engine = Engine::new();

        engine
            .exec("USE iridium_sql")
            .expect("USE iridium_sql failed");

        let stmt = parse_sql(
            "SELECT DB_NAME() AS current_db, DB_ID() AS current_db_id, database_id, authenticating_database_id \
             FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        )
        .unwrap();
        let result = engine.execute(stmt).unwrap().expect("Expected result");

        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row[0], Value::NVarChar("iridium_sql".to_string()));
        assert_eq!(row[1], Value::Int(5));
        assert_eq!(row[2], Value::SmallInt(5));
        assert_eq!(row[3], Value::Int(1));

        engine.exec("USE master").expect("USE master failed");

        let stmt = parse_sql(
            "SELECT DB_NAME() AS current_db, DB_ID() AS current_db_id, database_id, authenticating_database_id \
             FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        )
        .unwrap();
        let result = engine.execute(stmt).unwrap().expect("Expected result");
        let row = &result.rows[0];
        assert_eq!(row[0], Value::NVarChar("master".to_string()));
        assert_eq!(row[1], Value::Int(1));
        assert_eq!(row[2], Value::SmallInt(1));
        assert_eq!(row[3], Value::Int(1));
    }

    #[test]
    fn test_use_unknown_database_returns_sqlserver_style_error() {
        let engine = Engine::new();

        let err = engine
            .exec("USE does_not_exist")
            .expect_err("USE should fail for unknown database");
        assert!(
            err.to_string().contains(
                "Cannot open database 'does_not_exist' requested by the login. The login failed."
            ),
            "unexpected error: {err}"
        );
    }
}
