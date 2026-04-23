use iridium_core::{parse_sql, types::Value, Engine, QueryResult};

fn query(engine: &mut Engine, sql: &str) -> Result<QueryResult, String> {
    let stmt = parse_sql(sql).map_err(|e| format!("parse failed: {}", e))?;
    engine
        .execute(stmt)
        .map_err(|e| format!("execute failed: {}", e))?
        .ok_or_else(|| "expected result".to_string())
}

#[test]
fn test_ssms_databases_alias_resolution() {
    let mut engine = Engine::new();
    
    // This query mimics what SSMS sends. 
    // It uses a 3-part name (though here we just use 2-part since we haven't updated the parser yet)
    // and an alias 'dtb'.
    // The columns 'containment' and 'catalog_collation_type' are often missing or cause issues.
    let sql = "
        SELECT 
            dtb.name AS [DatabaseName],
            dtb.database_id,
            dtb.containment
        FROM sys.databases AS dtb
        WHERE dtb.name = 'master'
    ";
    
    let result = query(&mut engine, sql);
    
    // Currently, this might fail because 'containment' is in sys.databases but let's check a missing one too
    assert!(result.is_ok(), "Query failed: {:?}", result.err());
    let rows = result.unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::VarChar("master".to_string()));
}

#[test]
fn test_ssms_3_part_name_parsing() {
    let mut engine = Engine::new();
    
    // Test that we can parse and execute a 3-part name query
    let sql = "SELECT name FROM master.sys.databases WHERE name = 'master'";
    
    let result = query(&mut engine, sql);
    assert!(result.is_ok(), "3-part name query failed: {:?}", result.err());
    let rows = result.unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::VarChar("master".to_string()));
}

#[test]
fn test_ssms_object_property_stubs() {
    let mut engine = Engine::new();
    
    // Probing properties that SSMS expects
    let sql = "SELECT OBJECTPROPERTY(OBJECT_ID('sys.databases'), 'TableTextInRowLimit'), OBJECTPROPERTY(OBJECT_ID('sys.databases'), 'IsIndexable')";
    
    let result = query(&mut engine, sql);
    assert!(result.is_ok(), "OBJECTPROPERTY query failed: {:?}", result.err());
    let rows = result.unwrap().rows;
    assert_eq!(rows[0][0], Value::Int(0)); // TableTextInRowLimit
    assert_eq!(rows[0][1], Value::Int(1)); // IsIndexable (tables/views usually indexable or return 1 by default now)
}

#[test]
fn test_ssms_msdb_function_stub() {
    let mut engine = Engine::new();
    
    // Probing msdb function that SSMS uses for policy health
    let sql = "SELECT msdb.dbo.fn_syspolicy_is_automation_enabled()";
    
    let result = query(&mut engine, sql);
    assert!(result.is_ok(), "msdb function query failed: {:?}", result.err());
    let rows = result.unwrap().rows;
    assert_eq!(rows[0][0], Value::Int(1));
}

#[test]
fn test_ssms_object_id_3_part() {
    let mut engine = Engine::new();
    
    // Test OBJECT_ID with 3 parts and brackets
    let sql = "SELECT OBJECT_ID(N'[master].[sys].[databases]')";
    let result = query(&mut engine, sql);
    assert!(result.is_ok());
    let rows = result.unwrap().rows;
    assert!(!rows[0][0].is_null());
}

#[test]
fn test_new_sys_views_visibility() {
    let mut engine = Engine::new();
    
    // Verify assembly_types
    let sql = "SELECT * FROM sys.assembly_types";
    let result = query(&mut engine, sql);
    assert!(result.is_ok(), "assembly_types failed: {:?}", result.err());
    
    // Verify fulltext_index_columns
    let sql = "SELECT * FROM sys.fulltext_index_columns";
    let result = query(&mut engine, sql);
    assert!(result.is_ok(), "fulltext_index_columns failed: {:?}", result.err());
}

#[test]
fn test_sys_columns_extended() {
    let mut engine = Engine::new();
    
    // Verify new columns in sys.columns
    let sql = "SELECT is_filestream, is_identity, is_ansi_padded FROM sys.columns";
    let result = query(&mut engine, sql);
    assert!(result.is_ok(), "sys.columns extended failed: {:?}", result.err());
}
