use iridium_core::{parse_sql, types::Value, Engine};

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    let stmt = parse_sql(sql).expect("parse failed");
    engine
        .execute(stmt)
        .expect("execute failed")
        .expect("expected result")
}

#[test]
fn test_qualified_wildcard_parse() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT t.* FROM (SELECT 1 as a) AS t");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.columns, vec!["a"]);
    assert_eq!(r.rows[0][0], Value::Int(1));
}

#[test]
fn test_values_subquery_cross_apply() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "
        SELECT t.*
        FROM (SELECT 1 as id) as base
        CROSS APPLY (
            VALUES (1, 'foo'), (2, 'bar')
        ) AS t(col1, col2)
        ORDER BY t.col1
    ",
    );

    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.columns, vec!["col1", "col2"]);
    assert_eq!(r.rows[0][0], Value::Int(1));
    assert_eq!(r.rows[0][1], Value::VarChar("foo".to_string()));
    assert_eq!(r.rows[1][0], Value::Int(2));
    assert_eq!(r.rows[1][1], Value::VarChar("bar".to_string()));
}

#[test]
fn test_nested_parentheses_with_and_after_inner_group() {
    let sql = "
        SELECT 1
        FROM sys.types AS baset
        WHERE (baset.system_type_id = 1)
           OR ((baset.system_type_id = 2) AND (baset.user_type_id = 2) AND (baset.is_user_defined = 0))
    ";
    let parsed = parse_sql(sql);
    assert!(parsed.is_ok(), "parse failed: {:?}", parsed.err());
}

#[test]
fn test_ssms_partition_functions_join_group_parse() {
    let sql = "select func.name, func.function_id, func.type, func.fanout, func.boundary_value_on_right, para.parameter_id, tp.name as type_name, convert(smallint, case when (tp.name = N'nchar' or tp.name = N'nvarchar') then para.max_length / 2 else para.max_length end) as max_length, para.precision, para.scale, para.collation_name from sys.partition_functions func left outer join (sys.partition_parameters para join sys.types tp on tp.user_type_id = para.system_type_id) on para.function_id = func.function_id order by func.function_id, para.parameter_id";
    let parsed = parse_sql(sql);
    assert!(parsed.is_ok(), "parse failed: {:?}", parsed.err());
}

#[test]
fn test_ssms_partition_schemes_join_group_parse() {
    let sql = "select sch.name, sch.data_space_id, sch.function_id, dest.destination_id, fg.name as fg_name, fg.type from sys.partition_schemes sch inner join (sys.destination_data_spaces dest inner join sys.filegroups fg on fg.data_space_id = dest.data_space_id) on dest.partition_scheme_id = sch.data_space_id order by sch.data_space_id, dest.destination_id";
    let parsed = parse_sql(sql);
    assert!(parsed.is_ok(), "parse failed: {:?}", parsed.err());
}
