use iridium_core::{types::Value, Engine};

fn exec(engine: &mut Engine, sql: &str) {
    engine.exec(sql).expect(sql);
}

fn query(engine: &mut Engine, sql: &str) -> iridium_core::QueryResult {
    engine.query(sql).expect(sql)
}

#[test]
fn throw_propagates_into_catch() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "BEGIN TRY THROW 50001, 'boom', 1 END TRY BEGIN CATCH SELECT ERROR_NUMBER() AS n, ERROR_SEVERITY() AS s, ERROR_STATE() AS st, ERROR_MESSAGE() AS msg END CATCH",
    );

    assert_eq!(
        r.rows[0],
        vec![
            Value::Int(50001),
            Value::Int(16),
            Value::Int(1),
            Value::VarChar("boom".to_string()),
        ]
    );
}

#[test]
fn greatest_and_least_choose_extrema() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT GREATEST(1, 5, 3) AS g, LEAST(1, 5, 3) AS l");

    assert_eq!(r.rows[0][0].to_integer_i64(), Some(5));
    assert_eq!(r.rows[0][1].to_integer_i64(), Some(1));
}

#[test]
fn string_split_with_ordinal_is_queryable() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT value, ordinal FROM STRING_SPLIT('a,b,c', ',', 1) ORDER BY ordinal",
    );

    assert_eq!(r.rows.len(), 3);
    assert_eq!(r.rows[0], vec![Value::VarChar("a".to_string()), Value::Int(1)]);
    assert_eq!(r.rows[1], vec![Value::VarChar("b".to_string()), Value::Int(2)]);
    assert_eq!(r.rows[2], vec![Value::VarChar("c".to_string()), Value::Int(3)]);
}

#[test]
fn alter_column_updates_metadata_and_retains_values() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE dbo.AlterColumnTest (v INT NOT NULL)",
    );
    exec(&mut engine, "INSERT INTO dbo.AlterColumnTest VALUES (1)");
    exec(
        &mut engine,
        "ALTER TABLE dbo.AlterColumnTest ALTER COLUMN v BIGINT NOT NULL",
    );

    let data = query(&mut engine, "SELECT v FROM dbo.AlterColumnTest");
    assert_eq!(data.rows[0][0].to_integer_i64(), Some(1));

    let meta = query(
        &mut engine,
        "SELECT DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'AlterColumnTest' AND COLUMN_NAME = 'v'",
    );
    assert_eq!(meta.rows[0][0], Value::VarChar("bigint".to_string()));
    assert_eq!(meta.rows[0][1].to_integer_i64(), Some(19));
    assert_eq!(meta.rows[0][2].to_integer_i64(), Some(0));
    assert_eq!(meta.rows[0][3], Value::VarChar("NO".to_string()));
}

#[test]
fn vector_cast_round_trips_and_reports_length() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CAST('[1,2,3]' AS VECTOR(3)) AS v, DATALENGTH(CAST('[1,2,3]' AS VECTOR(3))) AS bytes",
    );

    assert_eq!(r.rows[0][0].to_string_value(), "[1,2,3]");
    assert_eq!(r.rows[0][1].to_integer_i64(), Some(12));
}

#[test]
fn vector_distance_metrics_match_expected_values() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT VECTOR_DISTANCE('euclidean', CAST('[1,0]' AS VECTOR(2)), CAST('[0,0]' AS VECTOR(2))) AS e, VECTOR_DISTANCE('cosine', CAST('[1,0]' AS VECTOR(2)), CAST('[0,1]' AS VECTOR(2))) AS c, VECTOR_DISTANCE('dot', CAST('[1,2]' AS VECTOR(2)), CAST('[3,4]' AS VECTOR(2))) AS d",
    );

    assert_eq!(r.rows[0][0].to_string_value(), "1");
    assert_eq!(r.rows[0][1].to_string_value(), "1");
    assert_eq!(r.rows[0][2].to_string_value(), "-11");
}

#[test]
fn vector_null_and_dimension_mismatch_handling() {
    let mut engine = Engine::new();

    let null_row = query(&mut engine, "SELECT CAST(NULL AS VECTOR(3)) AS v");
    assert_eq!(null_row.rows[0][0], Value::Null);

    let err = engine.query("SELECT CAST('[1,2]' AS VECTOR(3))");
    assert!(err.is_err());
}

#[test]
fn vector_metadata_is_exposed_through_information_schema_and_sys_columns() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE dbo.VectorMetadataTest (id INT NOT NULL, embedding VECTOR(3) NOT NULL)",
    );

    let isc = query(
        &mut engine,
        "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'VectorMetadataTest' AND COLUMN_NAME = 'embedding'",
    );
    assert_eq!(isc.rows.len(), 1);
    assert_eq!(isc.rows[0][0], Value::VarChar("vector".to_string()));
    assert_eq!(isc.rows[0][1], Value::Null);
    assert_eq!(isc.rows[0][2], Value::Null);
    assert_eq!(isc.rows[0][3], Value::Null);
    assert_eq!(isc.rows[0][4], Value::Null);
    assert_eq!(isc.rows[0][5], Value::Null);

    let sys = query(
        &mut engine,
        "SELECT system_type_id, max_length, vector_dimensions, vector_base_type, vector_base_type_desc FROM sys.columns WHERE object_id = OBJECT_ID('dbo.VectorMetadataTest') AND name = 'embedding'",
    );
    assert_eq!(sys.rows.len(), 1);
    assert_eq!(sys.rows[0][0].to_integer_i64(), Some(242));
    assert_eq!(sys.rows[0][1].to_integer_i64(), Some(12));
    assert_eq!(sys.rows[0][2].to_integer_i64(), Some(3));
    assert_eq!(sys.rows[0][3].to_integer_i64(), Some(0));
    assert_eq!(sys.rows[0][4], Value::VarChar("float32".to_string()));
}
