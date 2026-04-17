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
