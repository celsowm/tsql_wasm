use tsql_core::ast::{SessionOption, SessionOptionValue, Statement};
use tsql_core::types::Value;
use tsql_core::{parse_sql, Database, Engine, SupportStatus, SessionManager, StatementExecutor, SqlAnalyzer};

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_sql(sql).expect("parse")).expect("exec");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

#[test]
fn test_phase6_parser_set_options() {
    let stmts = [
        "SET ANSI_NULLS ON",
        "SET QUOTED_IDENTIFIER OFF",
        "SET NOCOUNT ON",
        "SET XACT_ABORT OFF",
        "SET DATEFIRST 1",
        "SET LANGUAGE us_english",
    ];
    for sql in stmts {
        let stmt = parse_sql(sql).expect("parse");
        assert!(matches!(stmt, Statement::SetOption(_)));
    }
}

#[test]
fn test_phase6_set_option_ast_values() {
    let s = parse_sql("SET DATEFIRST 9").unwrap();
    match s {
        Statement::SetOption(opt) => {
            assert_eq!(opt.option, SessionOption::DateFirst);
            assert_eq!(opt.value, SessionOptionValue::Int(9));
        }
        _ => panic!("expected set option"),
    }
}

#[test]
fn test_phase6_set_datefirst_out_of_range_error() {
    let mut engine = Engine::new();
    let err = engine
        .execute(parse_sql("SET DATEFIRST 9").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("DATEFIRST"));
    assert!(err.to_string().contains("outside the range"));
    assert!(err.to_string().contains("1-7"));
    
    let err2 = engine
        .execute(parse_sql("SET DATEFIRST 0").unwrap())
        .unwrap_err();
    assert!(err2.to_string().contains("DATEFIRST"));
    
    let err3 = engine
        .execute(parse_sql("SET DATEFIRST 8").unwrap())
        .unwrap_err();
    assert!(err3.to_string().contains("DATEFIRST"));
    
    exec(&mut engine, "SET DATEFIRST 1");
    exec(&mut engine, "SET DATEFIRST 7");
}

#[test]
fn test_phase6_xact_abort_rolls_back_transaction() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL)",
    );
    exec(&mut engine, "BEGIN TRANSACTION");
    exec(&mut engine, "SET XACT_ABORT ON");
    exec(&mut engine, "INSERT INTO t (id, v) VALUES (1, 10)");
    let err = engine
        .execute(parse_sql("INSERT INTO t (id, v) VALUES (1, 20)").unwrap())
        .unwrap_err();
    assert!(!err.to_string().is_empty());
    assert!(!engine.transaction_is_active());

    let rows = query(&mut engine, "SELECT COUNT(*) AS cnt FROM t");
    assert_eq!(rows.rows[0][0], Value::BigInt(0));
}

#[test]
fn test_phase6_compatibility_report_spans_status_and_warnings() {
    let engine = Engine::new();
    let report = engine.analyze_sql_batch(
        "SET LANGUAGE portuguese;\nSELECT 1 AS x;\nFOO BAR;",
    );
    assert_eq!(report.entries.len(), 3);
    assert_eq!(report.entries[0].status, SupportStatus::Partial);
    assert_eq!(report.entries[1].status, SupportStatus::Supported);
    assert_eq!(report.entries[2].status, SupportStatus::Unsupported);
    assert_eq!(report.entries[0].span.start_line, 1);
    assert_eq!(report.entries[2].span.start_line, 3);
}

#[test]
fn test_phase6_explain_plan_shape() {
    let engine = Engine::new();
    let plan = engine
        .explain_sql("SELECT id FROM dbo.t WHERE id = 1 ORDER BY id")
        .unwrap();
    assert_eq!(plan.statement_kind, "SELECT");
    assert!(plan.operators.iter().any(|op| op.op == "Filter"));
    assert!(plan.operators.iter().any(|op| op.op == "Sort"));
    assert!(plan.read_tables.contains(&"DBO.T".to_string()));
}

#[test]
fn test_phase6_trace_respects_nocount() {
    let db = Database::new();
    let sid = db.create_session();
    db.execute_session(
        sid,
        parse_sql("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)").unwrap(),
    )
    .unwrap();

    let trace = db
        .trace_execute_session_sql(
            sid,
            "SET NOCOUNT ON; INSERT INTO t (id) VALUES (1); SELECT id FROM t ORDER BY id;",
        )
        .unwrap();
    assert_eq!(trace.events.len(), 3);
    let last = trace.events.last().unwrap();
    assert_eq!(last.status, "ok");
    assert_eq!(last.row_count, None);
}

#[test]
fn test_phase6_explain_filter_detail() {
    let engine = Engine::new();
    let plan = engine
        .explain_sql("SELECT id FROM dbo.t WHERE id = 1 AND name <> 'foo'")
        .unwrap();
    let filter_op = plan.operators.iter().find(|op| op.op == "Filter").unwrap();
    assert!(filter_op.detail.contains("WHERE"));
    assert!(filter_op.detail.contains("id = 1"));
    assert!(filter_op.detail.contains("AND"));
    assert!(filter_op.detail.contains("name <> 'foo'"));
}

#[test]
fn test_phase6_explain_project_columns() {
    let engine = Engine::new();
    let plan = engine
        .explain_sql("SELECT id, name AS user_name, COUNT(*) AS cnt FROM dbo.t GROUP BY id, name")
        .unwrap();
    let project_op = plan.operators.iter().find(|op| op.op == "Project").unwrap();
    assert!(project_op.detail.contains("id"));
    assert!(project_op.detail.contains("name AS user_name"));
    assert!(project_op.detail.contains("COUNT(*) AS cnt"));
}

#[test]
fn test_phase6_explain_join_detail() {
    let engine = Engine::new();
    let plan = engine
        .explain_sql("SELECT u.id FROM dbo.users u LEFT JOIN dbo.orders o ON u.id = o.user_id")
        .unwrap();
    let join_op = plan.operators.iter().find(|op| op.op == "Join").unwrap();
    assert!(join_op.detail.contains("LEFT JOIN"));
    assert!(join_op.detail.contains("ON"));
    assert!(join_op.detail.contains("u.id = o.user_id"));
}

#[test]
fn test_phase6_explain_group_by_having() {
    let engine = Engine::new();
    let plan = engine
        .explain_sql("SELECT id FROM dbo.t GROUP BY id HAVING COUNT(*) > 5")
        .unwrap();
    let agg_op = plan.operators.iter().find(|op| op.op == "Aggregate").unwrap();
    assert!(agg_op.detail.contains("GROUP BY"));
    assert!(agg_op.detail.contains("id"));
    assert!(agg_op.detail.contains("HAVING"));
    assert!(agg_op.detail.contains("COUNT(*) > 5"));
}

#[test]
fn test_phase6_explain_order_by_direction() {
    let engine = Engine::new();
    let plan = engine
        .explain_sql("SELECT id FROM dbo.t ORDER BY id ASC, name DESC")
        .unwrap();
    let sort_op = plan.operators.iter().find(|op| op.op == "Sort").unwrap();
    assert!(sort_op.detail.contains("ORDER BY"));
    assert!(sort_op.detail.contains("id"));
    assert!(sort_op.detail.contains("name DESC"));
}

#[test]
fn test_phase6_explain_update_with_set() {
    let engine = Engine::new();
    let plan = engine
        .explain_sql("UPDATE dbo.t SET name = 'foo', score = 100 WHERE id = 1")
        .unwrap();
    assert_eq!(plan.statement_kind, "UPDATE");
    let update_op = plan.operators.iter().find(|op| op.op == "Update").unwrap();
    assert!(update_op.detail.contains("SET"));
    assert!(update_op.detail.contains("name = 'foo'"));
    assert!(update_op.detail.contains("score = 100"));
}

#[test]
fn test_phase6_metadata_routines_and_constraints_views() {
    let mut e = Engine::new();
    exec(
        &mut e,
        "CREATE TABLE dbo.t (id INT CONSTRAINT DF_t_id DEFAULT 1, v INT, CONSTRAINT CK_t_v CHECK (v > 0))",
    );
    exec(
        &mut e,
        "CREATE PROCEDURE dbo.bump @in INT, @out INT OUTPUT AS BEGIN SET @out = @in + 1; RETURN; END",
    );

    let r1 = query(
        &mut e,
        "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'dbo' AND ROUTINE_NAME = 'bump'",
    );
    assert_eq!(r1.rows.len(), 1);

    let r2 = query(
        &mut e,
        "SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 't' ORDER BY CONSTRAINT_NAME",
    );
    assert!(r2.rows.len() >= 2);

    let r3 = query(
        &mut e,
        "SELECT name FROM sys.routines WHERE name = 'bump'",
    );
    assert_eq!(r3.rows.len(), 1);
}

#[test]
fn test_phase6_quoted_identifier_on() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
    exec(&mut engine, "INSERT INTO dbo.t (id, name) VALUES (1, 'test')");
    
    let rows = query(&mut engine, "SELECT id FROM dbo.t WHERE name = 'test'");
    assert_eq!(rows.rows.len(), 1);
    
    let rows2 = query(&mut engine, "SELECT \"id\" FROM dbo.t WHERE \"name\" = 'test'");
    assert_eq!(rows2.rows.len(), 1);
}

#[test]
fn test_phase6_quoted_identifier_off() {
    let mut engine = Engine::new();
    exec(&mut engine, "SET QUOTED_IDENTIFIER OFF");
    exec(&mut engine, "CREATE TABLE dbo.t2 (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))");
    exec(&mut engine, "INSERT INTO dbo.t2 (id, name) VALUES (1, 'test')");
    
    let rows = query(&mut engine, "SELECT id FROM dbo.t2 WHERE name = 'test'");
    assert_eq!(rows.rows.len(), 1);
}
