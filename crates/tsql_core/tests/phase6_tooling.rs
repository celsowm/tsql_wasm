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
        "SET DATEFIRST 9;\nSET LANGUAGE portuguese;\nSELECT 1 AS x;\nFOO BAR;",
    );
    assert_eq!(report.entries.len(), 4);
    assert_eq!(report.entries[0].status, SupportStatus::Partial);
    assert_eq!(report.entries[1].status, SupportStatus::Partial);
    assert_eq!(report.entries[2].status, SupportStatus::Supported);
    assert_eq!(report.entries[3].status, SupportStatus::Unsupported);
    assert_eq!(report.entries[0].span.start_line, 1);
    assert_eq!(report.entries[1].span.start_line, 2);
    assert_eq!(report.entries[3].span.start_line, 4);
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
