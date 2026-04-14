use iridium_core::ast::{IsolationLevel, SessionStatement, Statement, TransactionStatement};
use iridium_core::parse_sql;

fn assert_parses(sql: &str) -> Statement {
    parse_sql(sql).unwrap_or_else(|e| panic!("failed to parse: {}\n  error: {}", sql, e))
}

// ── Isolation levels ────────────────────────────────────────────

#[test]
fn set_isolation_read_uncommitted() {
    let stmt = assert_parses("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    assert!(matches!(
        stmt,
        Statement::Session(SessionStatement::SetTransactionIsolationLevel(IsolationLevel::ReadUncommitted))
    ));
}

#[test]
fn set_isolation_read_committed() {
    let stmt = assert_parses("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    assert!(matches!(
        stmt,
        Statement::Session(SessionStatement::SetTransactionIsolationLevel(IsolationLevel::ReadCommitted))
    ));
}

#[test]
fn set_isolation_repeatable_read() {
    let stmt = assert_parses("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    assert!(matches!(
        stmt,
        Statement::Session(SessionStatement::SetTransactionIsolationLevel(IsolationLevel::RepeatableRead))
    ));
}

#[test]
fn set_isolation_serializable() {
    let stmt = assert_parses("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    assert!(matches!(
        stmt,
        Statement::Session(SessionStatement::SetTransactionIsolationLevel(IsolationLevel::Serializable))
    ));
}

#[test]
fn set_isolation_snapshot() {
    let stmt = assert_parses("SET TRANSACTION ISOLATION LEVEL SNAPSHOT");
    assert!(matches!(
        stmt,
        Statement::Session(SessionStatement::SetTransactionIsolationLevel(IsolationLevel::Snapshot))
    ));
}

// ── BEGIN TRANSACTION variants ──────────────────────────────────

#[test]
fn begin_tran_short() {
    let stmt = assert_parses("BEGIN TRAN");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Begin(None))));
}

#[test]
fn begin_transaction_named() {
    let stmt = assert_parses("BEGIN TRANSACTION MyTx");
    match stmt {
        Statement::Transaction(TransactionStatement::Begin(Some(name))) => assert_eq!(name, "MyTx"),
        _ => panic!("expected Begin(Some)"),
    }
}

#[test]
fn begin_tran_named() {
    let stmt = assert_parses("BEGIN TRAN tx1");
    match stmt {
        Statement::Transaction(TransactionStatement::Begin(Some(name))) => assert_eq!(name, "tx1"),
        _ => panic!("expected Begin(Some)"),
    }
}

#[test]
fn begin_tran_with_mark() {
    let stmt = assert_parses("BEGIN TRAN MyTx WITH MARK 'backup point'");
    match stmt {
        Statement::Transaction(TransactionStatement::Begin(Some(name))) => assert_eq!(name, "MyTx"),
        _ => panic!("expected Begin(Some) with WITH MARK stripped"),
    }
}

#[test]
fn begin_transaction_with_mark_no_desc() {
    // WITH MARK without a description string
    let stmt = assert_parses("BEGIN TRANSACTION LogTx WITH MARK");
    match stmt {
        Statement::Transaction(TransactionStatement::Begin(Some(name))) => assert_eq!(name, "LogTx"),
        _ => panic!("expected Begin(Some)"),
    }
}

// ── COMMIT variants ─────────────────────────────────────────────

#[test]
fn commit_bare() {
    let stmt = assert_parses("COMMIT");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Commit(None))));
}

#[test]
fn commit_tran() {
    let stmt = assert_parses("COMMIT TRAN");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Commit(_))));
}

#[test]
fn commit_transaction_keyword() {
    let stmt = assert_parses("COMMIT TRANSACTION");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Commit(_))));
}

#[test]
fn commit_transaction_named() {
    let stmt = assert_parses("COMMIT TRANSACTION MyTx");
    match stmt {
        Statement::Transaction(TransactionStatement::Commit(Some(name))) => assert_eq!(name, "MyTx"),
        _ => panic!("expected Commit(Some)"),
    }
}

#[test]
fn commit_tran_named() {
    let stmt = assert_parses("COMMIT TRAN tx1");
    match stmt {
        Statement::Transaction(TransactionStatement::Commit(Some(name))) => assert_eq!(name, "tx1"),
        _ => panic!("expected Commit(Some)"),
    }
}

// ── ROLLBACK variants ───────────────────────────────────────────

#[test]
fn rollback_bare() {
    let stmt = assert_parses("ROLLBACK");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Rollback(None))));
}

#[test]
fn rollback_tran() {
    let stmt = assert_parses("ROLLBACK TRAN");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Rollback(None))));
}

#[test]
fn rollback_transaction_keyword() {
    let stmt = assert_parses("ROLLBACK TRANSACTION");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Rollback(None))));
}

#[test]
fn rollback_to_savepoint() {
    let stmt = assert_parses("ROLLBACK TRANSACTION sp1");
    match stmt {
        Statement::Transaction(TransactionStatement::Rollback(Some(name))) => assert_eq!(name, "sp1"),
        _ => panic!("expected Rollback(Some) with savepoint"),
    }
}

// ── SAVE TRANSACTION ────────────────────────────────────────────

#[test]
fn save_transaction() {
    let stmt = assert_parses("SAVE TRANSACTION sp1");
    match stmt {
        Statement::Transaction(TransactionStatement::Save(name)) => assert_eq!(name, "sp1"),
        _ => panic!("expected Save"),
    }
}

#[test]
fn save_tran() {
    let stmt = assert_parses("SAVE TRAN checkpoint");
    match stmt {
        Statement::Transaction(TransactionStatement::Save(name)) => assert_eq!(name, "checkpoint"),
        _ => panic!("expected Save"),
    }
}

// ── BEGIN DISTRIBUTED TRANSACTION ───────────────────────────────

#[test]
fn begin_distributed_transaction() {
    let stmt = assert_parses("BEGIN DISTRIBUTED TRANSACTION");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Begin(_))));
}

#[test]
fn begin_distributed_tran() {
    let stmt = assert_parses("BEGIN DISTRIBUTED TRAN");
    assert!(matches!(stmt, Statement::Transaction(TransactionStatement::Begin(_))));
}

#[test]
fn begin_distributed_transaction_named() {
    let stmt = assert_parses("BEGIN DISTRIBUTED TRANSACTION dtx1");
    match stmt {
        Statement::Transaction(TransactionStatement::Begin(Some(name))) => assert_eq!(name, "dtx1"),
        _ => panic!("expected Begin(Some)"),
    }
}

