use iridium_core::executor::locks::{LockMode, LockResource, LockTable};

fn acquire_row_lock(
    lt: &parking_lot::Mutex<LockTable>,
    session_id: u64,
    table: &str,
    row_id: usize,
    mode: LockMode,
    timeout_ms: i64,
) -> Result<(), iridium_core::error::DbError> {
    LockTable::acquire_row_lock(lt, session_id, table, row_id, mode, timeout_ms, 0, &|_| 0)
}

#[test]
fn test_row_lock_basic_acquire_release() {
    let lt = parking_lot::Mutex::new(LockTable::new());
    // Session 1 acquires write lock on row 0 of table T
    acquire_row_lock(&lt, 1, "T", 0, LockMode::Write, 0).unwrap();
    // Session 2 can lock a different row
    acquire_row_lock(&lt, 2, "T", 1, LockMode::Write, 0).unwrap();
    // Session 2 cannot lock the same row
    let err = acquire_row_lock(&lt, 2, "T", 0, LockMode::Write, 0);
    assert!(err.is_err());
    // Release session 1's lock
    lt.lock().release_all_for_session(1);
    // Now session 2 can lock row 0
    acquire_row_lock(&lt, 2, "T", 0, LockMode::Write, 0).unwrap();
    lt.lock().release_all_for_session(2);
}

#[test]
fn test_row_lock_read_write_compatibility() {
    let lt = parking_lot::Mutex::new(LockTable::new());
    // Two sessions can read the same row
    acquire_row_lock(&lt, 1, "T", 0, LockMode::Read, 0).unwrap();
    acquire_row_lock(&lt, 2, "T", 0, LockMode::Read, 0).unwrap();
    // But write is blocked when read is held
    let err = acquire_row_lock(&lt, 3, "T", 0, LockMode::Write, 0);
    assert!(err.is_err());
    lt.lock().release_all_for_session(1);
    lt.lock().release_all_for_session(2);
    // Now write succeeds
    acquire_row_lock(&lt, 3, "T", 0, LockMode::Write, 0).unwrap();
    lt.lock().release_all_for_session(3);
}

#[test]
fn test_row_lock_table_lock_blocks_row_lock() {
    let lt = parking_lot::Mutex::new(LockTable::new());
    // Session 1 acquires table-level write lock
    {
        let mut guard = lt.lock();
        let mut ws: Option<iridium_core::executor::locks::TxWorkspace<(), ()>> = None;
        guard.perform_acquire_lock(1, &mut ws, "T", LockMode::Write, 0);
    }
    // Session 2 cannot acquire row lock on any row of that table
    let err = acquire_row_lock(&lt, 2, "T", 0, LockMode::Read, 0);
    assert!(err.is_err());
    lt.lock().release_all_for_session(1);
    // Now row lock succeeds
    acquire_row_lock(&lt, 2, "T", 0, LockMode::Read, 0).unwrap();
    lt.lock().release_all_for_session(2);
}

#[test]
fn test_row_lock_escalation() {
    let lt = parking_lot::Mutex::new(LockTable::new());
    // Set low threshold for testing
    lt.lock().set_escalation_threshold(3);

    // Acquire 3 row locks (at threshold)
    acquire_row_lock(&lt, 1, "T", 0, LockMode::Write, 0).unwrap();
    acquire_row_lock(&lt, 1, "T", 1, LockMode::Write, 0).unwrap();
    acquire_row_lock(&lt, 1, "T", 2, LockMode::Write, 0).unwrap();

    // After escalation, session 1 should have a table-level lock
    // Session 2 should be blocked on ANY row of T
    let err = acquire_row_lock(&lt, 2, "T", 99, LockMode::Write, 0);
    assert!(err.is_err(), "after escalation, other sessions should be blocked");

    lt.lock().release_all_for_session(1);
    // After release, session 2 can lock
    acquire_row_lock(&lt, 2, "T", 99, LockMode::Write, 0).unwrap();
    lt.lock().release_all_for_session(2);
}

#[test]
fn test_lock_resource_display() {
    let r1 = LockResource::Table("ORDERS".to_string());
    let r2 = LockResource::Row("ORDERS".to_string(), 42);
    assert_eq!(format!("{:?}", r1), "Table(\"ORDERS\")");
    assert_eq!(format!("{:?}", r2), "Row(\"ORDERS\", 42)");
}

#[test]
fn test_row_lock_different_tables_no_conflict() {
    let lt = parking_lot::Mutex::new(LockTable::new());
    // Row 0 on table A locked by session 1
    acquire_row_lock(&lt, 1, "A", 0, LockMode::Write, 0).unwrap();
    // Row 0 on table B locked by session 2 — no conflict
    acquire_row_lock(&lt, 2, "B", 0, LockMode::Write, 0).unwrap();
    lt.lock().release_all_for_session(1);
    lt.lock().release_all_for_session(2);
}

