# Concurrency Improvement Plan

## Current State

- **Multi-session support**: ✅ Sessions are tracked in `SharedState.sessions` with per-session state (`SessionRuntime`: transactions, temp tables, variables, cursors, identity, diagnostics).
- **Logical concurrency**: ✅ Pessimistic table-level locking (`LockTable` in `locks.rs`), dirty buffer for `READ UNCOMMITTED`, isolation levels (`READ UNCOMMITTED` → `SERIALIZABLE` + `SNAPSHOT`), MVCC write-skew detection.
- **Server layer**: ✅ Tokio `tokio::spawn` per TCP connection in `tsql_server/src/server.rs`.
- **Serialized execution**: ⚠️ Single `Arc<Mutex<SharedState>>` in `DatabaseInner` — every `execute_session`, `execute_session_batch`, `create_session`, and `close_session` acquires this global lock.
- **No deadlock detection**: ⚠️ Locks use no-wait (immediate error on conflict).
- **No connection pooling**: ⚠️ Each TCP connection creates a fresh `TdsSession`.

---

## Phase 1 — Fine-Grained Locking (replace global mutex)

**Goal**: Allow multiple sessions to execute SQL concurrently when they touch different tables.

### 1.1 Split `SharedState` into independently-lockable regions

| Component | Current location | New locking |
|-----------|-----------------|-------------|
| `SharedStorage` (catalog + storage) | Inside `Mutex<SharedState>` | `RwLock<SharedStorage>` — readers share, writers exclusive |
| `LockTable` | Inside `Mutex<SharedState>` | `Mutex<LockTable>` — short-lived acquisitions only |
| `sessions` map | Inside `Mutex<SharedState>` | `DashMap<SessionId, Mutex<SessionRuntime>>` — per-session locks |
| `DirtyBuffer` | Already `Arc<Mutex<DirtyBuffer>>` | Keep as-is |
| `durability` | Inside `Mutex<SharedState>` | `Mutex<Box<dyn DurabilitySink>>` — separate lock |

**File changes:**
- `crates/tsql_core/src/executor/session.rs` — restructure `SharedState` into `SharedStateV2` with multiple locks.
- `crates/tsql_core/src/executor/database/persistence.rs` — `DatabaseInner.inner` becomes the new struct (no single mutex wrapping everything).
- `crates/tsql_core/src/executor/database/session.rs` — `execute_session` acquires locks in defined order: (1) session lock, (2) lock table, (3) storage `RwLock`.

### 1.2 Lock ordering protocol

To prevent deadlocks between internal locks:

```
Session lock → LockTable → Storage (RwLock) → DirtyBuffer → Durability
```

Always acquire in this order. Never hold a downstream lock while acquiring an upstream one.

### 1.3 Read path optimization

For read-only queries outside a transaction:
1. Acquire `Storage` as `RwLock::read()` (shared).
2. Execute `ScriptExecutor` against shared refs.
3. No lock table interaction needed (auto-commit reads).

For read-only queries inside a transaction:
1. Acquire session lock.
2. Acquire lock table (short hold to register/check locks).
3. Execute against workspace snapshot (already cloned at `BEGIN TRANSACTION`).

### 1.4 Write path

1. Acquire session lock.
2. Acquire lock table → register write locks (short hold, release immediately after).
3. Execute against workspace (within transaction) or acquire `Storage` as `RwLock::write()` (auto-commit).
4. On commit: acquire `Storage` write lock → merge workspace → bump `commit_ts` → release.

### 1.5 Tests

- Modify `phase5_concurrency_mvcc.rs` to use `std::thread::spawn` (or `tokio::spawn`) to run sessions truly in parallel.
- Add stress test: 10 sessions doing concurrent inserts into different tables — must all succeed.
- Add stress test: 10 sessions doing concurrent updates to the same table — proper lock conflict errors.

---

## Phase 2 — Lock Wait with Timeout (replace no-wait policy)

**Goal**: Instead of immediate error on lock conflict, wait up to a configurable timeout.

### 2.1 Add `LockWaiter` mechanism

**File**: `crates/tsql_core/src/executor/locks.rs`

```rust
pub struct LockTable {
    locks: HashMap<String, TableLockState>,
    notify: Arc<tokio::sync::Notify>,  // or parking_lot::Condvar for sync
}
```

- On conflict: `acquire_lock` returns `LockResult::Wait` instead of immediate error.
- Caller spins/parks with timeout (default `LOCK_TIMEOUT = 5000ms`, configurable via `SET LOCK_TIMEOUT`).
- On release: `release_lock_count` / `release_all_for_session` signals the `Notify`/`Condvar`.

### 2.2 Session-level `SET LOCK_TIMEOUT`

- Add `lock_timeout_ms: Option<i64>` to `SessionOptions` (`-1` = wait forever, `0` = no-wait, `N` = N ms).
- Parse `SET LOCK_TIMEOUT <value>` in the SET option handler.
- Default: `0` (no-wait, preserving current behavior).

### 2.3 Tests

- Test that a session with `SET LOCK_TIMEOUT 100` waits and then fails after 100ms.
- Test that a session with `SET LOCK_TIMEOUT -1` waits until the lock is released (by another thread).
- Test that `SET LOCK_TIMEOUT 0` preserves current immediate-error behavior.

---

## Phase 3 — Deadlock Detection

**Goal**: Detect circular wait chains and abort the victim.

### 3.1 Wait-For Graph (WFG)

**File**: new `crates/tsql_core/src/executor/deadlock.rs`

```rust
pub struct WaitForGraph {
    /// session_id -> set of session_ids it's waiting on
    edges: HashMap<SessionId, HashSet<SessionId>>,
}

impl WaitForGraph {
    pub fn add_edge(&mut self, waiter: SessionId, holder: SessionId);
    pub fn remove_waiter(&mut self, waiter: SessionId);
    pub fn detect_cycle(&self) -> Option<Vec<SessionId>>;
}
```

### 3.2 Integration with `LockTable`

- When a session enters wait state, add edge: `waiter → holder`.
- When lock is acquired or wait times out, remove edge.
- Run cycle detection before parking (or on a periodic timer).
- On cycle: pick victim (lowest `session_id` or least work done), return `DbError::Deadlock`.

### 3.3 SQL Server compatibility

- Error message: `Msg 1205: Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim.`
- Deadlock victim's transaction is automatically rolled back.

### 3.4 Tests

- Two sessions: S1 holds lock on A, wants B; S2 holds lock on B, wants A → one gets deadlock error.
- Three-session cycle: S1→A, S2→B, S3→C, then S1→B, S2→C, S3→A.

---

## Phase 4 — Row-Level Locking (optional, advanced)

**Goal**: Increase concurrency by locking individual rows instead of entire tables.

### 4.1 Lock granularity

Current: `LockTable` uses table name as key → entire table locked.

New hierarchy:
```
DATABASE → TABLE → PAGE → ROW
```

For simplicity, skip PAGE level and go straight to ROW:

```rust
pub enum LockResource {
    Table(String),
    Row(String, RowId),  // table_name, row_index
}
```

### 4.2 Lock escalation

- Start with row locks.
- If a single session holds > N row locks on the same table (e.g., N=5000), escalate to table lock.
- SQL Server uses this same pattern.

### 4.3 Impact

- Requires changes to `ScriptExecutor` to report which rows were read/written.
- Much larger `LockTable` (many more entries).
- Only pursue this if real workloads show table-level locking as a bottleneck.

---

## Phase 5 — Connection Pooling (server layer)

**Goal**: Reuse database sessions across TCP connections.

### 5.1 Session pool in `TdsServer`

**File**: `crates/tsql_server/src/server.rs`

```rust
pub struct TdsServer {
    db: Database,
    config: Arc<ServerConfig>,
    listener: Option<TcpListener>,
    session_pool: Arc<SessionPool>,
}

pub struct SessionPool {
    available: Mutex<Vec<SessionId>>,
    max_size: usize,
}
```

### 5.2 Lifecycle

1. On new TCP connection: `session_pool.checkout()` → returns existing `SessionId` or creates new.
2. Before returning to pool: reset session state (`SessionRuntime::reset()`).
3. On pool exhaustion: queue connection or reject with error.

### 5.3 Configuration

Add to `ServerConfig`:
```rust
pub pool_min_size: usize,      // default: 1
pub pool_max_size: usize,      // default: 50
pub pool_idle_timeout_secs: u64, // default: 300
```

### 5.4 Tests

- Verify session reset clears variables, temp tables, cursors, transaction state.
- Verify pooled session doesn't leak state from previous connection.
- Verify pool respects `max_size`.

---

## Implementation Priority

| Phase | Effort | Impact | Recommendation |
|-------|--------|--------|----------------|
| **Phase 1** — Fine-grained locking | High | High | **Do first** — prerequisite for true parallelism |
| **Phase 2** — Lock wait + timeout | Medium | Medium | Do second — makes lock conflicts usable |
| **Phase 3** — Deadlock detection | Medium | Medium | Do third — required once waits are introduced |
| **Phase 5** — Connection pooling | Low | Low | Do fourth — simple, independent of other phases |
| **Phase 4** — Row-level locking | Very High | Varies | **Defer** — only if benchmarks justify it |

## Non-Goals

- **Distributed transactions**: Out of scope for an embedded database.
- **Lock partitioning / lock striping**: Premature optimization at this scale.
- **Async executor integration for WASM**: WASM runs single-threaded; fine-grained locking benefits the server path only.
