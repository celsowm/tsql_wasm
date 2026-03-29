# Refactor Plan: Cross-Session Dirty Reads (READ UNCOMMITTED)

## Problem

When Session A has `READ UNCOMMITTED` and Session B has an open transaction with uncommitted writes, Session A's `SELECT` reads from `SharedState.storage` (committed data only). It **cannot** see Session B's in-flight changes living in `SessionRuntime.workspace` because `with_session_mut` only grants access to the caller's own session — other sessions' workspaces are opaque.

## Root Cause

`SharedState.sessions` is a `HashMap<SessionId, SessionRuntime>`. When a session executes, it is **removed** from the map via `with_session_mut`, operates in isolation, then re-inserted. No session can peek into another session's `TxWorkspace`.

## Proposed Architecture: Dirty Write Buffer

Instead of making sessions reach into each other's workspaces, introduce a **shared dirty buffer** on `SharedState` that each writing transaction publishes into, and that `READ UNCOMMITTED` readers consume from.

### Phase 1 — Add `DirtyBuffer` to `SharedState`

```rust
// SharedState gains a new field:
SharedState {
    ...
    dirty_buffer: DirtyBuffer<S>,   // NEW
}
```

`DirtyBuffer` is a per-table overlay structure:

```rust
pub struct DirtyBuffer<S> {
    /// session_id → table_name → Vec<DirtyOp>
    pub pending: HashMap<SessionId, HashMap<String, Vec<DirtyOp>>>,
}

pub enum DirtyOp {
    Insert { row: StoredRow },
    Update { row_index: usize, new_row: StoredRow },
    Delete { row_index: usize },
}
```

**Files**: new file `crates/tsql_core/src/executor/dirty_buffer.rs`, modify `session.rs` (`SharedState`)

### Phase 2 — Publish dirty ops on write

In `execute_non_transaction_statement` (and the `ScriptExecutor` write path), when a session has an active transaction and executes INSERT/UPDATE/DELETE, **also** record the operation into `state.dirty_buffer.pending[session_id][table]`.

**Files**: `database/session.rs` (the `read_from_shared` branch logic), `mutation/insert.rs`, `mutation/update.rs`, `mutation/delete.rs`

Approach: Add a post-execution hook after line 519 (`script.execute`) that diffs the workspace storage against its pre-statement snapshot for the affected tables and pushes `DirtyOp` entries.

### Phase 3 — Read with dirty overlay for READ UNCOMMITTED

When `isolation_level == ReadUncommitted` and `stmt == Select`, instead of reading plain `SharedState.storage`, construct a **merged view**:

1. Start with `state.storage` (committed baseline)
2. Apply all `DirtyOp`s from `state.dirty_buffer.pending` (from **all** sessions, including self)
3. Execute the SELECT against this merged view

This can be done by:

- Cloning the committed storage (already cheap — it's what `BEGIN TRAN` does)
- Applying the overlay ops to the clone
- Passing the clone to `ScriptExecutor`

**File**: `database/session.rs` — replace the current `read_from_shared` path (lines 491–518)

### Phase 4 — Cleanup on COMMIT/ROLLBACK

- **COMMIT**: Remove the session's entries from `dirty_buffer.pending` (the data is now in committed state)
- **ROLLBACK**: Remove the session's entries (the data is discarded)

**File**: `transaction_exec.rs` — in the `CommitTransaction` and `RollbackTransaction` arms

### Phase 5 — Tests and docs

- Add multi-session test: Session B begins TX, inserts a row; Session A with `READ UNCOMMITTED` sees the row; Session B rolls back; Session A no longer sees it
- Update `docs/mvcc_conflict_matrix.md`: change "Blocked in current model" → "Allowed"

## Summary of Files to Change

| File | Change |
|---|---|
| `executor/dirty_buffer.rs` | **New** — `DirtyBuffer`, `DirtyOp` types |
| `executor/session.rs` | Add `dirty_buffer` field to `SharedState` |
| `executor/mod.rs` | Add `mod dirty_buffer` |
| `executor/database/session.rs` | Phase 3 — merged-view read for RU selects; Phase 2 — publish dirty ops after writes |
| `executor/transaction_exec.rs` | Phase 4 — cleanup on commit/rollback |
| `mutation/insert.rs`, `update.rs`, `delete.rs` | Phase 2 — optional: push `DirtyOp` at mutation site instead of post-diff |
| `docs/mvcc_conflict_matrix.md` | Update dirty read row |

## Risks & Trade-offs

- **Clone cost**: The merged view clones storage per-`SELECT` under RU. Acceptable since RU is opt-in and the engine is already clone-heavy.
- **Row-index stability**: `DirtyOp::Update/Delete` use row indexes which may shift if multiple sessions dirty the same table. Alternative: use a row-id or primary key reference instead of index.
- **Consistency**: The dirty buffer shows a *latest-write-wins* view across sessions — this is exactly what SQL Server's READ UNCOMMITTED does (no guarantees).

## Recommended Implementation Order

**Phase 1 → Phase 4 → Phase 2 → Phase 3 → Phase 5** (structure first, cleanup hooks, then write publishing, then read path, then tests).
