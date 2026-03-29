# MVCC Conflict Matrix (Modeled R5)

This document captures the currently modeled multi-session transaction outcomes in `tsql_core`.

## Anomaly Simulation Matrix

| Isolation level | Dirty read | Non-repeatable read | Phantom read | Lost update | Write skew |
|---|---|---|---|---|---|
| READ UNCOMMITTED | Allowed | Allowed | Allowed | Allowed | Allowed |
| READ COMMITTED | Blocked in current model | Allowed | Allowed | Allowed | Allowed |
| REPEATABLE READ | Blocked | Blocked (read lock + no-wait write conflict) | Blocked (read lock + no-wait write conflict) | Blocked (write lock no-wait) | Blocked at table-lock granularity (no-wait) |
| SERIALIZABLE | Blocked | Blocked (read lock + no-wait write conflict) | Blocked (read lock + no-wait write conflict) | Blocked (write lock no-wait) | Blocked at table-lock granularity (no-wait) |
| SNAPSHOT | Blocked | Blocked (read lock + no-wait write conflict) | Blocked (read lock + no-wait write conflict) | Blocked (write lock no-wait) | Blocked at table-lock granularity (no-wait) |

## Commit Conflict Matrix (Modeled)

| Conflict shape | RU | RC | RR | SER | SNAPSHOT |
|---|---|---|---|---|---|
| WW (same table) | Immediate lock conflict (no-wait) | Immediate lock conflict (no-wait) | Immediate lock conflict (no-wait) | Immediate lock conflict (no-wait) | Immediate lock conflict (no-wait) |
| RW then W on same table | Allow | Allow | Immediate lock conflict (no-wait) | Immediate lock conflict (no-wait) | Immediate lock conflict (no-wait) |
| Predicate-read then concurrent insert | Allow | Allow | Immediate lock conflict (no-wait) | Immediate lock conflict (no-wait) | Immediate lock conflict (no-wait) |

## Notes

- Conflict handling is deterministic and immediate: no lock-wait simulation.
- Locking is table-level and no-wait in this R5 model.
- RU dirty reads are supported by merging uncommitted writes from all active sessions into a transient view.
- Write skew remains table-level (not row/predicate-lock equivalent to SQL Server).
