# MVCC Conflict Matrix (Modeled R5)

This document captures the currently modeled multi-session transaction outcomes in `tsql_core`.

## Anomaly Simulation Matrix

| Isolation level | Dirty read | Non-repeatable read | Phantom read | Lost update | Write skew |
|---|---|---|---|---|---|
| READ UNCOMMITTED | Blocked in current model | Allowed | Allowed | Allowed | Allowed |
| READ COMMITTED | Blocked in current model | Allowed | Allowed | Allowed | Allowed |
| REPEATABLE READ | Blocked | Blocked | Blocked | Commit conflict on overlapping write sets | Model-dependent (can conflict by schedule) |
| SERIALIZABLE | Blocked | Blocked | Blocked | Commit conflict on overlapping write sets | Model-dependent (can conflict by schedule) |
| SNAPSHOT | Blocked | Blocked | Blocked | Commit conflict on overlapping write sets | Model-dependent (can conflict by schedule) |

## Commit Conflict Matrix (Modeled)

| Conflict shape | RU | RC | RR | SER | SNAPSHOT |
|---|---|---|---|---|---|
| WW (same table) | Allow last committer | Allow last committer | Conflict (late committer) | Conflict (late committer) | Conflict (late committer) |
| RW then W on same table | Allow | Allow | Conflict (table changed since snapshot) | Conflict (table changed since snapshot) | Conflict when writer overlaps changed table |
| Predicate-read then concurrent insert | Allow | Allow | May conflict by commit order | May conflict by commit order | May conflict by commit order |

## Notes

- Conflict handling is deterministic and immediate: no lock-wait simulation.
- RU dirty reads are intentionally blocked in the current implementation because reads only observe committed shared state.
- Write skew behavior is currently table-version based and may differ from SQL Server row/predicate-lock semantics.
