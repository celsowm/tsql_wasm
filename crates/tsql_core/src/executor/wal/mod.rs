//!
//! Write-Ahead Logging (WAL) module.
//!
//! Organised by Single Responsibility:
//!
//! | Module       | Responsibility                                 |
//! |--------------|-------------------------------------------------|
//! | `record`     | WAL data types (`Lsn`, `WalRecord`, `WalFrame`) |
//! | `log`        | WAL file I/O (append / read / truncate)         |
//! | `recovery`   | Checkpoint / commit analysis from WAL frames     |
//!
//! All public items are re-exported here so existing callers can keep
//! using `use crate::executor::wal::{Wal, WalRecord, …}` unchanged.

pub mod log;
pub mod record;
pub mod recovery;

// Re-export public API at the `wal` namespace boundary.
pub use log::Wal;
pub use record::{Lsn, WalFrame, WalRecord};
pub use recovery::{replay_wal_records, WalRecoveryResult};
