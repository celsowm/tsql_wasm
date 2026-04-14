//!
//! WAL data types: Lsn, WalRecord, WalFrame.

use serde::{Deserialize, Serialize};

use crate::types::Value;

/// Log sequence number. Monotonically increasing per-wal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016X}", self.0)
    }
}

/// A single WAL record capturing a mutation or lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    /// Transaction begin with isolation level
    Begin { tx_id: u64, isolation_level: String },
    /// Row insertion
    InsertRow {
        tx_id: u64,
        table_id: u32,
        row: Vec<Value>,
    },
    /// Row update
    UpdateRow {
        tx_id: u64,
        table_id: u32,
        index: usize,
        old_row: Vec<Value>,
        new_row: Vec<Value>,
    },
    /// Row deletion
    DeleteRow {
        tx_id: u64,
        table_id: u32,
        index: usize,
        old_row: Vec<Value>,
    },
    /// Data definition language statement
    Ddl {
        tx_id: u64,
        kind: String,
        table_name: String,
    },
    /// Transaction commit marker
    Commit { tx_id: u64 },
    /// Transaction rollback marker
    Rollback { tx_id: u64 },
    /// Savepoint marker
    Savepoint { tx_id: u64, name: String },
    /// Checkpoint marker with LSN
    Checkpoint { lsn: Lsn },
}

/// A WAL frame: one record with its assigned sequence number.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalFrame {
    pub lsn: Lsn,
    pub payload: WalRecord,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_display_hex() {
        let lsn = Lsn(255);
        assert_eq!(format!("{}", lsn), "00000000000000FF");
    }

    #[test]
    fn test_lsn_ordering() {
        assert!(Lsn(1) < Lsn(2));
        assert!(Lsn(10) > Lsn(5));
    }

    #[test]
    fn test_wal_record_serialization_roundtrip() {
        let record = WalRecord::Begin {
            tx_id: 1,
            isolation_level: "ReadCommitted".to_string(),
        };
        let json = serde_json::to_string(&record).unwrap();
        let restored: WalRecord = serde_json::from_str(&json).unwrap();
        match restored {
            WalRecord::Begin {
                tx_id,
                isolation_level,
            } => {
                assert_eq!(tx_id, 1);
                assert_eq!(isolation_level, "ReadCommitted");
            }
            _ => panic!("expected Begin after roundtrip"),
        }
    }

    #[test]
    fn test_wal_frame_serialization() {
        let frame = WalFrame {
            lsn: Lsn(7),
            payload: WalRecord::InsertRow {
                tx_id: 1,
                table_id: 3,
                row: vec![Value::Int(42)],
            },
        };
        let json = serde_json::to_string(&frame).unwrap();
        let restored: WalFrame = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.lsn, Lsn(7));
    }

    #[test]
    fn test_wal_frame_serialization_variants() {
        let frames = vec![
            WalFrame {
                lsn: Lsn(1),
                payload: WalRecord::Begin {
                    tx_id: 1,
                    isolation_level: "ReadCommitted".into(),
                },
            },
            WalFrame {
                lsn: Lsn(2),
                payload: WalRecord::InsertRow {
                    tx_id: 1,
                    table_id: 1,
                    row: vec![Value::Int(10)],
                },
            },
            WalFrame {
                lsn: Lsn(3),
                payload: WalRecord::UpdateRow {
                    tx_id: 1,
                    table_id: 1,
                    index: 0,
                    old_row: vec![Value::Int(10)],
                    new_row: vec![Value::Int(20)],
                },
            },
            WalFrame {
                lsn: Lsn(4),
                payload: WalRecord::DeleteRow {
                    tx_id: 1,
                    table_id: 1,
                    index: 0,
                    old_row: vec![Value::Int(20)],
                },
            },
            WalFrame {
                lsn: Lsn(5),
                payload: WalRecord::Ddl {
                    tx_id: 1,
                    kind: "CreateTable".into(),
                    table_name: "test".into(),
                },
            },
            WalFrame {
                lsn: Lsn(6),
                payload: WalRecord::Commit { tx_id: 1 },
            },
            WalFrame {
                lsn: Lsn(7),
                payload: WalRecord::Rollback { tx_id: 2 },
            },
            WalFrame {
                lsn: Lsn(8),
                payload: WalRecord::Savepoint {
                    tx_id: 1,
                    name: "sp1".into(),
                },
            },
            WalFrame {
                lsn: Lsn(9),
                payload: WalRecord::Checkpoint { lsn: Lsn(5) },
            },
        ];
        for frame in frames {
            let json = serde_json::to_string(&frame).unwrap();
            let restored: WalFrame = serde_json::from_str(&json).unwrap();
            assert_eq!(restored.lsn, frame.lsn);
        }
    }
}
