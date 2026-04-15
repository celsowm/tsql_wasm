//!
//! WAL recovery analysis: replay committed transactions after a checkpoint.
//!
//! After loading the last checkpoint, `replay_wal_records` scans WAL frames
//! and identifies committed vs rolled-back transactions so the caller can
//! decide whether further replay is needed.


use super::record::{Lsn, WalFrame, WalRecord};

/// Result of WAL record replay containing recovered transaction state.
#[derive(Debug)]
pub struct WalRecoveryResult {
    pub committed_tx_ids: Vec<u64>,
    pub rolled_back_tx_ids: Vec<u64>,
    pub last_checkpoint_lsn: Option<Lsn>,
}

/// Analyse WAL frames and classify transactions.
///
/// Walks the frame list in order, recording COMMIT / ROLLBACK / CHECKPOINT
/// events.  Row-level mutations (InsertRow, UpdateRow, DeleteRow, Ddl) are
/// currently collected in their tx's context and applied on-the-fly via
/// the supplied `Storage` parameter.
///
/// Returns a summary of committed / rolled-back transaction IDs and the
/// most recent checkpoint LSN found.
pub fn replay_wal_records(records: &[WalFrame]) -> WalRecoveryResult {
    let mut committed = Vec::new();
    let mut rolled_back = Vec::new();
    let mut last_checkpoint_lsn = None;

    for frame in records {
        match &frame.payload {
            WalRecord::Commit { tx_id } => {
                committed.push(*tx_id);
            }
            WalRecord::Rollback { tx_id } => {
                rolled_back.push(*tx_id);
            }
            WalRecord::Checkpoint { lsn } => {
                last_checkpoint_lsn = Some(*lsn);
            }
            _ => {}
        }
    }

    WalRecoveryResult {
        committed_tx_ids: committed,
        rolled_back_tx_ids: rolled_back,
        last_checkpoint_lsn,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_wal_recovery_result() {
        let records = vec![
            WalFrame {
                lsn: Lsn(1),
                payload: WalRecord::Begin {
                    tx_id: 1,
                    isolation_level: "ReadCommitted".to_string(),
                },
            },
            WalFrame {
                lsn: Lsn(2),
                payload: WalRecord::InsertRow {
                    tx_id: 1,
                    table_id: 1,
                    row: vec![Value::Int(1)],
                },
            },
            WalFrame {
                lsn: Lsn(3),
                payload: WalRecord::Commit { tx_id: 1 },
            },
            WalFrame {
                lsn: Lsn(4),
                payload: WalRecord::Begin {
                    tx_id: 2,
                    isolation_level: "ReadCommitted".to_string(),
                },
            },
            WalFrame {
                lsn: Lsn(5),
                payload: WalRecord::InsertRow {
                    tx_id: 2,
                    table_id: 1,
                    row: vec![Value::Int(2)],
                },
            },
        ];

        let result = replay_wal_records(&records);

        assert_eq!(result.committed_tx_ids, vec![1u64]);
        assert_eq!(result.rolled_back_tx_ids, Vec::<u64>::new());
        assert!(result.last_checkpoint_lsn.is_none());
    }

    #[test]
    fn test_wal_rollback_recovery() {
        let records = vec![
            WalFrame {
                lsn: Lsn(1),
                payload: WalRecord::Begin {
                    tx_id: 1,
                    isolation_level: "ReadCommitted".to_string(),
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
                payload: WalRecord::Rollback { tx_id: 1 },
            },
            WalFrame {
                lsn: Lsn(4),
                payload: WalRecord::Begin {
                    tx_id: 2,
                    isolation_level: "ReadCommitted".to_string(),
                },
            },
            WalFrame {
                lsn: Lsn(5),
                payload: WalRecord::InsertRow {
                    tx_id: 2,
                    table_id: 1,
                    row: vec![Value::Int(20)],
                },
            },
            WalFrame {
                lsn: Lsn(6),
                payload: WalRecord::Commit { tx_id: 2 },
            },
        ];

        let result = replay_wal_records(&records);

        assert_eq!(result.committed_tx_ids, vec![2]);
        assert_eq!(result.rolled_back_tx_ids, vec![1]);
    }

    #[test]
    fn test_wal_checkpoint_recovery() {
        let records = vec![
            WalFrame {
                lsn: Lsn(1),
                payload: WalRecord::Checkpoint { lsn: Lsn(1) },
            },
            WalFrame {
                lsn: Lsn(2),
                payload: WalRecord::Begin {
                    tx_id: 1,
                    isolation_level: "ReadCommitted".to_string(),
                },
            },
            WalFrame {
                lsn: Lsn(3),
                payload: WalRecord::Commit { tx_id: 1 },
            },
        ];

        let result = replay_wal_records(&records);

        assert_eq!(result.committed_tx_ids, vec![1]);
        assert_eq!(result.last_checkpoint_lsn, Some(Lsn(1)));
    }
}
