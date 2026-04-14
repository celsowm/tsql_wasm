//!
//! WAL log I/O: append-only file writes, reading, truncation.
//!
//! The Wal struct manages a single WAL file on disk (or in-memory for testing).

use std::io::Write;
use std::path::Path;

use crate::error::DbError;

use super::record::{Lsn, WalFrame, WalRecord};

/// Write-Ahead Log (WAL) for durable storage and crash recovery.
///
/// The WAL records all transaction lifecycle events (BEGIN, COMMIT, ROLLBACK,
/// INSERT, UPDATE, DELETE, DDL) and checkpoint markers. On startup, the
/// database replays WAL records after the last checkpoint to restore
/// consistency.
///
/// The log is append-only on disk (JSONL format, one frame per line).
/// After a checkpoint is persisted, the log is truncated.
#[derive(Debug)]
pub struct Wal {
    writer: Option<std::fs::File>,
    path: std::path::PathBuf,
    next_lsn: u64,
}

impl Wal {
    /// Open (or create) a WAL file at the given path.
    ///
    /// If the file already exists, reads existing frames to resume the LSN
    /// counter at `max_existing_lsn + 1`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let next_lsn = if path.exists() {
            let existing = std::fs::read_to_string(&path)
                .map_err(|e| DbError::Execution(format!("failed to read WAL file: {}", e)))?;
            existing
                .lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|line| serde_json::from_str::<WalFrame>(line).ok())
                .map(|frame| frame.lsn.0)
                .max()
                .unwrap_or(0)
                + 1
        } else {
            std::fs::File::create(&path)
                .map_err(|e| DbError::Execution(format!("failed to create WAL file: {}", e)))?;
            1
        };

        Ok(Self {
            writer: None,
            path,
            next_lsn,
        })
    }

    /// Create an in-memory WAL (no file persistence).
    /// Appends return LSNs but nothing is written.
    pub fn open_in_memory() -> Self {
        Self {
            writer: None,
            path: std::path::PathBuf::from(":memory:"),
            next_lsn: 1,
        }
    }

    fn ensure_writer(&mut self) -> Result<&mut std::fs::File, DbError> {
        if self.writer.is_none() {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(|e| DbError::Execution(format!("failed to open WAL: {}", e)))?;
            self.writer = Some(file);
        }
        Ok(self.writer.as_mut().unwrap())
    }

    /// Append a record to the WAL, returning its LSN.
    ///
    /// The record is immediately flushed to disk to guarantee durability.
    pub fn append(&mut self, record: WalRecord) -> Result<Lsn, DbError> {
        let lsn = Lsn(self.next_lsn);
        self.next_lsn += 1;

        let frame = WalFrame {
            lsn,
            payload: record,
        };

        if self.path.to_str() == Some(":memory:") {
            return Ok(lsn);
        }

        let writer = self.ensure_writer()?;
        let line = serde_json::to_string(&frame)
            .map_err(|e| DbError::Execution(format!("failed to serialize WAL frame: {}", e)))?;
        writeln!(writer, "{}", line)
            .map_err(|e| DbError::Execution(format!("failed to write WAL frame: {}", e)))?;
        writer
            .flush()
            .map_err(|e| DbError::Execution(format!("failed to flush WAL: {}", e)))?;

        Ok(lsn)
    }

    /// Force-flush any buffered writes to disk.
    pub fn flush(&mut self) -> Result<(), DbError> {
        if let Some(ref mut writer) = self.writer {
            writer
                .flush()
                .map_err(|e| DbError::Execution(format!("failed to flush WAL: {}", e)))?;
        }
        Ok(())
    }

    /// Read all frames from the WAL file.  In-memory WALs always return empty.
    pub fn read_all_records(&self) -> Result<Vec<WalFrame>, DbError> {
        if self.path.to_str() == Some(":memory:") {
            return Ok(Vec::new());
        }

        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let data = std::fs::read_to_string(&self.path)
            .map_err(|e| DbError::Execution(format!("failed to read WAL file: {}", e)))?;

        let mut frames = Vec::new();
        for line in data.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<WalFrame>(line) {
                Ok(frame) => frames.push(frame),
                Err(e) => {
                    return Err(DbError::Execution(format!("WAL corruption: {}", e)));
                }
            }
        }
        Ok(frames)
    }

    /// Truncate (wipe) the WAL file, resetting the LSN counter to 1.
    ///
    /// Called after a checkpoint is successfully persisted.
    pub fn truncate(&mut self) -> Result<(), DbError> {
        if self.path.to_str() == Some(":memory:") {
            return Ok(());
        }

        self.writer = None;
        std::fs::write(&self.path, "")
            .map_err(|e| DbError::Execution(format!("failed to truncate WAL: {}", e)))?;
        self.next_lsn = 1;
        Ok(())
    }

    /// The most recently assigned LSN (zero if no records have been written).
    pub fn current_lsn(&self) -> Lsn {
        Lsn(self.next_lsn.saturating_sub(1))
    }

    /// Path of the backing file (or `:memory:` sentinel).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use tempfile::TempDir;

    #[test]
    fn test_wal_open_create() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let wal = Wal::open(&path).unwrap();
        assert_eq!(wal.path(), path.as_path());
    }

    #[test]
    fn test_wal_append_and_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let mut wal = Wal::open(&path).unwrap();

        wal.append(WalRecord::Begin {
            tx_id: 1,
            isolation_level: "ReadCommitted".to_string(),
        })
        .unwrap();
        wal.append(WalRecord::InsertRow {
            tx_id: 1,
            table_id: 1,
            row: vec![Value::Int(42)],
        })
        .unwrap();
        wal.append(WalRecord::Commit { tx_id: 1 }).unwrap();

        let records = wal.read_all_records().unwrap();
        assert_eq!(records.len(), 3);

        match &records[0].payload {
            WalRecord::Begin { tx_id, .. } => assert_eq!(*tx_id, 1),
            _ => panic!("expected Begin record"),
        }
        match &records[1].payload {
            WalRecord::InsertRow {
                tx_id, table_id, ..
            } => {
                assert_eq!(*tx_id, 1);
                assert_eq!(*table_id, 1);
            }
            _ => panic!("expected InsertRow record"),
        }
        match &records[2].payload {
            WalRecord::Commit { tx_id } => assert_eq!(*tx_id, 1),
            _ => panic!("expected Commit record"),
        }
    }

    #[test]
    fn test_wal_lsn_monotonic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let mut wal = Wal::open(&path).unwrap();

        let lsn1 = wal
            .append(WalRecord::Begin {
                tx_id: 1,
                isolation_level: "ReadCommitted".to_string(),
            })
            .unwrap();
        let lsn2 = wal.append(WalRecord::Commit { tx_id: 1 }).unwrap();

        assert!(lsn2.0 > lsn1.0);
    }

    #[test]
    fn test_wal_truncate() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let mut wal = Wal::open(&path).unwrap();

        wal.append(WalRecord::Begin {
            tx_id: 1,
            isolation_level: "ReadCommitted".to_string(),
        })
        .unwrap();
        wal.append(WalRecord::Commit { tx_id: 1 }).unwrap();

        wal.truncate().unwrap();

        let records = wal.read_all_records().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_wal_in_memory() {
        let mut wal = Wal::open_in_memory();
        let lsn = wal
            .append(WalRecord::Begin {
                tx_id: 1,
                isolation_level: "Snapshot".to_string(),
            })
            .unwrap();
        assert_eq!(lsn, Lsn(1));

        let records = wal.read_all_records().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_wal_reopen_continues_lsn() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");

        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(WalRecord::Begin {
                tx_id: 1,
                isolation_level: "ReadCommitted".to_string(),
            })
            .unwrap();
            wal.append(WalRecord::Commit { tx_id: 1 }).unwrap();
        }

        let mut wal = Wal::open(&path).unwrap();
        let lsn = wal
            .append(WalRecord::Begin {
                tx_id: 2,
                isolation_level: "ReadCommitted".to_string(),
            })
            .unwrap();
        assert_eq!(lsn, Lsn(3));
    }
}
