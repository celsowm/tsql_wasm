use std::collections::BTreeMap;
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

use crate::error::DbError;
use crate::storage::{BTreeIndex, StoredRow};
use redb::{AccessGuard, Database, ReadableTable, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};

pub(crate) const ROWS_TABLE: TableDefinition<(u32, u64), &[u8]> = TableDefinition::new("rows");
pub(crate) const TABLE_META: TableDefinition<u32, u64> = TableDefinition::new("rows_meta");

#[derive(Debug, Clone, Default)]
pub struct RedbStorage {
    db: Option<Arc<Database>>,
    pub(crate) indexes: BTreeMap<u32, BTreeIndex>,
}

impl Serialize for RedbStorage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for RedbStorage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _: () = serde::Deserialize::deserialize(deserializer)?;
        Ok(Self {
            db: None,
            indexes: BTreeMap::new(),
        })
    }
}

impl RedbStorage {
    pub(crate) fn storage_err(context: &str, err: impl Display) -> DbError {
        DbError::Storage(format!("{context}: {err}"))
    }

    pub(crate) fn serialize_row(row: &StoredRow) -> Result<Vec<u8>, DbError> {
        bincode::serde::encode_to_vec(row, bincode::config::standard())
            .map_err(|e| Self::storage_err("failed to serialize row", e))
    }

    pub(crate) fn deserialize_row(bytes: &[u8]) -> Result<StoredRow, DbError> {
        match bincode::serde::decode_from_slice(bytes, bincode::config::standard()) {
            Ok((row, _)) => Ok(row),
            Err(_) => serde_json::from_slice(bytes)
                .map_err(|e| Self::storage_err("failed to deserialize row", e)),
        }
    }

    pub(crate) fn with_write_txn<R>(
        db: &Database,
        op: impl FnOnce(&mut WriteTransaction) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        let mut write_txn = db
            .begin_write()
            .map_err(|e| Self::storage_err("failed to begin write txn", e))?;
        let result = op(&mut write_txn)?;
        write_txn
            .commit()
            .map_err(|e| Self::storage_err("failed to commit txn", e))?;
        Ok(result)
    }

    pub(crate) fn delete_rows_in_table(
        table: &mut redb::Table<'_, (u32, u64), &[u8]>,
        table_id: u32,
    ) -> Result<(), DbError> {
        let range = if let Some(next_table_id) = table_id.checked_add(1) {
            table
                .range((table_id, 0)..(next_table_id, 0))
                .map_err(|e| Self::storage_err("failed to scan range", e))?
        } else {
            table
                .range((table_id, 0)..)
                .map_err(|e| Self::storage_err("failed to scan range", e))?
        };
        let keys_to_delete = range
            .flatten()
            .map(|(k, _v): (AccessGuard<'_, (u32, u64)>, AccessGuard<'_, &[u8]>)| k.value())
            .collect::<Vec<_>>();

        for key in keys_to_delete {
            table
                .remove(key)
                .map_err(|e| Self::storage_err("failed to remove row", e))?;
        }

        Ok(())
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DbError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Self::storage_err("failed to create redb directory", e))?;
        }
        let db = Database::create(path)
            .map_err(|e| Self::storage_err("failed to create redb database", e))?;

        // Ensure physical tables exist.
        Self::with_write_txn(&db, |write_txn| {
            let _ = write_txn
                .open_table(ROWS_TABLE)
                .map_err(|e| Self::storage_err("failed to open rows table", e))?;
            let _ = write_txn
                .open_table(TABLE_META)
                .map_err(|e| Self::storage_err("failed to open rows meta table", e))?;
            Ok(())
        })?;

        Ok(Self {
            db: Some(Arc::new(db)),
            indexes: BTreeMap::new(),
        })
    }

    pub(crate) fn db(&self) -> Result<&Database, DbError> {
        self.db.as_deref().ok_or_else(|| {
            DbError::Storage("RedbStorage not initialized (handle is missing)".into())
        })
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use tempfile::tempdir;

    fn sample_row(v: i32) -> StoredRow {
        StoredRow {
            values: vec![crate::types::Value::Int(v)],
            deleted: false,
        }
    }

    fn meta_value(storage: &RedbStorage, table_id: u32) -> Option<u64> {
        let db = storage.db.as_ref().expect("storage db missing");
        let read_txn = db.begin_read().expect("failed to begin read txn");
        let meta = read_txn
            .open_table(TABLE_META)
            .expect("failed to open meta table");
        meta.get(table_id)
            .expect("failed to read meta")
            .map(|value| value.value())
    }

    #[test]
    fn clear_table_preserves_meta_and_allows_reuse() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::new(dir.path().join("data.redb")).unwrap();
        let table_id = 7;

        storage.ensure_table(table_id).unwrap();
        storage.insert_row(table_id, sample_row(10)).unwrap();
        storage.insert_row(table_id, sample_row(20)).unwrap();

        storage.clear_table(table_id).unwrap();

        let rows: Vec<_> = storage.get_rows(table_id).unwrap();
        assert!(rows.is_empty());
        assert_eq!(meta_value(&storage, table_id), Some(0));

        storage.insert_row(table_id, sample_row(30)).unwrap();
        let rows: Vec<_> = storage.get_rows(table_id).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], crate::types::Value::Int(30));
        assert_eq!(meta_value(&storage, table_id), Some(1));
    }

    #[test]
    fn remove_table_drops_meta_and_rows() {
        let dir = tempdir().unwrap();
        let mut storage = RedbStorage::new(dir.path().join("data.redb")).unwrap();
        let table_id = 11;

        storage.ensure_table(table_id).unwrap();
        storage.insert_row(table_id, sample_row(40)).unwrap();

        storage.remove_table(table_id).unwrap();

        let rows: Vec<_> = storage.get_rows(table_id).unwrap();
        assert!(rows.is_empty());
        assert_eq!(meta_value(&storage, table_id), None);

        storage.ensure_table(table_id).unwrap();
        assert_eq!(meta_value(&storage, table_id), Some(0));
        storage.insert_row(table_id, sample_row(50)).unwrap();
        let rows: Vec<_> = storage.get_rows(table_id).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], crate::types::Value::Int(50));
    }
}
