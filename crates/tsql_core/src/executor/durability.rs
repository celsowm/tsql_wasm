use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::catalog::CatalogImpl;
use crate::error::DbError;
use crate::storage::InMemoryStorage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryCheckpoint {
    pub catalog: CatalogImpl,
    pub storage: InMemoryStorage,
    pub commit_ts: u64,
    pub table_versions: HashMap<String, u64>,
}

impl RecoveryCheckpoint {
    pub fn to_json(&self) -> Result<String, DbError> {
        serde_json::to_string(self)
            .map_err(|e| DbError::Execution(format!("failed to encode checkpoint: {}", e)))
    }

    pub fn from_json(payload: &str) -> Result<Self, DbError> {
        serde_json::from_str(payload)
            .map_err(|e| DbError::Execution(format!("failed to decode checkpoint: {}", e)))
    }
}

pub trait DurabilitySink: std::fmt::Debug + Send + Sync {
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint) -> Result<(), DbError>;
    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint>;
}

#[derive(Debug, Default)]
pub struct NoopDurability;

impl DurabilitySink for NoopDurability {
    fn persist_checkpoint(&mut self, _checkpoint: &RecoveryCheckpoint) -> Result<(), DbError> {
        Ok(())
    }

    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint> {
        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryDurability {
    latest: Option<RecoveryCheckpoint>,
}

impl InMemoryDurability {
    pub fn latest_json(&self) -> Result<Option<String>, DbError> {
        self.latest
            .as_ref()
            .map(|cp| cp.to_json())
            .transpose()
    }
}

impl DurabilitySink for InMemoryDurability {
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint) -> Result<(), DbError> {
        self.latest = Some(checkpoint.clone());
        Ok(())
    }

    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint> {
        self.latest.clone()
    }
}
