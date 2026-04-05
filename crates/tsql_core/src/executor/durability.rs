use std::collections::HashMap;
use std::fs;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::StorageCheckpointData;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "C: Serialize"))]
#[serde(bound(deserialize = "C: DeserializeOwned"))]
pub struct RecoveryCheckpoint<C> {
    pub catalog: C,
    pub storage_data: StorageCheckpointData,
    pub commit_ts: u64,
    pub table_versions: HashMap<String, u64>,
}

impl<C> RecoveryCheckpoint<C>
where
    C: Catalog + Serialize + DeserializeOwned,
{
    pub fn to_json(&self) -> Result<String, DbError> {
        serde_json::to_string(self)
            .map_err(|e| DbError::Execution(format!("failed to encode checkpoint: {}", e)))
    }

    pub fn from_json(payload: &str) -> Result<Self, DbError> {
        let mut cp: Self = serde_json::from_str(payload)
            .map_err(|e| DbError::Execution(format!("failed to decode checkpoint: {}", e)))?;
        cp.catalog.rebuild_maps();
        Ok(cp)
    }
}

/// Writes checkpoints to durable storage.
pub trait DurabilityWriter<C>: std::fmt::Debug + Send + Sync {
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint<C>) -> Result<(), DbError>;
}

/// Reads the latest checkpoint from durable storage.
/// Separated from DurabilityWriter so that write-only sinks (e.g. append-only audit log)
/// don't need to implement recovery, and read-only sinks don't need to implement writes.
pub trait RecoveryReader<C>: std::fmt::Debug + Send + Sync {
    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C>>;
}

/// Combined trait for sinks that support both writing and reading checkpoints.
/// Implementors should implement `DurabilityWriter` and `RecoveryReader` directly;
/// this trait is a convenience blanket that auto-implements from the two sub-traits.
pub trait DurabilitySink<C>: DurabilityWriter<C> + RecoveryReader<C> {}

impl<C, T> DurabilitySink<C> for T where T: DurabilityWriter<C> + RecoveryReader<C> {}

#[derive(Debug)]
pub struct NoopDurability<C> {
    _marker: PhantomData<C>,
}

impl<C> Default for NoopDurability<C> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<C> DurabilityWriter<C> for NoopDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned,
{
    fn persist_checkpoint(&mut self, _checkpoint: &RecoveryCheckpoint<C>) -> Result<(), DbError> {
        Ok(())
    }
}

impl<C> RecoveryReader<C> for NoopDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned,
{
    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C>> {
        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryDurability<C> {
    latest: Option<RecoveryCheckpoint<C>>,
}

impl<C> InMemoryDurability<C> {
    pub fn latest_json(&self) -> Result<Option<String>, DbError>
    where
        C: Catalog + Serialize + DeserializeOwned,
    {
        self.latest.as_ref().map(|cp| cp.to_json()).transpose()
    }
}

impl<C> DurabilityWriter<C> for InMemoryDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
{
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint<C>) -> Result<(), DbError> {
        self.latest = Some(checkpoint.clone());
        Ok(())
    }
}

impl<C> RecoveryReader<C> for InMemoryDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
{
    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C>> {
        self.latest.clone()
    }
}

#[derive(Debug)]
pub struct FileDurability<C> {
    path: PathBuf,
    latest: Option<RecoveryCheckpoint<C>>,
}

impl<C> FileDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
{
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let latest = if path.exists() {
            let payload = fs::read_to_string(&path).map_err(|e| {
                DbError::Execution(format!("failed to read checkpoint file: {}", e))
            })?;
            Some(RecoveryCheckpoint::from_json(&payload)?)
        } else {
            None
        };
        Ok(Self { path, latest })
    }
}

impl<C> DurabilityWriter<C> for FileDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
{
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint<C>) -> Result<(), DbError> {
        let payload = checkpoint.to_json()?;
        fs::write(&self.path, payload)
            .map_err(|e| DbError::Execution(format!("failed to write checkpoint file: {}", e)))?;
        self.latest = Some(checkpoint.clone());
        Ok(())
    }
}

impl<C> RecoveryReader<C> for FileDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
{
    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C>> {
        self.latest.clone()
    }
}
