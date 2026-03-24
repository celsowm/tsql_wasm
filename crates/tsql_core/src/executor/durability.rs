use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::fs;

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
    C: Serialize + DeserializeOwned,
{
    pub fn to_json(&self) -> Result<String, DbError> {
        serde_json::to_string(self)
            .map_err(|e| DbError::Execution(format!("failed to encode checkpoint: {}", e)))
    }

    pub fn from_json(payload: &str) -> Result<Self, DbError> {
        serde_json::from_str(payload)
            .map_err(|e| DbError::Execution(format!("failed to decode checkpoint: {}", e)))
    }
}

pub trait DurabilitySink<C>: std::fmt::Debug + Send + Sync {
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint<C>) -> Result<(), DbError>;
    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C>>;
}

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

impl<C> DurabilitySink<C> for NoopDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned,
{
    fn persist_checkpoint(&mut self, _checkpoint: &RecoveryCheckpoint<C>) -> Result<(), DbError> {
        Ok(())
    }

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
        C: Serialize + DeserializeOwned,
    {
        self.latest
            .as_ref()
            .map(|cp| cp.to_json())
            .transpose()
    }
}

impl<C> DurabilitySink<C> for InMemoryDurability<C>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static,
{
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint<C>) -> Result<(), DbError> {
        self.latest = Some(checkpoint.clone());
        Ok(())
    }

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
            let payload = fs::read_to_string(&path)
                .map_err(|e| DbError::Execution(format!("failed to read checkpoint file: {}", e)))?;
            Some(RecoveryCheckpoint::from_json(&payload)?)
        } else {
            None
        };
        Ok(Self { path, latest })
    }
}

impl<C> DurabilitySink<C> for FileDurability<C>
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

    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C>> {
        self.latest.clone()
    }
}
