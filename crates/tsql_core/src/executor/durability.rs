use std::collections::HashMap;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "C: Serialize, S: Serialize"))]
#[serde(bound(deserialize = "C: DeserializeOwned, S: DeserializeOwned"))]
pub struct RecoveryCheckpoint<C, S> {
    pub catalog: C,
    pub storage: S,
    pub commit_ts: u64,
    pub table_versions: HashMap<String, u64>,
}

impl<C, S> RecoveryCheckpoint<C, S>
where
    C: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
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

pub trait DurabilitySink<C, S>: std::fmt::Debug + Send + Sync {
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint<C, S>) -> Result<(), DbError>;
    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C, S>>;
}

#[derive(Debug)]
pub struct NoopDurability<C, S> {
    _marker: PhantomData<(C, S)>,
}

impl<C, S> Default for NoopDurability<C, S> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<C, S> DurabilitySink<C, S> for NoopDurability<C, S>
where
    C: Catalog + Serialize + DeserializeOwned,
    S: Storage + Serialize + DeserializeOwned,
{
    fn persist_checkpoint(&mut self, _checkpoint: &RecoveryCheckpoint<C, S>) -> Result<(), DbError> {
        Ok(())
    }

    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C, S>> {
        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryDurability<C, S> {
    latest: Option<RecoveryCheckpoint<C, S>>,
}

impl<C, S> InMemoryDurability<C, S> {
    pub fn latest_json(&self) -> Result<Option<String>, DbError>
    where
        C: Serialize + DeserializeOwned,
        S: Serialize + DeserializeOwned,
    {
        self.latest
            .as_ref()
            .map(|cp| cp.to_json())
            .transpose()
    }
}

impl<C, S> DurabilitySink<C, S> for InMemoryDurability<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone,
    S: Storage + Serialize + DeserializeOwned + Clone,
{
    fn persist_checkpoint(&mut self, checkpoint: &RecoveryCheckpoint<C, S>) -> Result<(), DbError> {
        self.latest = Some(checkpoint.clone());
        Ok(())
    }

    fn latest_checkpoint(&self) -> Option<RecoveryCheckpoint<C, S>> {
        self.latest.clone()
    }
}
