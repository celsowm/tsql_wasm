use crate::catalog::Catalog;
use crate::error::DbError;
use crate::executor::database::{CheckpointManager, CheckpointManagerService};
use crate::executor::durability::RecoveryCheckpoint;
use crate::storage::CheckpointableStorage;
use serde::de::DeserializeOwned;
use serde::Serialize;

impl<C, S> CheckpointManager for CheckpointManagerService<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: CheckpointableStorage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn export_checkpoint(&self) -> Result<String, DbError> {
        self.state.to_checkpoint().to_json()
    }

    fn import_checkpoint(&self, payload: &str) -> Result<(), DbError> {
        let checkpoint = RecoveryCheckpoint::<C>::from_json(payload)?;
        self.state.apply_checkpoint(checkpoint);
        Ok(())
    }
}
