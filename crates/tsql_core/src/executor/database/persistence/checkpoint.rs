use crate::error::DbError;
use crate::executor::database::{CheckpointManager, CheckpointManagerService};
use crate::executor::durability::RecoveryCheckpoint;
use super::{EngineCatalog, EngineStorage};

impl<C, S> CheckpointManager for CheckpointManagerService<C, S>
where
    C: EngineCatalog,
    S: EngineStorage,
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
