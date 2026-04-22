use crate::error::DbError;
use super::super::locks::SessionId;
use super::super::session::{SessionRuntime, SharedState};
use super::{EngineCatalog, EngineStorage};

pub(crate) fn with_session<C, S, R, F>(
    state: &SharedState<C, S>,
    session_id: SessionId,
    f: F,
) -> Result<R, DbError>
where
    C: EngineCatalog,
    S: EngineStorage,
    F: FnOnce(&mut SessionRuntime<C, S>) -> Result<R, DbError>,
{
    let session_mutex = state
        .sessions
        .get(&session_id)
        .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
    let mut session = session_mutex.lock();
    f(&mut session)
}
