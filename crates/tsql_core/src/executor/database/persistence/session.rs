use std::collections::HashSet;
use parking_lot::Mutex;

use crate::error::DbError;
use crate::executor::locks::SessionId;
use crate::executor::session::{SessionRuntime, SessionManager};
use crate::catalog::Catalog;
use crate::storage::Storage;
use crate::executor::journal::Journal;
use crate::executor::string_norm::normalize_identifier;
use serde::Serialize;
use serde::de::DeserializeOwned;
use crate::executor::database::SessionManagerService;

impl<C, S> SessionManager for SessionManagerService<C, S>
where
    C: Catalog + Serialize + DeserializeOwned + Clone + 'static + Default,
    S: Storage + Serialize + DeserializeOwned + Clone + 'static + Default,
{
    fn create_session(&self) -> SessionId {
        let id = self.state.next_session_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.state.sessions.insert(id, Mutex::new(SessionRuntime::new()));
        id
    }

    fn reset_session(&self, session_id: SessionId) -> Result<(), DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        let mut physical_tables = HashSet::new();
        for table in session.tables.temp_map.values() {
            physical_tables.insert(table.clone());
        }
        for table in session.tables.var_map.values() {
            physical_tables.insert(table.clone());
        }
        session.reset();
        drop(session);

        self.state.table_locks.lock().release_all_for_session(session_id);

        if !physical_tables.is_empty() {
            let mut storage = self.state.storage.write();
            for table_name in physical_tables {
                let Some(table) = storage
                    .catalog
                    .get_tables()
                    .iter()
                    .find(|table| table.name.eq_ignore_ascii_case(&table_name))
                    .cloned()
                else {
                    continue;
                };

                let schema_name = table.schema_name.clone();
                let _ = storage.catalog.drop_table(&schema_name, &table_name);
                storage.storage.remove_table(table.id);
                storage
                    .table_versions
                    .remove(&format!("{}.{}", normalize_identifier(&schema_name), normalize_identifier(&table_name)));
            }
        }
        Ok(())
    }

    fn close_session(&self, session_id: SessionId) -> Result<(), DbError> {
        self.state.table_locks.lock().release_all_for_session(session_id);
        let removed = self.state.sessions.remove(&session_id);
        if removed.is_none() {
            return Err(DbError::Execution(format!(
                "session {} not found",
                session_id
            )));
        }
        Ok(())
    }

    fn set_session_journal(
        &self,
        session_id: SessionId,
        journal: Box<dyn Journal>,
    ) -> Result<(), DbError> {
        let session_mutex = self.state.sessions.get(&session_id)
            .ok_or_else(|| DbError::Execution(format!("session {} not found", session_id)))?;
        let mut session = session_mutex.lock();
        session.journal = journal;
        Ok(())
    }
}
