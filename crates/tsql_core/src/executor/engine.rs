use std::fmt;

use crate::ast::Statement;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::InMemoryStorage;

use super::clock::{Clock, SystemClock};
use super::mutation::MutationExecutor;
use super::query::QueryExecutor;
use super::result::QueryResult;
use super::schema::SchemaExecutor;

pub struct Engine {
    pub catalog: Catalog,
    pub storage: InMemoryStorage,
    clock: Box<dyn Clock>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("catalog", &self.catalog)
            .field("storage", &self.storage)
            .field("clock", &"dyn Clock")
            .finish()
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Self {
        Self::with_clock(Box::new(SystemClock))
    }

    pub fn with_clock(clock: Box<dyn Clock>) -> Self {
        Self {
            catalog: Catalog::new(),
            storage: InMemoryStorage::default(),
            clock,
        }
    }

    pub fn reset(&mut self) {
        self.catalog = Catalog::new();
        self.storage = InMemoryStorage::default();
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<Option<QueryResult>, DbError> {
        match stmt {
            Statement::CreateTable(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .create_table(stmt)?;
                Ok(None)
            }
            Statement::DropTable(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .drop_table(stmt)?;
                Ok(None)
            }
            Statement::CreateSchema(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .create_schema(stmt)?;
                Ok(None)
            }
            Statement::DropSchema(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .drop_schema(stmt)?;
                Ok(None)
            }
            Statement::Insert(stmt) => {
                MutationExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                    clock: self.clock.as_ref(),
                }
                .execute_insert(stmt)?;
                Ok(None)
            }
            Statement::Select(stmt) => QueryExecutor {
                catalog: &self.catalog,
                storage: &self.storage,
                clock: self.clock.as_ref(),
            }
            .execute_select(stmt)
            .map(Some),
            Statement::Update(stmt) => {
                MutationExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                    clock: self.clock.as_ref(),
                }
                .execute_update(stmt)?;
                Ok(None)
            }
            Statement::Delete(stmt) => {
                MutationExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                    clock: self.clock.as_ref(),
                }
                .execute_delete(stmt)?;
                Ok(None)
            }
        }
    }
}
