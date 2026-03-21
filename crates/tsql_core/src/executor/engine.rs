use std::fmt;

use crate::ast::{SetOpKind, Statement};
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
            Statement::TruncateTable(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                let table_name = stmt.name.name.clone();
                let schema_id = self
                    .catalog
                    .get_schema_id(&schema)
                    .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;
                let table = self
                    .catalog
                    .tables
                    .iter()
                    .find(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(&table_name))
                    .ok_or_else(|| {
                        DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
                    })?
                    .clone();
                if let Some(rows) = self.storage.tables.get_mut(&table.id) {
                    rows.clear();
                }
                Ok(None)
            }
            Statement::AlterTable(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .alter_table(stmt)?;
                Ok(None)
            }
            Statement::SetOp(stmt) => {
                let left_result = self.execute(*stmt.left)?;
                let right_result = self.execute(*stmt.right)?;

                match (left_result, right_result) {
                    (Some(left), Some(right)) => {
                        let result = execute_set_op(left, right, stmt.op)?;
                        Ok(Some(result))
                    }
                    _ => Err(DbError::Execution(
                        "set operations require both sides to return results".into(),
                    )),
                }
            }
        }
    }
}

fn execute_set_op(
    left: QueryResult,
    right: QueryResult,
    op: SetOpKind,
) -> Result<QueryResult, DbError> {
    if left.columns.len() != right.columns.len() {
        return Err(DbError::Execution(
            "set operations require the same number of columns".into(),
        ));
    }

    let rows = match op {
        SetOpKind::UnionAll => {
            let mut r = left.rows;
            r.extend(right.rows);
            r
        }
        SetOpKind::Union => {
            let mut r = left.rows;
            for row in right.rows {
                if !r.iter().any(|existing| existing == &row) {
                    r.push(row);
                }
            }
            r
        }
        SetOpKind::Intersect => left
            .rows
            .into_iter()
            .filter(|row| right.rows.iter().any(|r| r == row))
            .collect(),
        SetOpKind::Except => left
            .rows
            .into_iter()
            .filter(|row| !right.rows.iter().any(|r| r == row))
            .collect(),
    };

    Ok(QueryResult {
        columns: left.columns,
        rows,
    })
}
