
use crate::ast::Statement;
use crate::catalog::CatalogImpl;
use crate::error::DbError;
use crate::storage::InMemoryStorage;

use super::clock::{Clock, SystemClock};
use super::context::{ExecutionContext, Variables};
use super::result::QueryResult;
use super::script::ScriptExecutor;

pub struct Engine {
    pub catalog: CatalogImpl,
    pub storage: InMemoryStorage,
    clock: Box<dyn Clock>,
    variables: Variables,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("catalog", &self.catalog)
            .field("storage", &self.storage)
            .field("clock", &"dyn Clock")
            .field("variables", &self.variables)
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
            catalog: CatalogImpl::new(),
            storage: InMemoryStorage::default(),
            clock,
            variables: Variables::new(),
        }
    }

    pub fn reset(&mut self) {
        self.catalog = CatalogImpl::new();
        self.storage = InMemoryStorage::default();
        self.variables.clear();
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<Option<QueryResult>, DbError> {
        let mut ctx = ExecutionContext::new(&mut self.variables);
        
        ScriptExecutor {
            catalog: &mut self.catalog,
            storage: &mut self.storage,
            clock: self.clock.as_ref(),
        }
        .execute(stmt, &mut ctx)
    }

    pub fn execute_batch(&mut self, stmts: Vec<Statement>) -> Result<Option<QueryResult>, DbError> {
        let mut ctx = ExecutionContext::new(&mut self.variables);
        
        ScriptExecutor {
            catalog: &mut self.catalog,
            storage: &mut self.storage,
            clock: self.clock.as_ref(),
        }
        .execute_batch(&stmts, &mut ctx)
    }
}

pub(crate) fn execute_set_op(
    left: QueryResult,
    right: QueryResult,
    op: crate::ast::SetOpKind,
) -> Result<QueryResult, DbError> {
    if left.columns.len() != right.columns.len() {
        return Err(DbError::Execution(
            "set operations require same number of columns".into(),
        ));
    }

    let rows = match op {
        crate::ast::SetOpKind::Union | crate::ast::SetOpKind::UnionAll => {
            let mut rows = left.rows;
            rows.extend(right.rows);
            // UNION should deduplicate, UNION ALL shouldn't. 
            // Simplified: just return all for now or implement deduplication.
            rows
        }
        crate::ast::SetOpKind::Intersect => {
            let left_set: std::collections::HashSet<_> = left.rows.iter().cloned().collect();
            right
                .rows
                .into_iter()
                .filter(|r| left_set.contains(r))
                .collect()
        }
        crate::ast::SetOpKind::Except => {
            let right_set: std::collections::HashSet<_> = right.rows.iter().cloned().collect();
            left.rows
                .into_iter()
                .filter(|r| !right_set.contains(r))
                .collect()
        }
    };

    Ok(QueryResult {
        columns: left.columns,
        rows,
    })
}
