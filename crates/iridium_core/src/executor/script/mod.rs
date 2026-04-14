mod ddl;
mod dml;
mod procedural;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::result::QueryResult;
use super::schema::SchemaExecutor;
use crate::ast::statements::StatementVisitor;
use crate::ast::{
    CursorStatement, DdlStatement, DmlStatement, DropTableStmt, ObjectName, ProceduralStatement,
    SessionStatement, Statement, TransactionStatement, WithCteStmt,
};
use crate::catalog::Catalog;
use crate::error::{DbError, StmtOutcome, StmtResult};
use crate::storage::Storage;

pub struct ScriptExecutor<'a> {
    pub catalog: &'a mut dyn Catalog,
    pub storage: &'a mut dyn Storage,
    pub clock: &'a dyn Clock,
}

impl<'a> ScriptExecutor<'a> {
    pub fn execute(
        &mut self,
        stmt: Statement,
        ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        self.visit_statement(stmt, ctx)
    }

    pub fn execute_batch(
        &mut self,
        stmts: &[Statement],
        ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        let mut last_result = StmtOutcome::Ok(None);
        for stmt in stmts {
            match self.execute(stmt.clone(), ctx) {
                Ok(r) => {
                    if r.is_control_flow() {
                        return Ok(r);
                    }
                    last_result = r;
                }
                Err(e) => return Err(e),
            };
        }
        Ok(last_result)
    }

    fn cleanup_scope_table_vars(&mut self, ctx: &mut ExecutionContext<'_>) -> Result<(), DbError> {
        let dropped_physical = ctx.leave_scope_collect_table_vars();
        for physical in dropped_physical {
            let Some(table) = self
                .catalog
                .get_tables()
                .iter()
                .find(|table| table.name.eq_ignore_ascii_case(&physical))
                .cloned()
            else {
                continue;
            };

            self.schema(ctx).drop_table(DropTableStmt {
                name: ObjectName {
                    schema: Some(table.schema_name),
                    name: physical,
                },
            })?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn push_dirty_insert(
        &self,
        ctx: &mut ExecutionContext<'_>,
        table_name: &str,
        row: &crate::storage::StoredRow,
    ) {
        super::dirty_buffer::push_dirty_op(
            &ctx.session.dirty_buffer,
            ctx.session_id(),
            table_name.to_string(),
            super::dirty_buffer::DirtyOp::Insert { row: row.clone() },
        );
    }

    fn schema<'b>(&'b mut self, ctx: &'b ExecutionContext<'_>) -> SchemaExecutor<'b> {
        SchemaExecutor {
            catalog: self.catalog,
            storage: self.storage,
            session_options: &ctx.options,
        }
    }
}

impl<'a> StatementVisitor<ExecutionContext<'_>> for ScriptExecutor<'a> {
    fn visit_transaction(
        &mut self,
        stmt: TransactionStatement,
        _ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        match stmt {
            TransactionStatement::Begin(_)
            | TransactionStatement::Commit(_)
            | TransactionStatement::Rollback(_)
            | TransactionStatement::Save(_) => Err(DbError::Execution(
                "transaction control statements are only supported at top-level execution".into(),
            )),
        }
        .map(StmtOutcome::Ok)
    }

    fn visit_cursor(
        &mut self,
        stmt: CursorStatement,
        ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        match stmt {
            CursorStatement::OpenCursor(name) => self.execute_open_cursor(name, ctx),
            CursorStatement::FetchCursor(stmt) => self.execute_fetch_cursor(stmt, ctx),
            CursorStatement::CloseCursor(name) => self.execute_close_cursor(name, ctx),
            CursorStatement::DeallocateCursor(name) => self.execute_deallocate_cursor(name, ctx),
        }
        .map(StmtOutcome::Ok)
    }

    /// Rejects session statements (`SET IDENTITY_INSERT`, `SET TRANSACTION ISOLATION LEVEL`,
    /// `SET` options) at runtime by design.
    ///
    /// These statements require access to shared state owned by the engine/database layer and are
    /// dispatched there (see `dispatch.rs`). `ScriptExecutor` does not hold that state, so
    /// execution here is intentionally unsupported. The visitor trait still requires this method
    /// for compile-time exhaustiveness over `SessionStatement`.
    fn visit_session(
        &mut self,
        stmt: SessionStatement,
        _ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        match stmt {
            SessionStatement::SetIdentityInsert(_) => Err(DbError::Execution(
                "SET IDENTITY_INSERT is handled at engine level".into(),
            )),
            SessionStatement::SetTransactionIsolationLevel(_) | SessionStatement::SetOption(_) => {
                Err(DbError::Execution(
                    "session option statements are handled at engine level".into(),
                ))
            }
        }
        .map(StmtOutcome::Ok)
    }

    fn visit_dml(
        &mut self,
        stmt: DmlStatement,
        ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        self.execute_dml(stmt, ctx)
    }

    fn visit_ddl(
        &mut self,
        stmt: DdlStatement,
        ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        self.execute_ddl(stmt, ctx).map(StmtOutcome::Ok)
    }

    fn visit_procedural(
        &mut self,
        stmt: ProceduralStatement,
        ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        self.execute_procedural(stmt, ctx)
    }

    fn visit_with_cte(
        &mut self,
        stmt: WithCteStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        self.execute_with_cte(stmt, ctx).map(StmtOutcome::Ok)
    }
}
