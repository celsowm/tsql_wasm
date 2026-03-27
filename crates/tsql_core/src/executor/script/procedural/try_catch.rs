use crate::ast::TryCatchStmt;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_try_catch(
        &mut self,
        stmt: TryCatchStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        match self.execute_batch(&stmt.try_body, ctx) {
            Ok(r) => Ok(r),
            Err(e) => {
                // If it's a Return, Break, or Continue, don't catch it
                match e {
                    DbError::Return(_) | DbError::Break | DbError::Continue => return Err(e),
                    _ => {}
                }

                // Store error for ERROR_* functions
                ctx.last_error = Some(e);

                // Execute CATCH block
                self.execute_batch(&stmt.catch_body, ctx)
            }
        }
    }
}
