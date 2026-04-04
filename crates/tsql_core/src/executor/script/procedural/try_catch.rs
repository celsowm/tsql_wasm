use crate::ast::TryCatchStmt;
use crate::error::StmtResult;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_try_catch(
        &mut self,
        stmt: TryCatchStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> StmtResult<Option<QueryResult>> {
        match self.execute_batch(&stmt.try_body, ctx) {
            Ok(outcome) => {
                // Control flow signals pass through TRY...CATCH unchanged
                if outcome.is_control_flow() {
                    return Ok(outcome);
                }
                Ok(outcome)
            }
            Err(e) => {
                // Store error for ERROR_* functions
                ctx.frame.last_error = Some(e);

                // Execute CATCH block
                self.execute_batch(&stmt.catch_body, ctx)
            }
        }
    }
}
