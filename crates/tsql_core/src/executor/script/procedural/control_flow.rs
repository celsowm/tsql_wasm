use crate::ast::{IfStmt, WhileStmt};
use crate::error::{StmtOutcome, StmtResult};
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_if(
        &mut self,
        stmt: IfStmt,
        ctx: &mut ExecutionContext,
    ) -> StmtResult<Option<QueryResult>> {
        let cond = crate::executor::evaluator::eval_expr(
            &stmt.condition,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;
        let truthy = crate::executor::value_ops::truthy(&cond);
        if truthy {
            self.execute_batch(&stmt.then_body, ctx)
        } else if let Some(ref else_body) = stmt.else_body {
            self.execute_batch(else_body, ctx)
        } else {
            Ok(StmtOutcome::Ok(None))
        }
    }

    pub(crate) fn execute_while(
        &mut self,
        stmt: WhileStmt,
        ctx: &mut ExecutionContext,
    ) -> StmtResult<Option<QueryResult>> {
        ctx.loop_depth += 1;
        let loop_result = (|| {
            let mut last_batch: StmtResult<Option<QueryResult>> = Ok(StmtOutcome::Ok(None));
            loop {
                let cond = crate::executor::evaluator::eval_expr(
                    &stmt.condition,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                if !crate::executor::value_ops::truthy(&cond) {
                    break;
                }

                match self.execute_batch(&stmt.body, ctx) {
                    Ok(StmtOutcome::Break) => {
                        last_batch = Ok(StmtOutcome::Ok(None));
                        break;
                    }
                    Ok(StmtOutcome::Continue) => {
                        last_batch = Ok(StmtOutcome::Ok(None));
                        continue;
                    }
                    Ok(StmtOutcome::Return(v)) => return Ok(StmtOutcome::Return(v)),
                    other => {
                        last_batch = other;
                        if last_batch.is_err() || last_batch.as_ref().map_or(false, |o| o.is_control_flow()) {
                            return last_batch;
                        }
                    }
                }
            }
            last_batch
        })();
        ctx.loop_depth -= 1;
        loop_result
    }

    pub(crate) fn execute_return(
        &mut self,
        expr: Option<crate::ast::Expr>,
        ctx: &mut ExecutionContext,
    ) -> StmtResult<Option<QueryResult>> {
        let value = if let Some(ref e) = expr {
            Some(crate::executor::evaluator::eval_expr(
                e,
                &[],
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?)
        } else {
            None
        };
        Ok(StmtOutcome::Return(value))
    }
}
