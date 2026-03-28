use crate::ast::{IfStmt, WhileStmt};
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_if(
        &mut self,
        stmt: IfStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        let cond = super::super::evaluator::eval_expr(
            &stmt.condition,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;
        let truthy = super::super::value_ops::truthy(&cond);
        if truthy {
            self.execute_batch(&stmt.then_body, ctx)
        } else if let Some(ref else_body) = stmt.else_body {
            self.execute_batch(else_body, ctx)
        } else {
            Ok(None)
        }
    }

    pub(crate) fn execute_while(
        &mut self,
        stmt: WhileStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        ctx.loop_depth += 1;
        let loop_result = (|| {
            let mut last_batch: Result<Option<QueryResult>, DbError> = Ok(None);
            loop {
                let cond = super::super::evaluator::eval_expr(
                    &stmt.condition,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                if !super::super::value_ops::truthy(&cond) {
                    break;
                }

                match self.execute_batch(&stmt.body, ctx) {
                    Err(DbError::Break) => {
                        last_batch = Ok(None);
                        break;
                    }
                    Err(DbError::Continue) => {
                        last_batch = Ok(None);
                        continue;
                    }
                    Err(DbError::Return(v)) => return Err(DbError::Return(v)),
                    other => {
                        last_batch = other;
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
    ) -> Result<Option<QueryResult>, DbError> {
        let value = if let Some(ref e) = expr {
            Some(super::super::evaluator::eval_expr(
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
        Err(DbError::Return(value))
    }
}
