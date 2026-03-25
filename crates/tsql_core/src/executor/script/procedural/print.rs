use crate::ast::Expr;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_print(
        &mut self,
        expr: Expr,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<crate::executor::result::QueryResult>, DbError> {
        let val = eval_expr(
            &expr,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;
        ctx.print_output.push(val.to_string_value());
        Ok(None)
    }
}
