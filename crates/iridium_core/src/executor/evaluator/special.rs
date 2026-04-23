use crate::ast::Expr;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::types::Value;

pub(crate) fn eval_special_runtime_expr(
    expr: &Expr,
    ctx: &mut ExecutionContext,
) -> Result<Value, DbError> {
    match expr {
        Expr::WindowFunction { .. } => {
            let key = format!("{:?}", expr);
            if let Some(val) = ctx.get_window_value(&key) {
                Ok(val)
            } else {
                Err(DbError::Execution(
                    "window function value not found in context".into(),
                ))
            }
        }
        Expr::NextValueFor { sequence_name } => {
            let _ = sequence_name;
            Ok(Value::BigInt(1))
        }
        _ => Err(DbError::Execution(
            "eval_special_runtime_expr called with non-special expression".into(),
        )),
    }
}
