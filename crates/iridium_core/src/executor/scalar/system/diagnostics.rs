use crate::error::DbError;
use crate::types::Value;

use super::super::super::context::ExecutionContext;

pub(crate) fn eval_error_message(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match &ctx.frame.last_error {
        Some(e) => Value::VarChar(e.to_string()),
        None => Value::Null,
    })
}

pub(crate) fn eval_error_number(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match &ctx.frame.last_error {
        Some(_) => Value::Int(50000), // Default error number
        None => Value::Null,
    })
}

pub(crate) fn eval_error_severity(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match &ctx.frame.last_error {
        Some(_) => Value::Int(16), // Default severity
        None => Value::Null,
    })
}

pub(crate) fn eval_error_state(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match &ctx.frame.last_error {
        Some(_) => Value::Int(1), // Default state
        None => Value::Null,
    })
}
