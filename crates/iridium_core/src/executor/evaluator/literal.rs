use crate::ast::Expr;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::identifier::{resolve_identifier, resolve_qualified_identifier};
use crate::executor::model::ContextTable;
use crate::types::Value;

pub(crate) fn eval_literal_expr(
    expr: &Expr,
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
) -> Result<Value, DbError> {
    match expr {
        Expr::Identifier(name) => resolve_identifier(row, name, ctx),
        Expr::QualifiedIdentifier(parts) => resolve_qualified_identifier(row, parts, ctx),
        Expr::Wildcard => Err(DbError::Execution(
            "wildcard is not a scalar expression".into(),
        )),
        Expr::QualifiedWildcard(_) => Err(DbError::Execution(
            "qualified wildcard is not a scalar expression".into(),
        )),
        Expr::Integer(v) => Ok(if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
            Value::Int(*v as i32)
        } else {
            Value::BigInt(*v)
        }),
        Expr::FloatLiteral(s) => crate::executor::value_ops::parse_numeric_literal(s),
        Expr::BinaryLiteral(bytes) => Ok(Value::Binary(bytes.clone())),
        Expr::String(v) => Ok(Value::VarChar(v.clone())),
        Expr::UnicodeString(v) => Ok(Value::NVarChar(v.clone())),
        Expr::Null => Ok(Value::Null),
        _ => Err(DbError::Execution(
            "eval_literal_expr called with non-literal expression".into(),
        )),
    }
}
