use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;
use super::common::eval_expr_to_value;

pub(crate) fn eval_databasepropertyex(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "DATABASEPROPERTYEX expects 2 arguments".into(),
        ));
    }
    let db_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    let prop_val = eval_expr_to_value(&args[1], row, ctx, catalog, storage, clock)?;
    if db_val.is_null() || prop_val.is_null() {
        return Ok(Value::Null);
    }
    let db_name = db_val.to_string_value();
    let active_db = ctx
        .metadata
        .database
        .as_ref()
        .unwrap_or(&ctx.metadata.original_database);
    if !active_db.eq_ignore_ascii_case(&db_name) {
        return Ok(Value::Null);
    }
    let prop = prop_val.to_string_value().to_ascii_uppercase();
    Ok(match prop.as_str() {
        "COLLATION" => Value::NVarChar("SQL_Latin1_General_CP1_CI_AS".to_string()),
        "STATUS" => Value::NVarChar("ONLINE".to_string()),
        "UPDATEABILITY" => Value::NVarChar("READ_WRITE".to_string()),
        "USERACCESS" => Value::NVarChar("MULTI_USER".to_string()),
        "ISREADONLY" => Value::Int(0),
        "ISANSINULLDEFAULT" | "ISANSI_NULL_DEFAULT" => {
            Value::Int(if ctx.metadata.ansi_nulls { 1 } else { 0 })
        }
        _ => Value::Null,
    })
}

pub(crate) fn eval_original_db_name(
    args: &[Expr],
    ctx: &ExecutionContext,
) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution(
            "ORIGINAL_DB_NAME expects no arguments".into(),
        ));
    }
    Ok(Value::NVarChar(ctx.metadata.original_database.clone()))
}
