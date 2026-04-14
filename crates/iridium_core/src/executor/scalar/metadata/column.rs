use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::common::{
    eval_expr_to_value, storage_length, table_by_object_id, table_column_by_ordinal,
    value_to_object_id,
};
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;

pub(crate) fn eval_col_name(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("COL_NAME expects 2 arguments".into()));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    let ordinal_val = eval_expr_to_value(&args[1], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() || ordinal_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    let Some(ordinal) = ordinal_val.to_integer_i64() else {
        return Ok(Value::Null);
    };
    let Some(table) = table_by_object_id(catalog, object_id) else {
        return Ok(Value::Null);
    };
    Ok(match table_column_by_ordinal(table, ordinal as i32) {
        Some(column) => Value::NVarChar(column.name.clone()),
        None => Value::Null,
    })
}

pub(crate) fn eval_col_length(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("COL_LENGTH expects 2 arguments".into()));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    let column_val = eval_expr_to_value(&args[1], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() || column_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    let Some(table) = table_by_object_id(catalog, object_id) else {
        return Ok(Value::Null);
    };
    let column_name = column_val.to_string_value();
    let Some(column) = table
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&column_name))
    else {
        return Ok(Value::Null);
    };
    Ok(Value::Int(storage_length(&column.data_type)))
}
