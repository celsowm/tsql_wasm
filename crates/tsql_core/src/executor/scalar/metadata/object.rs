use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;
use super::common::{
    eval_expr_to_value, object_definition_from_id, object_name_from_id,
    object_schema_name_from_id, schema_name_by_id, value_to_object_id,
};

pub(crate) fn eval_schema_id(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution("SCHEMA_ID expects 0 or 1 arguments".into()));
    }
    let schema_name = if args.is_empty() {
        "dbo".to_string()
    } else {
        let v = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
        if v.is_null() {
            return Ok(Value::Null);
        }
        v.to_string_value()
    };
    Ok(match catalog.get_schema_id(&schema_name) {
        Some(id) => Value::Int(id as i32),
        None => Value::Null,
    })
}

pub(crate) fn eval_schema_name(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution("SCHEMA_NAME expects 0 or 1 arguments".into()));
    }
    let schema_id = if args.is_empty() {
        catalog.get_schema_id("dbo")
    } else {
        let v = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
        if v.is_null() {
            return Ok(Value::Null);
        }
        match v {
            Value::Int(id) => Some(id as u32),
            Value::BigInt(id) => Some(id as u32),
            Value::SmallInt(id) => Some(id as u32),
            Value::TinyInt(id) => Some(id as u32),
            Value::VarChar(_) | Value::NVarChar(_) | Value::Char(_) | Value::NChar(_) => {
                catalog.get_schema_id(&v.to_string_value())
            }
            _ => None,
        }
    };
    Ok(match schema_id.and_then(|id| schema_name_by_id(catalog, id)) {
        Some(name) => Value::NVarChar(name),
        None => Value::Null,
    })
}

pub(crate) fn eval_object_name(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution("OBJECT_NAME expects 1 or 2 arguments".into()));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    Ok(match object_name_from_id(catalog, object_id) {
        Some(name) => Value::NVarChar(name),
        None => Value::Null,
    })
}

pub(crate) fn eval_object_schema_name(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution(
            "OBJECT_SCHEMA_NAME expects 1 or 2 arguments".into(),
        ));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    Ok(match object_schema_name_from_id(catalog, object_id) {
        Some(name) => Value::NVarChar(name),
        None => Value::Null,
    })
}

pub(crate) fn eval_object_definition(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution(
            "OBJECT_DEFINITION expects 1 or 2 arguments".into(),
        ));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    Ok(match object_definition_from_id(catalog, object_id) {
        Some(def) => Value::NVarChar(def),
        None => Value::Null,
    })
}

pub(crate) fn eval_procid(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match ctx.current_procid() {
        Some(id) => Value::Int(id),
        None => Value::Null,
    })
}
