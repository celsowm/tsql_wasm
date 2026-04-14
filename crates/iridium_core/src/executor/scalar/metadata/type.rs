use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::common::{
    eval_expr_to_value, parse_object_parts, resolve_type_id, resolve_type_name, type_precision,
    type_scale, value_to_object_id,
};
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;

pub(crate) fn eval_type_id(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("TYPE_ID expects 1 argument".into()));
    }
    let ty_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    if ty_val.is_null() {
        return Ok(Value::Null);
    }
    Ok(match resolve_type_id(catalog, &ty_val.to_string_value()) {
        Some(id) => Value::Int(id),
        None => Value::Null,
    })
}

pub(crate) fn eval_type_name(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("TYPE_NAME expects 1 argument".into()));
    }
    let ty_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    if ty_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(type_id) = ty_val
        .to_integer_i64()
        .map(|v| v as i32)
        .or_else(|| value_to_object_id(&ty_val, catalog, None))
    else {
        return Ok(Value::Null);
    };
    Ok(match resolve_type_name(catalog, type_id) {
        Some(name) => Value::NVarChar(name),
        None => Value::Null,
    })
}

pub(crate) fn eval_typeproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "TYPEPROPERTY expects 2 arguments".into(),
        ));
    }
    let ty_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    let prop_val = eval_expr_to_value(&args[1], row, ctx, catalog, storage, clock)?;
    if ty_val.is_null() || prop_val.is_null() {
        return Ok(Value::Null);
    }
    let type_name = ty_val.to_string_value();
    let prop = prop_val.to_string_value().to_ascii_uppercase();
    let (schema, name) = parse_object_parts(&type_name);
    let schema = schema.unwrap_or("dbo");
    if catalog.find_table_type(schema, name).is_some() {
        return Ok(match prop.as_str() {
            "ISTABLETYPE" => Value::Int(1),
            "ALLOWSNULL" => Value::Int(1),
            "OWNERID" => catalog
                .get_schema_id(schema)
                .map(|id| Value::Int(id as i32))
                .unwrap_or(Value::Null),
            _ => Value::Null,
        });
    }
    let builtin = name.to_ascii_lowercase();
    Ok(match prop.as_str() {
        "ALLOWSNULL" => Value::Int(1),
        "ISTABLETYPE" => Value::Int(0),
        "OWNERID" => Value::Null,
        "PRECISION" => type_precision(&builtin)
            .map(Value::Int)
            .unwrap_or(Value::Null),
        "SCALE" => type_scale(&builtin).map(Value::Int).unwrap_or(Value::Null),
        "USESANSITRIM" => {
            if matches!(
                builtin.as_str(),
                "char" | "varchar" | "nchar" | "nvarchar" | "binary" | "varbinary"
            ) {
                Value::Int(1)
            } else {
                Value::Int(0)
            }
        }
        _ => Value::Null,
    })
}
