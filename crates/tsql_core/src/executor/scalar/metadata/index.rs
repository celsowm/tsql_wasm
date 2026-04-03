use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;
use super::common::{
    eval_expr_to_value, index_by_id, index_by_name, table_by_object_id, table_has_primary_key,
    value_to_object_id,
};

pub(crate) fn eval_index_col(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("INDEX_COL expects 3 arguments".into()));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    let index_val = eval_expr_to_value(&args[1], row, ctx, catalog, storage, clock)?;
    let key_val = eval_expr_to_value(&args[2], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() || index_val.is_null() || key_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    let Some(table) = table_by_object_id(catalog, object_id) else {
        return Ok(Value::Null);
    };
    let Some(key_ordinal) = key_val.to_integer_i64() else {
        return Ok(Value::Null);
    };
    let index = if let Some(index_id) = index_val.to_integer_i64() {
        index_by_id(catalog, object_id, index_id as i32)
    } else {
        index_by_name(catalog, object_id, &index_val.to_string_value())
    };
    let Some(index) = index else {
        return Ok(Value::Null);
    };
    if key_ordinal <= 0 || (key_ordinal as usize) > index.column_ids.len() {
        return Ok(Value::Null);
    }
    let col_id = index.column_ids[(key_ordinal - 1) as usize];
    let Some(column) = table.columns.iter().find(|c| c.id == col_id) else {
        return Ok(Value::Null);
    };
    Ok(Value::NVarChar(column.name.clone()))
}

pub(crate) fn eval_indexkey_property(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 4 {
        return Err(DbError::Execution(
            "INDEXKEY_PROPERTY expects 4 arguments".into(),
        ));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    let index_val = eval_expr_to_value(&args[1], row, ctx, catalog, storage, clock)?;
    let key_val = eval_expr_to_value(&args[2], row, ctx, catalog, storage, clock)?;
    let prop_val = eval_expr_to_value(&args[3], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() || index_val.is_null() || key_val.is_null() || prop_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    let Some(key_ordinal) = key_val.to_integer_i64() else {
        return Ok(Value::Null);
    };
    let index = if let Some(index_id) = index_val.to_integer_i64() {
        index_by_id(catalog, object_id, index_id as i32)
    } else {
        index_by_name(catalog, object_id, &index_val.to_string_value())
    };
    let Some(index) = index else {
        return Ok(Value::Null);
    };
    if key_ordinal <= 0 || (key_ordinal as usize) > index.column_ids.len() {
        return Ok(Value::Null);
    }
    let col_id = index.column_ids[(key_ordinal - 1) as usize] as i32;
    let prop = prop_val.to_string_value().to_ascii_uppercase();
    Ok(match prop.as_str() {
        "COLUMNID" => Value::Int(col_id),
        "ISDESCENDING" => Value::Int(0),
        "KEYORDINAL" => Value::Int(key_ordinal as i32),
        _ => Value::Null,
    })
}

pub(crate) fn eval_indexproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("INDEXPROPERTY expects 3 arguments".into()));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    let index_val = eval_expr_to_value(&args[1], row, ctx, catalog, storage, clock)?;
    let prop_val = eval_expr_to_value(&args[2], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() || index_val.is_null() || prop_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    let Some(table) = table_by_object_id(catalog, object_id) else {
        return Ok(Value::Null);
    };
    let index = if let Some(index_id) = index_val.to_integer_i64() {
        index_by_id(catalog, object_id, index_id as i32)
    } else {
        index_by_name(catalog, object_id, &index_val.to_string_value())
    };
    let Some(index) = index else {
        return Ok(Value::Null);
    };
    let prop = prop_val.to_string_value().to_ascii_uppercase();
    Ok(match prop.as_str() {
        "INDEXID" => Value::Int(index.id as i32),
        "ISCLUSTERED" => Value::Int(if index.is_clustered { 1 } else { 0 }),
        "ISUNIQUE" => Value::Int(if index.is_unique { 1 } else { 0 }),
        "ISPRIMARYKEY" => Value::Int(if table_has_primary_key(table) { 1 } else { 0 }),
        "ISDISABLED" => Value::Int(0),
        "INDEXDEPTH" => Value::Int(1),
        "INDEXFILLFACTOR" => Value::Int(0),
        _ => Value::Null,
    })
}
