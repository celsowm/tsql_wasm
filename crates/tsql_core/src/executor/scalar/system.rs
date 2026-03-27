use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::types::Value;
use crate::storage::Storage;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr;
use super::super::model::ContextTable;

pub(crate) fn eval_object_id(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("OBJECT_ID expects 1 argument".into()));
    }

    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let raw = val.to_string_value();
    let cleaned = raw.trim().trim_matches('[').trim_matches(']');
    let parts: Vec<&str> = cleaned.split('.').collect();
    let (schema, name) = if parts.len() == 2 {
        (parts[0].trim(), parts[1].trim())
    } else {
        ("dbo", cleaned.trim())
    };

    Ok(match catalog.object_id(schema, name) {
        Some(id) => Value::Int(id),
        None => Value::Null,
    })
}

pub(crate) fn eval_columnproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution(
            "COLUMNPROPERTY expects 3 arguments".into(),
        ));
    }

    let object_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let column_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let property_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;

    if object_val.is_null() || column_val.is_null() || property_val.is_null() {
        return Ok(Value::Null);
    }

    let object_id = match object_val {
        Value::Int(v) => Some(v),
        Value::BigInt(v) => Some(v as i32),
        Value::SmallInt(v) => Some(v as i32),
        Value::TinyInt(v) => Some(v as i32),
        Value::VarChar(_) | Value::NVarChar(_) | Value::Char(_) | Value::NChar(_) => {
            let raw = object_val.to_string_value();
            let cleaned = raw.trim().trim_matches('[').trim_matches(']');
            let parts: Vec<&str> = cleaned.split('.').collect();
            let (schema, name) = if parts.len() == 2 {
                (parts[0].trim(), parts[1].trim())
            } else {
                ("dbo", cleaned.trim())
            };
            catalog.object_id(schema, name)
        }
        _ => None,
    };

    let Some(object_id) = object_id else {
        return Ok(Value::Null);
    };
    let column_name = column_val.to_string_value();
    let property_name = property_val.to_string_value().to_uppercase();

    let Some(table) = catalog
        .get_tables()
        .iter()
        .find(|t| t.id as i32 == object_id)
    else {
        return Ok(Value::Null);
    };

    let Some((ordinal, col)) = table
        .columns
        .iter()
        .enumerate()
        .find(|(_, c)| c.name.eq_ignore_ascii_case(&column_name))
    else {
        return Ok(Value::Null);
    };

    match property_name.as_str() {
        "ALLOWSNULL" => Ok(Value::Int(if col.nullable { 1 } else { 0 })),
        "ISCOMPUTED" => Ok(Value::Int(if col.computed_expr.is_some() { 1 } else { 0 })),
        "COLUMNID" => Ok(Value::Int((ordinal + 1) as i32)),
        _ => Ok(Value::Null),
    }
}

pub(crate) fn eval_ident_current(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "IDENT_CURRENT expects 1 argument".into(),
        ));
    }
    let val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if val.is_null() {
        return Ok(Value::Null);
    }
    let raw = val.to_string_value();
    let parts: Vec<&str> = raw.split('.').collect();
    let (schema, name) = if parts.len() == 2 {
        (parts[0].trim(), parts[1].trim())
    } else {
        ("dbo", raw.trim())
    };
    let Some(table) = catalog.find_table(schema, name) else {
        return Ok(Value::Null);
    };
    for col in &table.columns {
        if let Some(identity) = &col.identity {
            return Ok(Value::BigInt(identity.current - identity.increment));
        }
    }
    Ok(Value::Null)
}

pub(crate) fn deterministic_uuid(state: &mut u64) -> String {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    let bytes = state.to_be_bytes();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[0] ^ bytes[4], bytes[1] ^ bytes[5],
        bytes[2] ^ bytes[6], bytes[3] ^ bytes[7],
        bytes[4] ^ bytes[0], bytes[5] ^ bytes[1],
        bytes[6] ^ bytes[2], bytes[7] ^ bytes[3]
    )
}

pub(crate) fn deterministic_rand(state: &mut u64) -> f64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    let bits = (*state >> 33) as u32;
    bits as f64 / (1u64 << 31) as f64
}

pub(crate) fn eval_error_message(
    ctx: &ExecutionContext,
) -> Result<Value, DbError> {
    Ok(match &ctx.last_error {
        Some(e) => Value::VarChar(e.to_string()),
        None => Value::Null,
    })
}

pub(crate) fn eval_error_number(
    ctx: &ExecutionContext,
) -> Result<Value, DbError> {
    Ok(match &ctx.last_error {
        Some(_) => Value::Int(50000), // Default error number
        None => Value::Null,
    })
}

pub(crate) fn eval_error_severity(
    ctx: &ExecutionContext,
) -> Result<Value, DbError> {
    Ok(match &ctx.last_error {
        Some(_) => Value::Int(16), // Default severity
        None => Value::Null,
    })
}

pub(crate) fn eval_error_state(
    ctx: &ExecutionContext,
) -> Result<Value, DbError> {
    Ok(match &ctx.last_error {
        Some(_) => Value::Int(1), // Default state
        None => Value::Null,
    })
}
