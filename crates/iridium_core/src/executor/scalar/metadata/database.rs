use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::common::eval_expr_to_value;
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::metadata::database_catalog::{
    current_database_id, current_database_name, database_id_for_name, database_name_for_id,
    recovery_model_for_name,
};
use crate::executor::model::ContextTable;

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
    let db_name = match db_val {
        Value::Int(v) => database_name_for_id(v).map(|name| name.to_string()),
        Value::BigInt(v) => database_name_for_id(v as i32).map(|name| name.to_string()),
        Value::SmallInt(v) => database_name_for_id(v as i32).map(|name| name.to_string()),
        Value::TinyInt(v) => database_name_for_id(v as i32).map(|name| name.to_string()),
        _ => None,
    }
    .unwrap_or_else(|| db_val.to_string_value());
    let is_known_db = database_id_for_name(&db_name).is_some();
    let active_db = current_database_name(ctx).to_string();
    if !is_known_db && !active_db.eq_ignore_ascii_case(&db_name) {
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
        "COMPATIBILITYLEVEL" => Value::Int(160),
        "RECOVERY" | "RECOVERYMODEL" => Value::NVarChar(
            recovery_model_for_name(&db_name)
                .unwrap_or("FULL")
                .to_string(),
        ),
        "ISAUTOSHRINK" | "ISAUTO_SHRINK_ON" => Value::Int(0),
        "ISAUTOCLOSE" | "ISAUTO_CLOSE_ON" => Value::Int(0),
        "ISFULLTEXTENABLED" => Value::Int(0),
        "ISBROKERPRIORITYHONORED" => Value::Int(0),
        "ISBROKERENABLED" | "SERVICEBROKERENABLED" => Value::Int(0),
        "ISARITHMETICABORT" | "ISARITHMETIC_ABORT" => Value::Int(0),
        "ISCONCATNULLYIELDSNULL" | "ISCONCAT_NULL_YIELDS_NULL" => Value::Int(1),
        "ISNUMERICROUNDABORT" | "ISNUMERIC_ROUNDABORT" => Value::Int(0),
        "ISQUOTEDIDENTIFIER" | "ISQUOTED_IDENTIFIER" => Value::Int(1),
        "ISRECURSIVETRIGENABLED" | "ISRECURSIVE_TRIGGERS_ENABLED" => Value::Int(0),
        "ISDATEPRIORITYYMD" | "ISDATE_CORRELATION_OPTIMIZATION" => Value::Int(0),
        "ISPARAMETERIZATIONFORCED" | "ISPARAMETERIZATION_FORCED" => Value::Int(0),
        "ISSUPPLEMENTALLOGGINGENABLED" | "ISSUPPLEMENTAL_LOGGING" => Value::Int(0),
        "ISTRANSFORMNOISEWORDSENABLED" | "ISTRANSFORM_NOISE_WORDS" => Value::Int(0),
        "ISTRUSTWORTHY" | "ISTRUSTWORTHY_ON" => Value::Int(0),
        "ISCURSORDEFAULT" | "ISCURSORCLOSE_ON_COMMIT" => Value::Int(0),
        "ISINSTANDBY" => Value::Int(0),
        "ISAUTOCREATESTATISTICS" | "ISAUTOCREATESTATISTICS_ON" => Value::Int(1),
        "ISAUTOUPDATESTATISTICS" | "ISAUTOUPDATESTATISTICS_ON" => Value::Int(1),
        "ISAUTOUPDATESTATISTICSASYNC" | "ISAUTOUPDATESTATISTICSASYNC_ON" => Value::Int(0),
        "ISENCRYPTED" | "ISENCRYPTION_ON" => Value::Int(0),
        "ISPUBLISHED" | "ISSUBSCRIBED" | "ISMERGEPUBLISHED" | "ISDISTRIBUTOR" => Value::Int(0),
        "VERSION" => Value::Int(957),
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

pub(crate) fn eval_db_name(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "DB_NAME expects 0 or 1 arguments".into(),
        ));
    }
    if let Some(arg) = args.first() {
        let name = match arg {
            Expr::Integer(v) => database_name_for_id(*v as i32),
            Expr::String(s) | Expr::UnicodeString(s) => {
                if database_id_for_name(s).is_some() {
                    Some(s.as_str())
                } else {
                    None
                }
            }
            _ => None,
        };
        return Ok(name
            .map(|name| Value::NVarChar(name.to_string()))
            .unwrap_or(Value::Null));
    }
    Ok(Value::NVarChar(current_database_name(ctx).to_string()))
}

pub(crate) fn eval_db_id(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution("DB_ID expects 0 or 1 arguments".into()));
    }
    if let Some(arg) = args.first() {
        return Ok(match arg {
            Expr::Integer(v) => database_name_for_id(*v as i32)
                .map(|_| Value::Int(*v as i32))
                .unwrap_or(Value::Null),
            Expr::String(s) | Expr::UnicodeString(s) => database_id_for_name(s)
                .map(Value::Int)
                .unwrap_or(Value::Null),
            _ => Value::Null,
        });
    }
    Ok(Value::Int(current_database_id(ctx)))
}
