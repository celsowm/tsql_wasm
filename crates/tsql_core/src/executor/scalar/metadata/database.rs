use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::common::eval_expr_to_value;
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
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
        Value::Int(1) | Value::BigInt(1) | Value::SmallInt(1) | Value::TinyInt(1) => {
            "master".to_string()
        }
        Value::Int(2) | Value::BigInt(2) | Value::SmallInt(2) | Value::TinyInt(2) => {
            "tempdb".to_string()
        }
        Value::Int(3) | Value::BigInt(3) | Value::SmallInt(3) | Value::TinyInt(3) => {
            "model".to_string()
        }
        Value::Int(4) | Value::BigInt(4) | Value::SmallInt(4) | Value::TinyInt(4) => {
            "msdb".to_string()
        }
        Value::Int(5) | Value::BigInt(5) | Value::SmallInt(5) | Value::TinyInt(5) => {
            "tsql_wasm".to_string()
        }
        _ => db_val.to_string_value(),
    };
    let is_known_db = matches!(
        db_name.to_ascii_lowercase().as_str(),
        "master" | "tempdb" | "model" | "msdb" | "tsql_wasm"
    );
    let active_db = ctx
        .metadata
        .database
        .as_ref()
        .unwrap_or(&ctx.metadata.original_database)
        .to_string();
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
        "RECOVERY" | "RECOVERYMODEL" => {
            Value::NVarChar(if db_name.eq_ignore_ascii_case("tempdb") {
                "SIMPLE".to_string()
            } else {
                "FULL".to_string()
            })
        }
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
            Expr::Integer(1) => "master".to_string(),
            Expr::Integer(2) => "tempdb".to_string(),
            Expr::Integer(3) => "model".to_string(),
            Expr::Integer(4) => "msdb".to_string(),
            Expr::Integer(5) => "tsql_wasm".to_string(),
            Expr::String(s) | Expr::UnicodeString(s) => {
                if s.eq_ignore_ascii_case("master")
                    || s.eq_ignore_ascii_case("tempdb")
                    || s.eq_ignore_ascii_case("model")
                    || s.eq_ignore_ascii_case("msdb")
                    || s.eq_ignore_ascii_case("tsql_wasm")
                {
                    s.clone()
                } else {
                    return Ok(Value::Null);
                }
            }
            _ => return Ok(Value::Null),
        };
        return Ok(Value::NVarChar(name));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .database
            .clone()
            .unwrap_or_else(|| "master".to_string()),
    ))
}

pub(crate) fn eval_db_id(args: &[Expr], _ctx: &ExecutionContext) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution("DB_ID expects 0 or 1 arguments".into()));
    }
    if let Some(arg) = args.first() {
        return Ok(match arg {
            Expr::Integer(1) => Value::Int(1),
            Expr::Integer(2) => Value::Int(2),
            Expr::Integer(3) => Value::Int(3),
            Expr::Integer(4) => Value::Int(4),
            Expr::Integer(5) => Value::Int(5),
            Expr::String(s) | Expr::UnicodeString(s) if s.eq_ignore_ascii_case("master") => {
                Value::Int(1)
            }
            Expr::String(s) | Expr::UnicodeString(s) if s.eq_ignore_ascii_case("tempdb") => {
                Value::Int(2)
            }
            Expr::String(s) | Expr::UnicodeString(s) if s.eq_ignore_ascii_case("model") => {
                Value::Int(3)
            }
            Expr::String(s) | Expr::UnicodeString(s) if s.eq_ignore_ascii_case("msdb") => {
                Value::Int(4)
            }
            Expr::String(s) | Expr::UnicodeString(s) if s.eq_ignore_ascii_case("tsql_wasm") => {
                Value::Int(5)
            }
            _ => Value::Null,
        });
    }
    Ok(Value::Int(1))
}
