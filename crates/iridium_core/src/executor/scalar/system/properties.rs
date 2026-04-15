use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::super::clock::Clock;
use super::super::super::context::ExecutionContext;
use super::super::super::evaluator::eval_expr;
use super::super::super::model::ContextTable;

pub(crate) fn eval_serverproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "SERVERPROPERTY expects 1 argument".into(),
        ));
    }

    let property = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if property.is_null() {
        return Ok(Value::Null);
    }

    let name = property.to_string_value().to_uppercase();
    Ok(match name.as_str() {
        "EDITION" => Value::NVarChar("Developer Edition (64-bit)".to_string()),
        "ENGINEEDITION" => Value::Int(3),
        "PRODUCTVERSION" => Value::NVarChar("16.0.1000.6".to_string()),
        "PRODUCTLEVEL" => Value::NVarChar("RTM".to_string()),
        "PRODUCTUPDATELEVEL" => Value::NVarChar("".to_string()),
        "PRODUCTMAJORVERSION" => Value::NVarChar("16".to_string()),
        "PRODUCTMINORVERSION" => Value::NVarChar("0".to_string()),
        "PRODUCTBUILD" => Value::NVarChar("1000".to_string()),
        "PRODUCTBUILDTYPE" => Value::NVarChar("RTM".to_string()),
        "MACHINENAME" | "COMPUTERNAMEPHYSICALNETBIOS" => Value::NVarChar("localhost".to_string()),
        "SERVERNAME" => Value::NVarChar("localhost".to_string()),
        "INSTANCENAME"
        | "INSTANCEDEFAULTDATAPATH"
        | "INSTANCEDEFAULTLOGPATH"
        | "INSTANCEDEFAULTBACKUPPATH" => Value::Null,
        "COLLATION" | "SQLCOLLATION" => Value::NVarChar("SQL_Latin1_General_CP1_CI_AS".to_string()),
        "SQLCHARSETNAME" => Value::NVarChar("iso_1".to_string()),
        "SQLSORTORDERNAME" => Value::NVarChar("nocase_iso".to_string()),
        "ISINTEGRATEDSECURITYONLY" => Value::Int(0),
        "ISSINGLEUSER" => Value::Int(0),
        "ISXTPSUPPORTED" => Value::Int(0),
        "ISPOLYBASEINSTALLED" => Value::Int(0),
        "ISHADRENABLED" | "HADRMANAGERSTATUS" => Value::Int(0),
        "ISCLUSTERED" => Value::Int(0),
        "ISFULLTEXTINSTALLED" => Value::Int(0),
        "ISADVANCEDANALYTICSINSTALLED" => Value::Int(0),
        "ISTEMPDBMETADATAMEMORYOPTIMIZED" => Value::Int(0),
        "ISLOCALDB" | "ISLOCALDBENABLED" => Value::Int(0),
        "LCID" => Value::Int(1033),
        "BUILDCLRVERSION" => Value::NVarChar("v4.0.30319".to_string()),
        "RESOURCELASTMODIFIEDTIME" | "RESOURCELASTUPDATEDATETIME" => {
            Value::NVarChar("2024-01-01 00:00:00.000".to_string())
        }
        "RESOURCEVERSION" => Value::NVarChar("16.0.1000.6".to_string()),
        "FILESTREAMCONFIGUREDLEVEL" | "FILESTREAMLEVEL" => Value::Int(0),
        "FILESTREAMSHAREDFOLDER" | "FILESTREAMEFFECTIVELEVEL" | "FILESTREAMSHARENAME" => {
            Value::Null
        }
        "PROCESSORUSAGE" => Value::Int(0),
        "PATHSEPARATOR" => Value::NVarChar("\\".to_string()),
        "SERVERMANAGEMENTISINSTALLED" => Value::Int(0),
        "ISSTATEDSESSION" => Value::Int(0),
        "PROCESSID" => Value::Int(std::process::id() as i32),
        _ => Value::Null,
    })
}

pub(crate) fn eval_sessionproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "SESSIONPROPERTY expects 1 argument".into(),
        ));
    }

    let property = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if property.is_null() {
        return Ok(Value::Null);
    }

    let name = property.to_string_value().to_uppercase();
    Ok(match name.as_str() {
        "ANSI_NULLS" => Value::Int(if ctx.options.ansi_nulls { 1 } else { 0 }),
        "ANSI_PADDING" => Value::Int(if ctx.options.ansi_padding { 1 } else { 0 }),
        "ANSI_WARNINGS" => Value::Int(if ctx.options.ansi_warnings { 1 } else { 0 }),
        "ARITHABORT" => Value::Int(if ctx.options.arithabort { 1 } else { 0 }),
        "CONCAT_NULL_YIELDS_NULL" => {
            Value::Int(if ctx.options.concat_null_yields_null { 1 } else { 0 })
        }
        "NUMERIC_ROUNDABORT" => Value::Int(0),
        "QUOTED_IDENTIFIER" => Value::Int(if ctx.options.quoted_identifier { 1 } else { 0 }),
        _ => Value::Null,
    })
}

pub(crate) fn eval_fulltextserviceproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "FULLTEXTSERVICEPROPERTY expects 1 argument".into(),
        ));
    }

    let property = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if property.is_null() {
        return Ok(Value::Null);
    }

    let name = property.to_string_value().to_uppercase();
    Ok(match name.as_str() {
        "ISFULLTEXTINSTALLED" => Value::Int(0),
        _ => Value::Null,
    })
}

pub(crate) fn eval_connectionproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "CONNECTIONPROPERTY expects 1 argument".into(),
        ));
    }

    let property = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if property.is_null() {
        return Ok(Value::Null);
    }

    let name = property.to_string_value().to_uppercase();
    Ok(match name.as_str() {
        "NET_TRANSPORT" | "PHYSICAL_NET_TRANSPORT" => Value::NVarChar("TCP".to_string()),
        "PROTOCOL_TYPE" => Value::NVarChar("TSQL".to_string()),
        "AUTH_SCHEME" => Value::NVarChar("SQL".to_string()),
        "LOCAL_NET_ADDRESS" | "CLIENT_NET_ADDRESS" => Value::NVarChar("127.0.0.1".to_string()),
        "LOCAL_TCP_PORT" => Value::NVarChar("1433".to_string()),
        _ => Value::Null,
    })
}

pub(crate) fn eval_microsoft_version() -> Value {
    Value::Int(0x1000_1009)
}
