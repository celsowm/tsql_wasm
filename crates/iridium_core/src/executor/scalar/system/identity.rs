use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::super::clock::Clock;
use super::super::super::context::ExecutionContext;
use super::super::super::evaluator::eval_expr;
use super::super::super::model::ContextTable;

pub(crate) fn eval_suser_sname(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "SUSER_SNAME expects 0 or 1 arguments".into(),
        ));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .user
            .clone()
            .unwrap_or_else(|| "sa".to_string()),
    ))
}

pub(crate) fn eval_suser_id(args: &[Expr], _ctx: &ExecutionContext) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "SUSER_ID expects 0 or 1 arguments".into(),
        ));
    }
    Ok(Value::Int(1))
}

pub(crate) fn eval_user_name(args: &[Expr], _ctx: &ExecutionContext) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "USER_NAME expects 0 or 1 arguments".into(),
        ));
    }
    Ok(Value::NVarChar("dbo".to_string()))
}

pub(crate) fn eval_user_id(args: &[Expr], _ctx: &ExecutionContext) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "USER_ID expects 0 or 1 arguments".into(),
        ));
    }
    Ok(Value::Int(1))
}

pub(crate) fn eval_database_principal_id(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "DATABASE_PRINCIPAL_ID expects 0 or 1 arguments".into(),
        ));
    }
    let name = if args.is_empty() {
        ctx.metadata
            .user
            .clone()
            .unwrap_or_else(|| "dbo".to_string())
    } else {
        let v = eval_expr(args.first().unwrap(), row, ctx, catalog, storage, clock)?;
        if v.is_null() {
            return Ok(Value::Null);
        }
        v.to_string_value()
    };

    Ok(match name.to_ascii_lowercase().as_str() {
        "dbo" => Value::Int(1),
        "guest" => Value::Int(2),
        _ => Value::Null,
    })
}

pub(crate) fn eval_database_principal_name(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "DATABASE_PRINCIPAL_NAME expects 0 or 1 arguments".into(),
        ));
    }
    let id = if args.is_empty() {
        Some(1)
    } else {
        let v = eval_expr(args.first().unwrap(), row, ctx, catalog, storage, clock)?;
        if v.is_null() {
            return Ok(Value::Null);
        }
        v.to_integer_i64()
    };

    Ok(match id {
        Some(1) => Value::NVarChar("dbo".to_string()),
        Some(2) => Value::NVarChar("guest".to_string()),
        _ => Value::Null,
    })
}

pub(crate) fn eval_app_name(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution("APP_NAME expects no arguments".into()));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .app_name
            .clone()
            .unwrap_or_else(|| "iridium_sql".to_string()),
    ))
}

pub(crate) fn eval_host_name(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution("HOST_NAME expects no arguments".into()));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .host_name
            .clone()
            .unwrap_or_else(|| "localhost".to_string()),
    ))
}

pub(crate) fn eval_host_id(args: &[Expr], _ctx: &ExecutionContext) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution("HOST_ID expects no arguments".into()));
    }
    // Return a fixed dummy host ID for now
    Ok(Value::Int(12345))
}

pub(crate) fn eval_system_user(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution(
            "SYSTEM_USER expects no arguments".into(),
        ));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .user
            .clone()
            .unwrap_or_else(|| "sa".to_string()),
    ))
}

pub(crate) fn eval_original_login(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution(
            "ORIGINAL_LOGIN expects no arguments".into(),
        ));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .user
            .clone()
            .unwrap_or_else(|| "sa".to_string()),
    ))
}

pub(crate) fn eval_session_user(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution(
            "SESSION_USER expects no arguments".into(),
        ));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .user
            .clone()
            .unwrap_or_else(|| "dbo".to_string()),
    ))
}

pub(crate) fn eval_current_user(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution(
            "CURRENT_USER expects no arguments".into(),
        ));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .user
            .clone()
            .unwrap_or_else(|| "dbo".to_string()),
    ))
}

pub(crate) fn eval_is_srvrolemember(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "IS_SRVROLEMEMBER expects 1 argument".into(),
        ));
    }

    let role = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if role.is_null() {
        return Ok(Value::Null);
    }

    let role_name = role.to_string_value().to_ascii_lowercase();
    let is_member = match role_name.as_str() {
        "sysadmin" => 1,
        _ => 0,
    };
    Ok(Value::Int(is_member))
}

pub(crate) fn eval_has_dbaccess(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("HAS_DBACCESS expects 1 argument".into()));
    }
    let db = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if db.is_null() {
        return Ok(Value::Null);
    }
    let db_name = db.to_string_value();
    Ok(Value::Int(
        if db_name.eq_ignore_ascii_case("master")
            || db_name.eq_ignore_ascii_case("tempdb")
            || db_name.eq_ignore_ascii_case("model")
            || db_name.eq_ignore_ascii_case("msdb")
            || db_name.eq_ignore_ascii_case("iridium_sql")
        {
            1
        } else {
            0
        },
    ))
}

pub(crate) fn eval_has_perms_by_name(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(DbError::Execution(
            "HAS_PERMS_BY_NAME expects 2 to 4 arguments".into(),
        ));
    }

    let securable = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let class = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    let perm = if args.len() >= 3 {
        eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_string_value()
            .to_ascii_uppercase()
    } else {
        String::new()
    };
    let securable_name = if securable.is_null() {
        String::new()
    } else {
        securable.to_string_value()
    };
    let class_name = if class.is_null() {
        String::new()
    } else {
        class.to_string_value().to_ascii_uppercase()
    };

    let is_server_context = class_name.is_empty()
        || class_name == "SERVER"
        || securable_name.eq_ignore_ascii_case("server");
    let is_database_context =
        class_name == "DATABASE" || securable_name.eq_ignore_ascii_case("master");

    let allowed = if is_server_context {
        matches!(
            perm.as_str(),
            "VIEW ANY DATABASE" | "CONNECT SQL" | "VIEW SERVER STATE" | "VIEW ANY DEFINITION"
        )
    } else if is_database_context {
        match perm.as_str() {
            "CONNECT" | "ANY" | "VIEW DATABASE STATE" | "VIEW DEFINITION" => true,
            _ => securable_name.eq_ignore_ascii_case("master"),
        }
    } else {
        false
    };
    Ok(Value::Int(if allowed { 1 } else { 0 }))
}

pub(crate) fn eval_scope_identity(ctx: &ExecutionContext) -> Value {
    ctx.session
        .current_scope_identity()
        .map(Value::BigInt)
        .unwrap_or(Value::Null)
}

pub(crate) fn eval_identity(ctx: &ExecutionContext) -> Value {
    ctx.session
        .last_identity
        .as_ref()
        .map(|&v| Value::BigInt(v))
        .unwrap_or(Value::Null)
}

pub(crate) fn eval_suser_sid(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "SUSER_SID expects 0 or 1 arguments".into(),
        ));
    }
    if !args.is_empty() {
        let v = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        if v.is_null() {
            return Ok(Value::Null);
        }
    }
    // Return a standard dummy SID: S-1-5-18 (LocalSystem) or similar
    Ok(Value::VarBinary(vec![
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x00, 0x00, 0x00,
    ]))
}

pub(crate) fn eval_user_sid(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() > 1 {
        return Err(DbError::Execution(
            "USER_SID expects 0 or 1 arguments".into(),
        ));
    }
    if !args.is_empty() {
        let v = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        if v.is_null() {
            return Ok(Value::Null);
        }
    }
    Ok(Value::VarBinary(vec![
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x12, 0x00, 0x00, 0x00,
    ]))
}

pub(crate) fn eval_is_member(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("IS_MEMBER expects 1 argument".into()));
    }

    let role = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if role.is_null() {
        return Ok(Value::Null);
    }

    let role_name = role.to_string_value().to_ascii_lowercase();
    let is_member = match role_name.as_str() {
        "db_owner" | "db_accessadmin" | "db_securityadmin" | "db_ddladmin"
        | "db_backupoperator" | "db_datareader" | "db_datawriter" | "db_denydatareader"
        | "db_denydatawriter" => 1,
        "public" => 1,
        _ => 0,
    };
    Ok(Value::Int(is_member))
}

pub(crate) fn eval_permissions(
    args: &[Expr],
    _row: &[ContextTable],
    _ctx: &mut ExecutionContext,
    _catalog: &dyn Catalog,
    _storage: &dyn Storage,
    _clock: &dyn Clock,
) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution(
            "PERMISSIONS expects no arguments".into(),
        ));
    }

    Ok(Value::BigInt(0x3FFFFFFF))
}
