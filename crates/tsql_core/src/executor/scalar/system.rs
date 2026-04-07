use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

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
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution("OBJECT_ID expects 1 or 2 arguments".into()));
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

    let object_type = if let Some(arg) = args.get(1) {
        let ty = eval_expr(arg, row, ctx, catalog, storage, clock)?;
        if ty.is_null() {
            None
        } else {
            Some(ty.to_string_value().to_ascii_uppercase())
        }
    } else {
        None
    };

    if schema.eq_ignore_ascii_case("sys")
        && name.eq_ignore_ascii_case("sp_MSIsContainedAGSession")
        && object_type.as_deref().unwrap_or("P") == "P"
    {
        return Ok(Value::Int(2147483001));
    }

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
        "MACHINENAME" => Value::NVarChar("localhost".to_string()),
        "SERVERNAME" => Value::NVarChar("localhost".to_string()),
        "INSTANCENAME" => Value::Null,
        "COLLATION" => Value::NVarChar("SQL_Latin1_General_CP1_CI_AS".to_string()),
        "ISINTEGRATEDSECURITYONLY" => Value::Int(0),
        "ISSINGLEUSER" => Value::Int(0),
        "ISXTPSUPPORTED" => Value::Int(0),
        "ISPOLYBASEINSTALLED" => Value::Int(0),
        "ISHADRENABLED" => Value::Int(0),
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
        // Emulated server: treat login as sysadmin to satisfy SSMS server-node probes.
        "sysadmin" => 1,
        "serveradmin" | "setupadmin" | "securityadmin" | "processadmin" | "dbcreator"
        | "diskadmin" | "bulkadmin" => 0,
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
    Ok(Value::Int(if db_name.eq_ignore_ascii_case("master")
        || db_name.eq_ignore_ascii_case("tempdb")
        || db_name.eq_ignore_ascii_case("model")
        || db_name.eq_ignore_ascii_case("msdb")
    {
        1
    } else {
        0
    }))
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

    // Minimal permission model for SSMS object explorer probes.
    let is_server_context = class_name.is_empty()
        || class_name == "SERVER"
        || securable_name.eq_ignore_ascii_case("server");
    let is_database_context = class_name == "DATABASE" || securable_name.eq_ignore_ascii_case("master");

    let allowed = if is_server_context {
        match perm.as_str() {
            "VIEW ANY DATABASE" | "CONNECT SQL" | "VIEW SERVER STATE" | "VIEW ANY DEFINITION" => {
                true
            }
            _ => false,
        }
    } else if is_database_context {
        match perm.as_str() {
            "CONNECT" | "ANY" => true,
            _ => securable_name.eq_ignore_ascii_case("master"),
        }
    } else {
        false
    };
    Ok(Value::Int(if allowed { 1 } else { 0 }))
}

pub(crate) fn eval_microsoft_version() -> Value {
    Value::Int(0x1000_0000)
}

pub(crate) fn deterministic_uuid(state: &mut u64) -> uuid::Uuid {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let bytes = state.to_be_bytes();
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes[0] = bytes[0];
    uuid_bytes[1] = bytes[1];
    uuid_bytes[2] = bytes[2];
    uuid_bytes[3] = bytes[3];
    uuid_bytes[4] = bytes[4];
    uuid_bytes[5] = bytes[5];
    uuid_bytes[6] = bytes[6];
    uuid_bytes[7] = bytes[7];
    uuid_bytes[8] = bytes[0] ^ bytes[4];
    uuid_bytes[9] = bytes[1] ^ bytes[5];
    uuid_bytes[10] = bytes[2] ^ bytes[6];
    uuid_bytes[11] = bytes[3] ^ bytes[7];
    uuid_bytes[12] = bytes[4] ^ bytes[0];
    uuid_bytes[13] = bytes[5] ^ bytes[1];
    uuid_bytes[14] = bytes[6] ^ bytes[2];
    uuid_bytes[15] = bytes[7] ^ bytes[3];
    uuid::Uuid::from_bytes(uuid_bytes)
}

pub(crate) fn deterministic_rand(state: &mut u64) -> f64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let bits = (*state >> 33) as u32;
    bits as f64 / (1u64 << 31) as f64
}

pub(crate) fn eval_error_message(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match &ctx.frame.last_error {
        Some(e) => Value::VarChar(e.to_string()),
        None => Value::Null,
    })
}

pub(crate) fn eval_error_number(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match &ctx.frame.last_error {
        Some(_) => Value::Int(50000), // Default error number
        None => Value::Null,
    })
}

pub(crate) fn eval_error_severity(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match &ctx.frame.last_error {
        Some(_) => Value::Int(16), // Default severity
        None => Value::Null,
    })
}

pub(crate) fn eval_error_state(ctx: &ExecutionContext) -> Result<Value, DbError> {
    Ok(match &ctx.frame.last_error {
        Some(_) => Value::Int(1), // Default state
        None => Value::Null,
    })
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
            Expr::String(s) | Expr::UnicodeString(s) => {
                if s.eq_ignore_ascii_case("master")
                    || s.eq_ignore_ascii_case("tempdb")
                    || s.eq_ignore_ascii_case("model")
                    || s.eq_ignore_ascii_case("msdb")
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
            _ => Value::Null,
        });
    }
    Ok(Value::Int(1))
}

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

pub(crate) fn eval_app_name(args: &[Expr], ctx: &ExecutionContext) -> Result<Value, DbError> {
    if !args.is_empty() {
        return Err(DbError::Execution("APP_NAME expects no arguments".into()));
    }
    Ok(Value::NVarChar(
        ctx.metadata
            .app_name
            .clone()
            .unwrap_or_else(|| "tsql_wasm".to_string()),
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

pub(crate) fn eval_hashbytes(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("HASHBYTES expects 2 arguments".into()));
    }
    let algo_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let data_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if algo_val.is_null() || data_val.is_null() {
        return Ok(Value::Null);
    }

    let algo = algo_val.to_string_value().to_uppercase();
    let data = data_val.to_string_value();

    // Simple hash simulation using built-in Rust hashing
    let hash_bytes = match algo.as_str() {
        "MD5" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            data.hash(&mut hasher);
            let h = hasher.finish();
            let mut bytes = h.to_be_bytes().to_vec();
            bytes.resize(16, 0);
            bytes
        }
        "SHA1" | "SHA_1" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher1 = DefaultHasher::new();
            let mut hasher2 = DefaultHasher::new();
            data.hash(&mut hasher1);
            data.len().hash(&mut hasher2);
            let mut bytes = Vec::with_capacity(20);
            bytes.extend_from_slice(&hasher1.finish().to_be_bytes());
            bytes.extend_from_slice(&hasher2.finish().to_be_bytes());
            bytes.extend_from_slice(&[0u8; 4]);
            bytes
        }
        "SHA2_256" | "SHA256" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut bytes = Vec::with_capacity(32);
            for i in 0..4 {
                let mut hasher = DefaultHasher::new();
                data.hash(&mut hasher);
                i.hash(&mut hasher);
                bytes.extend_from_slice(&hasher.finish().to_be_bytes());
            }
            bytes
        }
        "SHA2_512" | "SHA512" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut bytes = Vec::with_capacity(64);
            for i in 0..8 {
                let mut hasher = DefaultHasher::new();
                data.hash(&mut hasher);
                i.hash(&mut hasher);
                bytes.extend_from_slice(&hasher.finish().to_be_bytes());
            }
            bytes
        }
        _ => {
            return Err(DbError::Execution(format!(
                "Unsupported hash algorithm '{}'. Supported: MD5, SHA1, SHA2_256, SHA2_512",
                algo
            )))
        }
    };

    Ok(Value::VarBinary(hash_bytes))
}

pub(crate) fn eval_parsename(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("PARSENAME expects 2 arguments".into()));
    }
    let obj_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let piece_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;

    if obj_val.is_null() || piece_val.is_null() {
        return Ok(Value::Null);
    }

    let obj = obj_val.to_string_value();
    let piece = piece_val.to_integer_i64().unwrap_or(0);

    let parts: Vec<&str> = obj.split('.').rev().collect();
    let result = match piece {
        1 => parts.first().copied(), // Object name
        2 => {
            if parts.len() >= 2 {
                Some(parts[1])
            } else {
                None
            }
        } // Schema name
        3 => {
            if parts.len() >= 3 {
                Some(parts[2])
            } else {
                None
            }
        } // Database name
        4 => {
            if parts.len() >= 4 {
                Some(parts[3])
            } else {
                None
            }
        } // Server name
        _ => None,
    };

    match result {
        Some(s) => Ok(Value::NVarChar(s.to_string())),
        None => Ok(Value::Null),
    }
}

pub(crate) fn eval_quotename(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution(
            "QUOTENAME expects 1 or 2 arguments".into(),
        ));
    }
    let name_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if name_val.is_null() {
        return Ok(Value::Null);
    }
    let quote_char = if args.len() == 2 {
        eval_expr(&args[1], row, ctx, catalog, storage, clock)?.to_string_value()
    } else {
        "]".to_string()
    };

    let name = name_val.to_string_value();
    let open = if quote_char == "]" {
        "["
    } else {
        &quote_char[..quote_char.len().min(1)]
    };
    let close = &quote_char;

    Ok(Value::NVarChar(format!("{}{}{}", open, name, close)))
}
