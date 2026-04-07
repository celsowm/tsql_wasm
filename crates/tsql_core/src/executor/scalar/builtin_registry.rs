use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::metadata::system_vars;
use super::super::model::ContextTable;
use super::{datetime, math, metadata, string, system};

pub(crate) type ScalarHandler = for<'a> fn(
    &[Expr],
    &[ContextTable],
    &mut ExecutionContext<'a>,
    &dyn Catalog,
    &dyn Storage,
    &dyn Clock,
) -> Result<Value, DbError>;

#[derive(Clone, Copy)]
struct BuiltinScalarFunction {
    name: &'static str,
    handler: ScalarHandler,
}

macro_rules! builtin {
    ($name:literal => $handler:expr) => {
        BuiltinScalarFunction {
            name: $name,
            handler: $handler,
        }
    };
}

const SYSTEM_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("NEWID" => |args, _row, ctx, _catalog, _storage, _clock| {
        if !args.is_empty() {
            return Err(DbError::Execution("NEWID expects no arguments".into()));
        }
        let uuid = system::deterministic_uuid(&mut *ctx.session.random_state);
        Ok(Value::UniqueIdentifier(uuid))
    }),
    builtin!("RAND" => |_args, _row, ctx, _catalog, _storage, _clock| {
        let val = system::deterministic_rand(&mut *ctx.session.random_state);
        Ok(Value::Decimal((val * 1_000_000_000.0) as i128, 9))
    }),
    builtin!("OBJECT_ID" => system::eval_object_id),
    builtin!("COLUMNPROPERTY" => system::eval_columnproperty),
    builtin!("OBJECT_NAME" => metadata::eval_object_name),
    builtin!("OBJECT_SCHEMA_NAME" => metadata::eval_object_schema_name),
    builtin!("OBJECT_DEFINITION" => metadata::eval_object_definition),
    builtin!("OBJECTPROPERTY" => metadata::eval_objectproperty),
    builtin!("OBJECTPROPERTYEX" => metadata::eval_objectpropertyex),
    builtin!("SCHEMA_ID" => metadata::eval_schema_id),
    builtin!("SCHEMA_NAME" => metadata::eval_schema_name),
    builtin!("TYPE_ID" => metadata::eval_type_id),
    builtin!("TYPE_NAME" => metadata::eval_type_name),
    builtin!("TYPEPROPERTY" => metadata::eval_typeproperty),
    builtin!("COL_NAME" => metadata::eval_col_name),
    builtin!("COL_LENGTH" => metadata::eval_col_length),
    builtin!("INDEX_COL" => metadata::eval_index_col),
    builtin!("INDEXKEY_PROPERTY" => metadata::eval_indexkey_property),
    builtin!("INDEXPROPERTY" => metadata::eval_indexproperty),
    builtin!("DATABASEPROPERTYEX" => metadata::eval_databasepropertyex),
    builtin!("ORIGINAL_DB_NAME" => |_args, _row, ctx, _catalog, _storage, _clock| {
        metadata::eval_original_db_name(_args, ctx)
    }),
    builtin!("@@PROCID" => |_args, _row, ctx, _catalog, _storage, _clock| {
        if !_args.is_empty() {
            return Err(DbError::Execution("@@PROCID expects no arguments".into()));
        }
        Ok(metadata::eval_procid(ctx)?)
    }),
    builtin!("SCOPE_IDENTITY" => |_args, _row, ctx, _catalog, _storage, _clock| {
        if !_args.is_empty() {
            return Err(DbError::Execution("SCOPE_IDENTITY expects no arguments".into()));
        }
        Ok(match ctx.current_scope_identity() {
            Some(v) => Value::BigInt(v),
            None => Value::Null,
        })
    }),
    builtin!("@@IDENTITY" => |_args, _row, ctx, _catalog, _storage, _clock| {
        if !_args.is_empty() {
            return Err(DbError::Execution("@@IDENTITY expects no arguments".into()));
        }
        Ok(match *ctx.session.last_identity {
            Some(v) => Value::BigInt(v),
            None => Value::Null,
        })
    }),
    builtin!("IDENT_CURRENT" => system::eval_ident_current),
    builtin!("@@VERSION" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::NVarChar(
            "Microsoft SQL Server 2022 (RTM) - 16.0.1000.6 (tsql_wasm emulator)".into(),
        ))
    }),
    builtin!("@@SERVERNAME" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::NVarChar("localhost".into()))
    }),
    builtin!("@@SERVICENAME" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::NVarChar("MSSQLSERVER".into()))
    }),
    builtin!("@@SPID" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::SmallInt(1))
    }),
    builtin!("@@TRANCOUNT" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(ctx.trancount() as i32))
    }),
    builtin!("XACT_STATE" => |_args, _row, ctx, _catalog, _storage, _clock| {
        if !_args.is_empty() {
            return Err(DbError::Execution("XACT_STATE expects no arguments".into()));
        }
        Ok(Value::Int(ctx.xact_state() as i32))
    }),
    builtin!("@@ERROR" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(0))
    }),
    builtin!("@@FETCH_STATUS" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(*ctx.session.fetch_status))
    }),
    builtin!("@@LANGUAGE" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::NVarChar(ctx.options.language.clone()))
    }),
    builtin!("@@TEXTSIZE" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(ctx.options.textsize))
    }),
    builtin!("@@MAX_PRECISION" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::TinyInt(38))
    }),
    builtin!("@@DATEFIRST" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::TinyInt(ctx.options.datefirst as u8))
    }),
    builtin!("@@MICROSOFTVERSION" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(system::eval_microsoft_version())
    }),
    builtin!("ERROR_MESSAGE" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_error_message(ctx)
    }),
    builtin!("ERROR_NUMBER" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_error_number(ctx)
    }),
    builtin!("ERROR_SEVERITY" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_error_severity(ctx)
    }),
    builtin!("ERROR_STATE" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_error_state(ctx)
    }),
    builtin!("DB_NAME" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_db_name(_args, ctx)
    }),
    builtin!("DB_ID" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_db_id(_args, ctx)
    }),
    builtin!("SUSER_SNAME" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_suser_sname(_args, ctx)
    }),
    builtin!("SUSER_ID" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_suser_id(_args, ctx)
    }),
    builtin!("USER_NAME" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_user_name(_args, ctx)
    }),
    builtin!("USER_ID" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_user_id(_args, ctx)
    }),
    builtin!("APP_NAME" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_app_name(_args, ctx)
    }),
    builtin!("HOST_NAME" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_host_name(_args, ctx)
    }),
    builtin!("SYSTEM_USER" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_system_user(_args, ctx)
    }),
    builtin!("ORIGINAL_LOGIN" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_original_login(_args, ctx)
    }),
    builtin!("HASHBYTES" => system::eval_hashbytes),
    builtin!("PARSENAME" => system::eval_parsename),
    builtin!("QUOTENAME" => system::eval_quotename),
    builtin!("SESSION_USER" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_session_user(_args, ctx)
    }),
    builtin!("CURRENT_USER" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::eval_current_user(_args, ctx)
    }),
    builtin!("SERVERPROPERTY" => system::eval_serverproperty),
    builtin!("CONNECTIONPROPERTY" => system::eval_connectionproperty),
    builtin!("FULLTEXTSERVICEPROPERTY" => system::eval_fulltextserviceproperty),
    builtin!("IS_SRVROLEMEMBER" => system::eval_is_srvrolemember),
    builtin!("HAS_DBACCESS" => system::eval_has_dbaccess),
    builtin!("HAS_PERMS_BY_NAME" => system::eval_has_perms_by_name),
];

const DATETIME_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("GETDATE" => |_args, _row, _ctx, _catalog, _storage, clock| {
        Ok(Value::DateTime(clock.now_datetime_literal()))
    }),
    builtin!("CURRENT_TIMESTAMP" => |_args, _row, _ctx, _catalog, _storage, clock| {
        Ok(Value::DateTime(clock.now_datetime_literal()))
    }),
    builtin!("CURRENT_DATE" => |_args, _row, _ctx, _catalog, _storage, clock| {
        let dt = clock.now_datetime_literal();
        Ok(Value::Date(dt.date()))
    }),
    builtin!("DATEADD" => datetime::eval_dateadd),
    builtin!("DATEDIFF" => datetime::eval_datediff),
    builtin!("DATEPART" => datetime::eval_datepart),
    builtin!("DATENAME" => datetime::eval_datename),
    builtin!("YEAR" => datetime::eval_year),
    builtin!("MONTH" => datetime::eval_month),
    builtin!("DAY" => datetime::eval_day),
];

const STRING_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("LEN" => string::eval_len),
    builtin!("SUBSTRING" => string::eval_substring),
    builtin!("UPPER" => string::eval_upper),
    builtin!("LOWER" => string::eval_lower),
    builtin!("LTRIM" => |_args, _row, ctx, catalog, storage, clock| {
        string::eval_trim(_args, _row, ctx, catalog, storage, clock, true, false)
    }),
    builtin!("RTRIM" => |_args, _row, ctx, catalog, storage, clock| {
        string::eval_trim(_args, _row, ctx, catalog, storage, clock, false, true)
    }),
    builtin!("TRIM" => |_args, _row, ctx, catalog, storage, clock| {
        string::eval_trim(_args, _row, ctx, catalog, storage, clock, true, true)
    }),
    builtin!("REPLACE" => string::eval_replace),
    builtin!("LEFT" => string::eval_left),
    builtin!("RIGHT" => string::eval_right),
    builtin!("CHARINDEX" => string::eval_charindex),
    builtin!("UNISTR" => string::eval_unistr),
    builtin!("ASCII" => string::eval_ascii),
    builtin!("CHAR" => string::eval_char),
    builtin!("NCHAR" => string::eval_nchar),
    builtin!("UNICODE" => string::eval_unicode),
    builtin!("STRING_ESCAPE" => string::eval_string_escape),
    builtin!("CONCAT" => string::eval_concat),
    builtin!("CONCAT_WS" => string::eval_concat_ws),
    builtin!("REPLICATE" => string::eval_replicate),
    builtin!("REVERSE" => string::eval_reverse),
    builtin!("STUFF" => string::eval_stuff),
    builtin!("SPACE" => string::eval_space),
    builtin!("STR" => string::eval_str),
    builtin!("TRANSLATE" => string::eval_translate),
    builtin!("FORMAT" => string::eval_format),
    builtin!("PATINDEX" => string::eval_patindex),
    builtin!("SOUNDEX" => string::eval_soundex),
    builtin!("DIFFERENCE" => string::eval_difference),
];

const MATH_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("ROUND" => math::eval_round),
    builtin!("CEILING" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "CEILING", |f| f.ceil())
    }),
    builtin!("FLOOR" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "FLOOR", |f| f.floor())
    }),
    builtin!("ABS" => math::eval_abs),
    builtin!("POWER" => math::eval_power),
    builtin!("SQRT" => math::eval_sqrt),
    builtin!("SIGN" => math::eval_sign),
    builtin!("ACOS" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "ACOS", f64::acos)
    }),
    builtin!("ASIN" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "ASIN", f64::asin)
    }),
    builtin!("ATAN" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "ATAN", f64::atan)
    }),
    builtin!("ATN2" => math::eval_atn2),
    builtin!("COS" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "COS", f64::cos)
    }),
    builtin!("COT" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "COT", |f| 1.0 / f.tan())
    }),
    builtin!("DEGREES" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "DEGREES", f64::to_degrees)
    }),
    builtin!("EXP" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "EXP", f64::exp)
    }),
    builtin!("LOG" => math::eval_log),
    builtin!("LOG10" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "LOG10", f64::log10)
    }),
    builtin!("PI" => |args, _row, _ctx, _catalog, _storage, _clock| {
        math::eval_pi(args)
    }),
    builtin!("RADIANS" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "RADIANS", f64::to_radians)
    }),
    builtin!("SIN" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "SIN", f64::sin)
    }),
    builtin!("SQUARE" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "SQUARE", |f| f * f)
    }),
    builtin!("TAN" => |_args, _row, ctx, catalog, storage, clock| {
        math::eval_math_unary(_args, _row, ctx, catalog, storage, clock, "TAN", f64::tan)
    }),
    builtin!("CHECKSUM" => math::eval_checksum),
];

fn lookup_builtin_function(
    functions: &[BuiltinScalarFunction],
    name: &str,
) -> Option<ScalarHandler> {
    functions
        .iter()
        .find(|entry| entry.name.eq_ignore_ascii_case(name))
        .map(|entry| entry.handler)
}

pub(crate) fn lookup_system_function(name: &str) -> Option<ScalarHandler> {
    lookup_builtin_function(SYSTEM_FUNCTIONS, name)
}

pub(crate) fn lookup_datetime_function(name: &str) -> Option<ScalarHandler> {
    lookup_builtin_function(DATETIME_FUNCTIONS, name)
}

pub(crate) fn lookup_string_function(name: &str) -> Option<ScalarHandler> {
    lookup_builtin_function(STRING_FUNCTIONS, name)
}

pub(crate) fn lookup_math_function(name: &str) -> Option<ScalarHandler> {
    lookup_builtin_function(MATH_FUNCTIONS, name)
}

pub(crate) fn lookup_system_variable(
    name: &str,
    ctx: &ExecutionContext<'_>,
) -> Option<Result<Value, DbError>> {
    if !name.starts_with("@@") {
        return None;
    }
    match system_vars::resolve_system_variable(name, ctx) {
        Ok(Some(val)) => Some(Ok(val)),
        Ok(None) => None,
        Err(err) => Some(Err(err)),
    }
}
