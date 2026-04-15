use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use std::collections::HashMap;
use std::sync::OnceLock;

use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;
use crate::executor::scalar::{datetime, logic, math, metadata as tsql_metadata, string, system};
use crate::executor::{fuzzy, json, metadata as exec_metadata, regexp};

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
        Ok(system::random::eval_newid(ctx))
    }),
    builtin!("RAND" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(system::random::eval_rand(ctx))
    }),
    builtin!("OBJECT_ID" => tsql_metadata::eval_object_id),
    builtin!("OBJECT_NAME" => tsql_metadata::eval_object_name),
    builtin!("OBJECT_DEFINITION" => tsql_metadata::eval_object_definition),
    builtin!("OBJECT_SCHEMA_NAME" => tsql_metadata::eval_object_schema_name),
    builtin!("SCHEMA_NAME" => tsql_metadata::eval_schema_name),
    builtin!("SCHEMA_ID" => tsql_metadata::eval_schema_id),
    builtin!("TYPE_NAME" => tsql_metadata::eval_type_name),
    builtin!("TYPE_ID" => tsql_metadata::eval_type_id),
    builtin!("TYPEPROPERTY" => tsql_metadata::eval_typeproperty),
    builtin!("COLUMNPROPERTY" => tsql_metadata::eval_columnproperty),
    builtin!("OBJECTPROPERTY" => tsql_metadata::eval_objectproperty),
    builtin!("OBJECTPROPERTYEX" => tsql_metadata::eval_objectpropertyex),
    builtin!("INDEXPROPERTY" => tsql_metadata::eval_indexproperty),
    builtin!("INDEX_COL" => tsql_metadata::eval_index_col),
    builtin!("INDEXKEY_PROPERTY" => tsql_metadata::eval_indexkey_property),
    builtin!("COL_NAME" => tsql_metadata::eval_col_name),
    builtin!("COL_LENGTH" => tsql_metadata::eval_col_length),
    builtin!("DATABASEPROPERTYEX" => tsql_metadata::eval_databasepropertyex),
    builtin!("IDENT_CURRENT" => tsql_metadata::eval_ident_current),
    builtin!("IDENT_SEED" => tsql_metadata::eval_ident_seed),
    builtin!("IDENT_INCR" => tsql_metadata::eval_ident_incr),
    builtin!("SCOPE_IDENTITY" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(system::identity::eval_scope_identity(ctx))
    }),
    builtin!("XACT_STATE" => |args, _row, ctx, _catalog, _storage, _clock| {
        if !args.is_empty() {
            return Err(DbError::Execution("XACT_STATE expects no arguments".into()));
        }
        Ok(Value::Int(ctx.frame.xact_state as i32))
    }),
    builtin!("@@IDENTITY" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(system::identity::eval_identity(ctx))
    }),
    builtin!("@@ROWCOUNT" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(ctx.session.variables.get("@@ROWCOUNT").and_then(|v| v.1.to_integer_i64()).unwrap_or(0) as i32))
    }),
    builtin!("@@TRANCOUNT" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(ctx.frame.trancount as i32))
    }),
    builtin!("@@ERROR" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(if ctx.frame.last_error.is_some() { 1 } else { 0 }))
    }),
    builtin!("@@SPID" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(ctx.metadata.id as i32))
    }),
    builtin!("@@SERVERNAME" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::NVarChar("iridium-wasm-server".into()))
    }),
    builtin!("@@VERSION" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::NVarChar("PostgreSQL 15.0 on x86_64-pc-linux-gnu (iridium-wasm emulation)".into()))
    }),
    builtin!("@@MAX_CONNECTIONS" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(100))
    }),
    builtin!("@@REMSERVER" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::Null)
    }),
    builtin!("@@FETCH_STATUS" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(*ctx.session.fetch_status))
    }),
    builtin!("@@NESTLEVEL" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(ctx.frame.depth as i32))
    }),
    builtin!("@@OPTIONS" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::Int(0))
    }),
    builtin!("@@MAX_PRECISION" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(Value::TinyInt(38))
    }),
    builtin!("@@DATEFIRST" => |_args, _row, ctx, _catalog, _storage, _clock| {
        Ok(Value::TinyInt(ctx.metadata.datefirst as u8))
    }),
    builtin!("@@MICROSOFTVERSION" => |_args, _row, _ctx, _catalog, _storage, _clock| {
        Ok(system::properties::eval_microsoft_version())
    }),
    builtin!("ERROR_MESSAGE" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::diagnostics::eval_error_message(ctx)
    }),
    builtin!("ERROR_NUMBER" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::diagnostics::eval_error_number(ctx)
    }),
    builtin!("ERROR_SEVERITY" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::diagnostics::eval_error_severity(ctx)
    }),
    builtin!("ERROR_STATE" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::diagnostics::eval_error_state(ctx)
    }),
    builtin!("DB_NAME" => |args, _row, ctx, _catalog, _storage, _clock| {
        tsql_metadata::eval_db_name(args, ctx)
    }),
    builtin!("DB_ID" => |args, _row, ctx, _catalog, _storage, _clock| {
        tsql_metadata::eval_db_id(args, ctx)
    }),
    builtin!("SUSER_SNAME" => |args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_suser_sname(args, ctx)
    }),
    builtin!("SUSER_ID" => |args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_suser_id(args, ctx)
    }),
    builtin!("USER_NAME" => |args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_user_name(args, ctx)
    }),
    builtin!("USER_ID" => |args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_user_id(args, ctx)
    }),
    builtin!("DATABASE_PRINCIPAL_ID" => system::identity::eval_database_principal_id),
    builtin!("DATABASE_PRINCIPAL_NAME" => system::identity::eval_database_principal_name),
    builtin!("APP_NAME" => |args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_app_name(args, ctx)
    }),
    builtin!("HOST_NAME" => |args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_host_name(args, ctx)
    }),
    builtin!("SYSTEM_USER" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_system_user(_args, ctx)
    }),
    builtin!("SESSION_USER" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_session_user(_args, ctx)
    }),
    builtin!("CURRENT_USER" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_current_user(_args, ctx)
    }),
    builtin!("ORIGINAL_LOGIN" => |_args, _row, ctx, _catalog, _storage, _clock| {
        system::identity::eval_original_login(_args, ctx)
    }),
    builtin!("ORIGINAL_DB_NAME" => |args, _row, ctx, _catalog, _storage, _clock| {
        tsql_metadata::eval_original_db_name(args, ctx)
    }),
    builtin!("PROCID" => |_args, _row, ctx, _catalog, _storage, _clock| {
        tsql_metadata::eval_procid(ctx)
    }),
    builtin!("SERVERPROPERTY" => system::properties::eval_serverproperty),
    builtin!("FULLTEXTSERVICEPROPERTY" => system::properties::eval_fulltextserviceproperty),
    builtin!("CONNECTIONPROPERTY" => system::properties::eval_connectionproperty),
    builtin!("IS_SRVROLEMEMBER" => system::identity::eval_is_srvrolemember),
    builtin!("HAS_DBACCESS" => system::identity::eval_has_dbaccess),
    builtin!("HAS_PERMS_BY_NAME" => system::identity::eval_has_perms_by_name),
    builtin!("PARSENAME" => system::formatting::eval_parsename),
    builtin!("QUOTENAME" => system::formatting::eval_quotename),
    builtin!("HASHBYTES" => system::crypto::eval_hashbytes),
];

const DATETIME_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("GETDATE" => datetime::eval_getdate),
    builtin!("CURRENT_TIMESTAMP" => datetime::eval_current_timestamp),
    builtin!("CURRENT_DATE" => datetime::eval_current_date),
    builtin!("GETUTCDATE" => datetime::eval_getutcdate),
    builtin!("SYSDATETIME" => datetime::eval_sysdatetime),
    builtin!("SYSUTCDATETIME" => datetime::eval_sysutcdatetime),
    builtin!("SYSDATETIMEOFFSET" => datetime::eval_sysdatetimeoffset),
    builtin!("DATEPART" => datetime::eval_datepart),
    builtin!("DATENAME" => datetime::eval_datename),
    builtin!("DATEDIFF" => datetime::eval_datediff),
    builtin!("DATEDIFF_BIG" => datetime::eval_datediff_big),
    builtin!("DATEADD" => datetime::eval_dateadd),
    builtin!("EOMONTH" => datetime::eval_eomonth),
    builtin!("ISDATE" => datetime::eval_isdate),
    builtin!("YEAR" => datetime::eval_year),
    builtin!("MONTH" => datetime::eval_month),
    builtin!("DAY" => datetime::eval_day),
];

const STRING_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("LEN" => string::eval_len),
    builtin!("DATALENGTH" => string::eval_datalength),
    builtin!("SUBSTRING" => string::eval_substring),
    builtin!("UPPER" => string::eval_upper),
    builtin!("LOWER" => string::eval_lower),
    builtin!("LTRIM" => |args, row, ctx, catalog, storage, clock| {
        string::eval_trim(args, row, ctx, catalog, storage, clock, true, false)
    }),
    builtin!("RTRIM" => |args, row, ctx, catalog, storage, clock| {
        string::eval_trim(args, row, ctx, catalog, storage, clock, false, true)
    }),
    builtin!("TRIM" => |args, row, ctx, catalog, storage, clock| {
        string::eval_trim(args, row, ctx, catalog, storage, clock, true, true)
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
    builtin!("STRING_ESCAPE" => string::eval_string_escape),
];

const MATH_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("ABS" => math::eval_abs),
    builtin!("CEILING" => math::eval_ceiling),
    builtin!("FLOOR" => math::eval_floor),
    builtin!("ROUND" => math::eval_round),
    builtin!("SQRT" => math::eval_sqrt),
    builtin!("SQUARE" => math::eval_square),
    builtin!("POWER" => math::eval_power),
    builtin!("EXP" => math::eval_exp),
    builtin!("LOG" => math::eval_log),
    builtin!("LOG10" => math::eval_log10),
    builtin!("SIN" => math::eval_sin),
    builtin!("COS" => math::eval_cos),
    builtin!("TAN" => math::eval_tan),
    builtin!("ASIN" => math::eval_asin),
    builtin!("ACOS" => math::eval_acos),
    builtin!("ATAN" => math::eval_atan),
    builtin!("ATN2" => math::eval_atn2),
    builtin!("COT" => math::eval_cot),
    builtin!("DEGREES" => math::eval_degrees),
    builtin!("RADIANS" => math::eval_radians),
    builtin!("PI" => math::eval_pi),
    builtin!("SIGN" => math::eval_sign),
    builtin!("RAND" => math::eval_rand),
    builtin!("CHECKSUM" => math::eval_checksum),
];

const LOGIC_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("ISNULL" => logic::eval_isnull),
    builtin!("COALESCE" => logic::eval_coalesce),
    builtin!("IIF" => logic::eval_iif),
    builtin!("NULLIF" => logic::eval_nullif),
    builtin!("CHOOSE" => logic::eval_choose),
];

const JSON_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("JSON_VALUE" => |args, row, ctx, catalog, storage, clock| {
        if args.len() != 2 {
            return Err(DbError::Execution("JSON_VALUE expects 2 arguments".into()));
        }
        let json_val = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        let path_val = crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
        if json_val.is_null() {
            return Ok(Value::Null);
        }
        json::json_value(&json_val.to_string_value(), &path_val.to_string_value())
    }),
    builtin!("JSON_QUERY" => |args, row, ctx, catalog, storage, clock| {
        if args.len() != 2 {
            return Err(DbError::Execution("JSON_QUERY expects 2 arguments".into()));
        }
        let json_val = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        let path_val = crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
        if json_val.is_null() {
            return Ok(Value::Null);
        }
        json::json_query(&json_val.to_string_value(), &path_val.to_string_value())
    }),
    builtin!("JSON_MODIFY" => |args, row, ctx, catalog, storage, clock| {
        if args.len() != 3 {
            return Err(DbError::Execution("JSON_MODIFY expects 3 arguments".into()));
        }
        let json_val = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        let path_val = crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
        let new_val = crate::executor::evaluator::eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
        if json_val.is_null() {
            return Ok(Value::Null);
        }
        json::json_modify(
            &json_val.to_string_value(),
            &path_val.to_string_value(),
            &new_val.to_string_value(),
        )
    }),
    builtin!("ISJSON" => |args, row, ctx, catalog, storage, clock| {
        if args.len() != 1 {
            return Err(DbError::Execution("ISJSON expects 1 argument".into()));
        }
        let json_val = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        if json_val.is_null() {
            return Ok(Value::Null);
        }
        json::is_json(&json_val.to_string_value())
    }),
    builtin!("JSON_ARRAY_LENGTH" => |args, row, ctx, catalog, storage, clock| {
        if args.len() != 1 {
            return Err(DbError::Execution("JSON_ARRAY_LENGTH expects 1 argument".into()));
        }
        let json_val = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        if json_val.is_null() {
            return Ok(Value::Null);
        }
        json::json_array_length(&json_val.to_string_value())
    }),
    builtin!("JSON_KEYS" => |args, row, ctx, catalog, storage, clock| {
        if args.is_empty() || args.len() > 2 {
            return Err(DbError::Execution("JSON_KEYS expects 1 or 2 arguments".into()));
        }
        let json_val = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
        if json_val.is_null() {
            return Ok(Value::Null);
        }
        let path = if args.len() == 2 {
            let path_val = crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
            Some(path_val.to_string_value())
        } else {
            None
        };
        json::json_keys(&json_val.to_string_value(), path.as_deref())
    }),
];

const REGEXP_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("REGEXP_LIKE" => regexp::eval_regexp_like),
    builtin!("REGEXP_REPLACE" => regexp::eval_regexp_replace),
    builtin!("REGEXP_SUBSTR" => regexp::eval_regexp_substr),
    builtin!("REGEXP_INSTR" => regexp::eval_regexp_instr),
    builtin!("REGEXP_COUNT" => regexp::eval_regexp_count),
];

const FUZZY_FUNCTIONS: &[BuiltinScalarFunction] = &[
    builtin!("EDIT_DISTANCE" => fuzzy::eval_edit_distance),
    builtin!("EDIT_DISTANCE_SIMILARITY" => fuzzy::eval_edit_distance_similarity),
    builtin!("JARO_WINKLER_DISTANCE" => fuzzy::eval_jaro_winkler_distance),
    builtin!("JARO_WINKLER_SIMILARITY" => fuzzy::eval_jaro_winkler_similarity),
];

fn builtin_handler_map() -> &'static HashMap<&'static str, ScalarHandler> {
    static HANDLERS: OnceLock<HashMap<&'static str, ScalarHandler>> = OnceLock::new();
    HANDLERS.get_or_init(|| {
        let mut handlers = HashMap::new();
        for group in [
            SYSTEM_FUNCTIONS,
            DATETIME_FUNCTIONS,
            STRING_FUNCTIONS,
            MATH_FUNCTIONS,
            LOGIC_FUNCTIONS,
            JSON_FUNCTIONS,
            REGEXP_FUNCTIONS,
            FUZZY_FUNCTIONS,
        ] {
            for entry in group {
                handlers.entry(entry.name).or_insert(entry.handler);
            }
        }
        handlers
    })
}

pub(crate) fn lookup_builtin_handler(name: &str) -> Option<ScalarHandler> {
    builtin_handler_map().get(name).copied()
}

pub(crate) fn lookup_system_variable(
    name: &str,
    ctx: &ExecutionContext<'_>,
) -> Option<Result<Value, DbError>> {
    if !name.starts_with("@@") {
        return None;
    }
    match exec_metadata::system_vars::resolve_system_variable(name, ctx) {
        Ok(Some(val)) => Some(Ok(val)),
        Ok(None) => None,
        Err(err) => Some(Err(err)),
    }
}

