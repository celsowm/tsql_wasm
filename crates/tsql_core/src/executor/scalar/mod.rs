pub(crate) mod datetime;
pub(crate) mod metadata;
pub(crate) mod logic;
pub(crate) mod math;
pub(crate) mod string;
pub(crate) mod system;

use crate::ast::Expr;
use crate::ast::RoutineParamType;
use crate::catalog::{Catalog, RoutineKind};
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::aggregates::{dispatch_aggregate, is_aggregate_function};
use super::clock::Clock;
use super::context::{ExecutionContext, ModuleFrame, ModuleKind};
use super::evaluator::eval_expr;
use super::metadata::system_vars;
use super::fuzzy;
use super::json;
use super::model::ContextTable;
use super::regexp;

pub(crate) fn eval_function(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if is_aggregate_function(name) {
        if let Some(group) = ctx.current_group().clone() {
            if let Some(res) = dispatch_aggregate(name, args, &group, ctx, catalog, storage, clock)
            {
                return res;
            }
        }
    }

    let upper = name.to_uppercase();
    let upper_str = upper.as_str();

    // Try category-based dispatch first
    if let Some(result) = try_datetime_dispatch(upper_str, args, row, ctx, catalog, storage, clock) {
        return result;
    }
    if let Some(result) = try_string_dispatch(upper_str, args, row, ctx, catalog, storage, clock) {
        return result;
    }
    if let Some(result) = try_math_dispatch(upper_str, args, row, ctx, catalog, storage, clock) {
        return result;
    }
    if let Some(result) = try_system_dispatch(upper_str, args, row, ctx, catalog, storage, clock) {
        return result;
    }
    if let Some(result) = try_json_dispatch(upper_str, args, row, ctx, catalog, storage, clock) {
        return result;
    }
    if let Some(result) = try_regexp_dispatch(upper_str, args, row, ctx, catalog, storage, clock) {
        return result;
    }
    if let Some(result) = try_fuzzy_dispatch(upper_str, args, row, ctx, catalog, storage, clock) {
        return result;
    }

    // Remaining functions not yet categorized
    match upper_str {
        "ISNULL" => logic::eval_isnull(args, row, ctx, catalog, storage, clock),
        "COALESCE" => logic::eval_coalesce(args, row, ctx, catalog, storage, clock),
        "IIF" => logic::eval_iif(args, row, ctx, catalog, storage, clock),
        "NULLIF" => logic::eval_nullif(args, row, ctx, catalog, storage, clock),
        "CHOOSE" => logic::eval_choose(args, row, ctx, catalog, storage, clock),
        "COUNT" | "SUM" | "AVG" => Err(DbError::Execution(format!(
            "{} is only supported in grouped projection",
            name
        ))),
        "MIN" | "MAX" => Err(DbError::Execution(
            "MIN/MAX require a FROM clause when used in scalar context (use in GROUP BY)".into(),
        )),
        "COUNT_BIG" => Err(DbError::Execution(
            "COUNT_BIG is only supported in grouped projection".into(),
        )),
        _ => eval_user_scalar_function(name, args, row, ctx, catalog, storage, clock),
    }
}

fn try_datetime_dispatch(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Option<Result<Value, DbError>> {
    match name {
        "GETDATE" | "CURRENT_TIMESTAMP" => {
            if !args.is_empty() {
                return Some(Err(DbError::Execution(format!("{} expects no arguments", name))));
            }
            Some(Ok(Value::DateTime(clock.now_datetime_literal())))
        }
        "CURRENT_DATE" => {
            if !args.is_empty() {
                return Some(Err(DbError::Execution("CURRENT_DATE expects no arguments".into())));
            }
            let dt = clock.now_datetime_literal();
            let date_str = if dt.len() >= 10 { &dt[..10] } else { "1970-01-01" };
            Some(Ok(Value::Date(date_str.to_string())))
        }
        "DATEADD" => Some(datetime::eval_dateadd(args, row, ctx, catalog, storage, clock)),
        "DATEDIFF" => Some(datetime::eval_datediff(args, row, ctx, catalog, storage, clock)),
        "DATEPART" => Some(datetime::eval_datepart(args, row, ctx, catalog, storage, clock)),
        "DATENAME" => Some(datetime::eval_datename(args, row, ctx, catalog, storage, clock)),
        "YEAR" => Some(datetime::eval_year(args, row, ctx, catalog, storage, clock)),
        "MONTH" => Some(datetime::eval_month(args, row, ctx, catalog, storage, clock)),
        "DAY" => Some(datetime::eval_day(args, row, ctx, catalog, storage, clock)),
        _ => None,
    }
}

fn try_string_dispatch(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Option<Result<Value, DbError>> {
    match name {
        "LEN" => Some(string::eval_len(args, row, ctx, catalog, storage, clock)),
        "SUBSTRING" => Some(string::eval_substring(args, row, ctx, catalog, storage, clock)),
        "UPPER" => Some(string::eval_upper(args, row, ctx, catalog, storage, clock)),
        "LOWER" => Some(string::eval_lower(args, row, ctx, catalog, storage, clock)),
        "LTRIM" => Some(string::eval_trim(args, row, ctx, catalog, storage, clock, true, false)),
        "RTRIM" => Some(string::eval_trim(args, row, ctx, catalog, storage, clock, false, true)),
        "TRIM" => Some(string::eval_trim(args, row, ctx, catalog, storage, clock, true, true)),
        "REPLACE" => Some(string::eval_replace(args, row, ctx, catalog, storage, clock)),
        "LEFT" => Some(string::eval_left(args, row, ctx, catalog, storage, clock)),
        "RIGHT" => Some(string::eval_right(args, row, ctx, catalog, storage, clock)),
        "CHARINDEX" => Some(string::eval_charindex(args, row, ctx, catalog, storage, clock)),
        "UNISTR" => Some(string::eval_unistr(args, row, ctx, catalog, storage, clock)),
        "ASCII" => Some(string::eval_ascii(args, row, ctx, catalog, storage, clock)),
        "CHAR" => Some(string::eval_char(args, row, ctx, catalog, storage, clock)),
        "NCHAR" => Some(string::eval_nchar(args, row, ctx, catalog, storage, clock)),
        "UNICODE" => Some(string::eval_unicode(args, row, ctx, catalog, storage, clock)),
        "STRING_ESCAPE" => Some(string::eval_string_escape(args, row, ctx, catalog, storage, clock)),
        "CONCAT" => Some(string::eval_concat(args, row, ctx, catalog, storage, clock)),
        "CONCAT_WS" => Some(string::eval_concat_ws(args, row, ctx, catalog, storage, clock)),
        "REPLICATE" => Some(string::eval_replicate(args, row, ctx, catalog, storage, clock)),
        "REVERSE" => Some(string::eval_reverse(args, row, ctx, catalog, storage, clock)),
        "STUFF" => Some(string::eval_stuff(args, row, ctx, catalog, storage, clock)),
        "SPACE" => Some(string::eval_space(args, row, ctx, catalog, storage, clock)),
        "STR" => Some(string::eval_str(args, row, ctx, catalog, storage, clock)),
        "TRANSLATE" => Some(string::eval_translate(args, row, ctx, catalog, storage, clock)),
        "FORMAT" => Some(string::eval_format(args, row, ctx, catalog, storage, clock)),
        "PATINDEX" => Some(string::eval_patindex(args, row, ctx, catalog, storage, clock)),
        "SOUNDEX" => Some(string::eval_soundex(args, row, ctx, catalog, storage, clock)),
        "DIFFERENCE" => Some(string::eval_difference(args, row, ctx, catalog, storage, clock)),
        _ => None,
    }
}

fn try_math_dispatch(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Option<Result<Value, DbError>> {
    match name {
        "ROUND" => Some(math::eval_round(args, row, ctx, catalog, storage, clock)),
        "CEILING" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "CEILING", |f| f.ceil())),
        "FLOOR" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "FLOOR", |f| f.floor())),
        "ABS" => Some(math::eval_abs(args, row, ctx, catalog, storage, clock)),
        "POWER" => Some(math::eval_power(args, row, ctx, catalog, storage, clock)),
        "SQRT" => Some(math::eval_sqrt(args, row, ctx, catalog, storage, clock)),
        "SIGN" => Some(math::eval_sign(args, row, ctx, catalog, storage, clock)),
        "ACOS" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "ACOS", f64::acos)),
        "ASIN" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "ASIN", f64::asin)),
        "ATAN" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "ATAN", f64::atan)),
        "ATN2" => Some(math::eval_atn2(args, row, ctx, catalog, storage, clock)),
        "COS" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "COS", f64::cos)),
        "COT" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "COT", |f| 1.0 / f.tan())),
        "DEGREES" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "DEGREES", f64::to_degrees)),
        "EXP" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "EXP", f64::exp)),
        "LOG" => Some(math::eval_log(args, row, ctx, catalog, storage, clock)),
        "LOG10" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "LOG10", f64::log10)),
        "PI" => Some(math::eval_pi(args)),
        "RADIANS" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "RADIANS", f64::to_radians)),
        "SIN" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "SIN", f64::sin)),
        "SQUARE" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "SQUARE", |f| f * f)),
        "TAN" => Some(math::eval_math_unary(args, row, ctx, catalog, storage, clock, "TAN", f64::tan)),
        "CHECKSUM" => Some(math::eval_checksum(args, row, ctx, catalog, storage, clock)),
        _ => None,
    }
}

fn try_system_dispatch(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Option<Result<Value, DbError>> {
    match name {
        "NEWID" => {
            if !args.is_empty() {
                return Some(Err(DbError::Execution("NEWID expects no arguments".into())));
            }
            let uuid = system::deterministic_uuid(&mut *ctx.session.random_state);
            Some(Ok(Value::UniqueIdentifier(uuid)))
        }
        "RAND" => {
            let val = system::deterministic_rand(&mut *ctx.session.random_state);
            Some(Ok(Value::Decimal((val * 1_000_000_000.0) as i128, 9)))
        }
        "OBJECT_ID" => Some(system::eval_object_id(args, row, ctx, catalog, storage, clock)),
        "COLUMNPROPERTY" => Some(system::eval_columnproperty(args, row, ctx, catalog, storage, clock)),
        "OBJECT_NAME" => Some(metadata::eval_object_name(args, row, ctx, catalog, storage, clock)),
        "OBJECT_SCHEMA_NAME" => Some(metadata::eval_object_schema_name(args, row, ctx, catalog, storage, clock)),
        "OBJECT_DEFINITION" => Some(metadata::eval_object_definition(args, row, ctx, catalog, storage, clock)),
        "OBJECTPROPERTY" => Some(metadata::eval_objectproperty(args, row, ctx, catalog, storage, clock)),
        "OBJECTPROPERTYEX" => Some(metadata::eval_objectpropertyex(args, row, ctx, catalog, storage, clock)),
        "SCHEMA_ID" => Some(metadata::eval_schema_id(args, row, ctx, catalog, storage, clock)),
        "SCHEMA_NAME" => Some(metadata::eval_schema_name(args, row, ctx, catalog, storage, clock)),
        "TYPE_ID" => Some(metadata::eval_type_id(args, row, ctx, catalog, storage, clock)),
        "TYPE_NAME" => Some(metadata::eval_type_name(args, row, ctx, catalog, storage, clock)),
        "TYPEPROPERTY" => Some(metadata::eval_typeproperty(args, row, ctx, catalog, storage, clock)),
        "COL_NAME" => Some(metadata::eval_col_name(args, row, ctx, catalog, storage, clock)),
        "COL_LENGTH" => Some(metadata::eval_col_length(args, row, ctx, catalog, storage, clock)),
        "INDEX_COL" => Some(metadata::eval_index_col(args, row, ctx, catalog, storage, clock)),
        "INDEXKEY_PROPERTY" => Some(metadata::eval_indexkey_property(args, row, ctx, catalog, storage, clock)),
        "INDEXPROPERTY" => Some(metadata::eval_indexproperty(args, row, ctx, catalog, storage, clock)),
        "DATABASEPROPERTYEX" => Some(metadata::eval_databasepropertyex(args, row, ctx, catalog, storage, clock)),
        "ORIGINAL_DB_NAME" => Some(metadata::eval_original_db_name(args, ctx)),
        "@@PROCID" => Some(metadata::eval_procid(ctx)),
        "SCOPE_IDENTITY" => {
            if !args.is_empty() {
                return Some(Err(DbError::Execution("SCOPE_IDENTITY expects no arguments".into())));
            }
            Some(Ok(match ctx.current_scope_identity() {
                Some(v) => Value::BigInt(v),
                None => Value::Null,
            }))
        }
        "@@IDENTITY" => {
            if !args.is_empty() {
                return Some(Err(DbError::Execution("@@IDENTITY expects no arguments".into())));
            }
            Some(Ok(match *ctx.session.last_identity {
                Some(v) => Value::BigInt(v),
                None => Value::Null,
            }))
        }
        "IDENT_CURRENT" => Some(system::eval_ident_current(args, row, ctx, catalog, storage, clock)),
        "@@VERSION" => Some(Ok(Value::NVarChar("Microsoft SQL Server 2022 (RTM) - 16.0.1000.6 (tsql_wasm emulator)".into()))),
        "@@SERVERNAME" => Some(Ok(Value::NVarChar("localhost".into()))),
        "@@SERVICENAME" => Some(Ok(Value::NVarChar("MSSQLSERVER".into()))),
        "@@SPID" => Some(Ok(Value::SmallInt(1))),
        "@@TRANCOUNT" => Some(Ok(Value::Int(ctx.trancount() as i32))),
        "XACT_STATE" => {
            if !args.is_empty() {
                return Some(Err(DbError::Execution("XACT_STATE expects no arguments".into())));
            }
            Some(Ok(Value::Int(ctx.xact_state() as i32)))
        }
        "@@ERROR" => Some(Ok(Value::Int(0))),
        "@@FETCH_STATUS" => Some(Ok(Value::Int(*ctx.session.fetch_status))),
        "@@LANGUAGE" => Some(Ok(Value::NVarChar("us_english".into()))),
        "@@TEXTSIZE" => Some(Ok(Value::Int(2147483647))),
        "@@MAX_PRECISION" => Some(Ok(Value::TinyInt(38))),
        "@@DATEFIRST" => Some(Ok(Value::TinyInt(ctx.datefirst as u8))),
        "@@MICROSOFTVERSION" => Some(Ok(system::eval_microsoft_version())),
        "ERROR_MESSAGE" => Some(system::eval_error_message(ctx)),
        "ERROR_NUMBER" => Some(system::eval_error_number(ctx)),
        "ERROR_SEVERITY" => Some(system::eval_error_severity(ctx)),
        "ERROR_STATE" => Some(system::eval_error_state(ctx)),
        "DB_NAME" => Some(system::eval_db_name(args, ctx)),
        "DB_ID" => Some(system::eval_db_id(args, ctx)),
        "SUSER_SNAME" => Some(system::eval_suser_sname(args, ctx)),
        "SUSER_ID" => Some(system::eval_suser_id(args, ctx)),
        "USER_NAME" => Some(system::eval_user_name(args, ctx)),
        "USER_ID" => Some(system::eval_user_id(args, ctx)),
        "APP_NAME" => Some(system::eval_app_name(args, ctx)),
        "HOST_NAME" => Some(system::eval_host_name(args, ctx)),
        "SYSTEM_USER" => Some(system::eval_system_user(args, ctx)),
        "ORIGINAL_LOGIN" => Some(system::eval_original_login(args, ctx)),
        "HASHBYTES" => Some(system::eval_hashbytes(args, row, ctx, catalog, storage, clock)),
        "PARSENAME" => Some(system::eval_parsename(args, row, ctx, catalog, storage, clock)),
        "QUOTENAME" => Some(system::eval_quotename(args, row, ctx, catalog, storage, clock)),
        "SESSION_USER" => Some(system::eval_session_user(args, ctx)),
        "CURRENT_USER" => Some(system::eval_current_user(args, ctx)),
        "SERVERPROPERTY" => Some(system::eval_serverproperty(args, row, ctx, catalog, storage, clock)),
        "CONNECTIONPROPERTY" => Some(system::eval_connectionproperty(args, row, ctx, catalog, storage, clock)),
        _ if name.starts_with("@@") => match system_vars::resolve_system_variable(name, ctx) {
            Ok(Some(val)) => Some(Ok(val)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        },
        _ => None,
    }
}

fn try_json_dispatch(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Option<Result<Value, DbError>> {
    match name {
        "JSON_VALUE" => Some(eval_json_value(args, row, ctx, catalog, storage, clock)),
        "JSON_QUERY" => Some(eval_json_query(args, row, ctx, catalog, storage, clock)),
        "JSON_MODIFY" => Some(eval_json_modify(args, row, ctx, catalog, storage, clock)),
        "ISJSON" => Some(eval_isjson(args, row, ctx, catalog, storage, clock)),
        "JSON_ARRAY_LENGTH" => Some(eval_json_array_length(args, row, ctx, catalog, storage, clock)),
        "JSON_KEYS" => Some(eval_json_keys(args, row, ctx, catalog, storage, clock)),
        _ => None,
    }
}

fn try_regexp_dispatch(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Option<Result<Value, DbError>> {
    match name {
        "REGEXP_LIKE" => Some(eval_regexp_like(args, row, ctx, catalog, storage, clock)),
        "REGEXP_REPLACE" => Some(eval_regexp_replace(args, row, ctx, catalog, storage, clock)),
        "REGEXP_SUBSTR" => Some(eval_regexp_substr(args, row, ctx, catalog, storage, clock)),
        "REGEXP_INSTR" => Some(eval_regexp_instr(args, row, ctx, catalog, storage, clock)),
        "REGEXP_COUNT" => Some(eval_regexp_count(args, row, ctx, catalog, storage, clock)),
        _ => None,
    }
}

fn try_fuzzy_dispatch(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Option<Result<Value, DbError>> {
    match name {
        "EDIT_DISTANCE" => Some(eval_edit_distance(args, row, ctx, catalog, storage, clock)),
        "EDIT_DISTANCE_SIMILARITY" => Some(eval_edit_distance_similarity(args, row, ctx, catalog, storage, clock)),
        "JARO_WINKLER_DISTANCE" => Some(eval_jaro_winkler_distance(args, row, ctx, catalog, storage, clock)),
        "JARO_WINKLER_SIMILARITY" => Some(eval_jaro_winkler_similarity(args, row, ctx, catalog, storage, clock)),
        _ => None,
    }
}

fn eval_user_scalar_function<'a>(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let (schema, fname) = if let Some(dot) = name.find('.') {
        (&name[..dot], &name[dot + 1..])
    } else {
        ("dbo", name)
    };
    let Some(routine) = catalog.find_routine(schema, fname) else {
        return Err(DbError::Execution(format!(
            "function '{}' not supported",
            name
        )));
    };
    let RoutineKind::Function { body, .. } = &routine.kind else {
        return Err(DbError::Execution(format!("'{}' is not a function", name)));
    };
    if args.len() != routine.params.len() {
        return Err(DbError::Execution(format!(
            "function '{}' expected {} args, got {}",
            name,
            routine.params.len(),
            args.len()
        )));
    }

    ctx.push_module(ModuleFrame {
        object_id: routine.object_id,
        schema: routine.schema.clone(),
        name: routine.name.clone(),
        kind: ModuleKind::Function,
    });
    let scope_depth = ctx.frame.scope_vars.len();
    let out = (|| {
        ctx.enter_scope();
        for (param, arg_expr) in routine.params.iter().zip(args.iter()) {
            let RoutineParamType::Scalar(dt) = &param.param_type else {
                return Err(DbError::Execution(format!(
                    "function '{}' has unsupported non-scalar parameter '{}'",
                    name, param.name
                )));
            };
            let val = eval_expr(arg_expr, row, ctx, catalog, storage, clock)?;
            let ty = super::type_mapping::data_type_spec_to_runtime(dt);
            let coerced = super::value_ops::coerce_value_to_type(val, &ty)?;
            ctx.session.variables.insert(param.name.clone(), (ty, coerced));
            ctx.register_declared_var(&param.name);
        }
        let out = match body {
            crate::ast::FunctionBody::ScalarReturn(expr) => {
                eval_expr(expr, row, ctx, catalog, storage, clock)
            }
            crate::ast::FunctionBody::Scalar(stmts) => {
                super::evaluator::eval_udf_body(stmts, ctx, catalog, storage, clock)
            }
            crate::ast::FunctionBody::InlineTable(_) => Err(DbError::Execution(format!(
                "inline TVF '{}' cannot be used in scalar context",
                name
            ))),
        };
        ctx.leave_scope();
        out
    })();
    while ctx.frame.scope_vars.len() > scope_depth {
        ctx.leave_scope();
    }
    ctx.pop_module();
    out
}

// Delegate JSON functions to existing json module
fn eval_json_value(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("JSON_VALUE expects 2 arguments".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let path_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if json_val.is_null() {
        return Ok(Value::Null);
    }
    json::json_value(&json_val.to_string_value(), &path_val.to_string_value())
}

fn eval_json_query(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution("JSON_QUERY expects 2 arguments".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let path_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if json_val.is_null() {
        return Ok(Value::Null);
    }
    json::json_query(&json_val.to_string_value(), &path_val.to_string_value())
}

fn eval_json_modify(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 3 {
        return Err(DbError::Execution("JSON_MODIFY expects 3 arguments".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let path_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let new_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
    if json_val.is_null() {
        return Ok(Value::Null);
    }
    json::json_modify(
        &json_val.to_string_value(),
        &path_val.to_string_value(),
        &new_val.to_string_value(),
    )
}

fn eval_isjson(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution("ISJSON expects 1 argument".into()));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if json_val.is_null() {
        return Ok(Value::Null);
    }
    json::is_json(&json_val.to_string_value())
}

fn eval_json_array_length(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 1 {
        return Err(DbError::Execution(
            "JSON_ARRAY_LENGTH expects 1 argument".into(),
        ));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if json_val.is_null() {
        return Ok(Value::Null);
    }
    json::json_array_length(&json_val.to_string_value())
}

fn eval_json_keys(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.is_empty() || args.len() > 2 {
        return Err(DbError::Execution(
            "JSON_KEYS expects 1 or 2 arguments".into(),
        ));
    }
    let json_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    if json_val.is_null() {
        return Ok(Value::Null);
    }
    let path = if args.len() == 2 {
        Some(eval_expr(&args[1], row, ctx, catalog, storage, clock)?.to_string_value())
    } else {
        None
    };
    json::json_keys(&json_val.to_string_value(), path.as_deref())
}

// Delegate Regexp functions
fn eval_regexp_like(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(DbError::Execution(
            "REGEXP_LIKE expects 2 or 3 arguments".into(),
        ));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s_val.is_null() || p_val.is_null() {
        return Ok(Value::Null);
    }
    let flags = if args.len() == 3 {
        Some(eval_expr(&args[2], row, ctx, catalog, storage, clock)?.to_string_value())
    } else {
        None
    };
    regexp::regexp_like(
        &s_val.to_string_value(),
        &p_val.to_string_value(),
        flags.as_deref(),
    )
}

fn eval_regexp_replace(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 3 || args.len() > 4 {
        return Err(DbError::Execution(
            "REGEXP_REPLACE expects 3 or 4 arguments".into(),
        ));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    let r_val = eval_expr(&args[2], row, ctx, catalog, storage, clock)?;
    if s_val.is_null() || p_val.is_null() || r_val.is_null() {
        return Ok(Value::Null);
    }
    let flags = if args.len() == 4 {
        Some(eval_expr(&args[3], row, ctx, catalog, storage, clock)?.to_string_value())
    } else {
        None
    };
    regexp::regexp_replace(
        &s_val.to_string_value(),
        &p_val.to_string_value(),
        &r_val.to_string_value(),
        flags.as_deref(),
    )
}

fn eval_regexp_substr(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 5 {
        return Err(DbError::Execution(
            "REGEXP_SUBSTR expects 2 to 5 arguments".into(),
        ));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s_val.is_null() || p_val.is_null() {
        return Ok(Value::Null);
    }
    let pos = if args.len() >= 3 {
        eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_string_value()
            .parse::<usize>()
            .unwrap_or(1)
    } else {
        1
    };
    let occurrence = if args.len() >= 4 {
        eval_expr(&args[3], row, ctx, catalog, storage, clock)?
            .to_string_value()
            .parse::<usize>()
            .unwrap_or(0)
    } else {
        0
    };
    let flags = if args.len() == 5 {
        Some(eval_expr(&args[4], row, ctx, catalog, storage, clock)?.to_string_value())
    } else {
        None
    };
    regexp::regexp_substr(
        &s_val.to_string_value(),
        &p_val.to_string_value(),
        pos,
        occurrence,
        flags.as_deref(),
    )
}

fn eval_regexp_instr(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 6 {
        return Err(DbError::Execution(
            "REGEXP_INSTR expects 2 to 6 arguments".into(),
        ));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s_val.is_null() || p_val.is_null() {
        return Ok(Value::Null);
    }
    let pos = if args.len() >= 3 {
        eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_string_value()
            .parse::<usize>()
            .unwrap_or(1)
    } else {
        1
    };
    let occurrence = if args.len() >= 4 {
        eval_expr(&args[3], row, ctx, catalog, storage, clock)?
            .to_string_value()
            .parse::<usize>()
            .unwrap_or(0)
    } else {
        0
    };
    let return_opt = if args.len() >= 5 {
        eval_expr(&args[4], row, ctx, catalog, storage, clock)?
            .to_string_value()
            .parse::<usize>()
            .unwrap_or(0)
    } else {
        0
    };
    let flags = if args.len() == 6 {
        Some(eval_expr(&args[5], row, ctx, catalog, storage, clock)?.to_string_value())
    } else {
        None
    };
    regexp::regexp_instr(
        &s_val.to_string_value(),
        &p_val.to_string_value(),
        pos,
        occurrence,
        return_opt,
        flags.as_deref(),
    )
}

fn eval_regexp_count(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() < 2 || args.len() > 4 {
        return Err(DbError::Execution(
            "REGEXP_COUNT expects 2 to 4 arguments".into(),
        ));
    }
    let s_val = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let p_val = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s_val.is_null() || p_val.is_null() {
        return Ok(Value::Null);
    }
    let pos = if args.len() >= 3 {
        eval_expr(&args[2], row, ctx, catalog, storage, clock)?
            .to_string_value()
            .parse::<usize>()
            .unwrap_or(1)
    } else {
        1
    };
    let flags = if args.len() == 4 {
        Some(eval_expr(&args[3], row, ctx, catalog, storage, clock)?.to_string_value())
    } else {
        None
    };
    regexp::regexp_count(
        &s_val.to_string_value(),
        &p_val.to_string_value(),
        pos,
        flags.as_deref(),
    )
}

// Delegate Fuzzy functions
fn eval_edit_distance(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "EDIT_DISTANCE expects 2 arguments".into(),
        ));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }
    Ok(Value::Int(fuzzy::edit_distance(
        &s1.to_string_value(),
        &s2.to_string_value(),
    )))
}

fn eval_edit_distance_similarity(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "EDIT_DISTANCE_SIMILARITY expects 2 arguments".into(),
        ));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }
    let similarity = fuzzy::edit_distance_similarity(&s1.to_string_value(), &s2.to_string_value());
    Ok(Value::Decimal((similarity * 1_000_000_000.0) as i128, 9))
}

fn eval_jaro_winkler_distance(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "JARO_WINKLER_DISTANCE expects 2 arguments".into(),
        ));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }
    let distance = fuzzy::jaro_winkler_distance(&s1.to_string_value(), &s2.to_string_value());
    Ok(Value::Decimal((distance * 1_000_000_000.0) as i128, 9))
}

fn eval_jaro_winkler_similarity(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "JARO_WINKLER_SIMILARITY expects 2 arguments".into(),
        ));
    }
    let s1 = eval_expr(&args[0], row, ctx, catalog, storage, clock)?;
    let s2 = eval_expr(&args[1], row, ctx, catalog, storage, clock)?;
    if s1.is_null() || s2.is_null() {
        return Ok(Value::Null);
    }
    let similarity = fuzzy::jaro_winkler_similarity(&s1.to_string_value(), &s2.to_string_value());
    Ok(Value::Decimal((similarity * 1_000_000_000.0) as i128, 9))
}
