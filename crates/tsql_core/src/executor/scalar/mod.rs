pub(crate) mod datetime;
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
use super::context::ExecutionContext;
use super::evaluator::eval_expr;
use super::fuzzy;
use super::json;
use super::model::ContextTable;
use super::regexp;

pub(crate) fn eval_function(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if is_aggregate_function(name) {
        if let Some(group) = ctx.current_group.clone() {
            if let Some(res) = dispatch_aggregate(name, args, &group, ctx, catalog, storage, clock)
            {
                return res;
            }
        }
    }

    match name.to_uppercase().as_str() {
        "GETDATE" => {
            if !args.is_empty() {
                return Err(DbError::Execution("GETDATE expects no arguments".into()));
            }
            Ok(Value::DateTime(clock.now_datetime_literal()))
        }
        "ISNULL" => logic::eval_isnull(args, row, ctx, catalog, storage, clock),
        "COALESCE" => logic::eval_coalesce(args, row, ctx, catalog, storage, clock),
        "LEN" => string::eval_len(args, row, ctx, catalog, storage, clock),
        "SUBSTRING" => string::eval_substring(args, row, ctx, catalog, storage, clock),
        "DATEADD" => datetime::eval_dateadd(args, row, ctx, catalog, storage, clock),
        "DATEDIFF" => datetime::eval_datediff(args, row, ctx, catalog, storage, clock),
        "DATEPART" => datetime::eval_datepart(args, row, ctx, catalog, storage, clock),
        "DATENAME" => datetime::eval_datename(args, row, ctx, catalog, storage, clock),
        "YEAR" => datetime::eval_year(args, row, ctx, catalog, storage, clock),
        "MONTH" => datetime::eval_month(args, row, ctx, catalog, storage, clock),
        "DAY" => datetime::eval_day(args, row, ctx, catalog, storage, clock),
        "UPPER" => string::eval_upper(args, row, ctx, catalog, storage, clock),
        "LOWER" => string::eval_lower(args, row, ctx, catalog, storage, clock),
        "LTRIM" => string::eval_trim(args, row, ctx, catalog, storage, clock, true, false),
        "RTRIM" => string::eval_trim(args, row, ctx, catalog, storage, clock, false, true),
        "TRIM" => string::eval_trim(args, row, ctx, catalog, storage, clock, true, true),
        "REPLACE" => string::eval_replace(args, row, ctx, catalog, storage, clock),
        "ROUND" => math::eval_round(args, row, ctx, catalog, storage, clock),
        "CEILING" => {
            math::eval_math_unary(args, row, ctx, catalog, storage, clock, "CEILING", |f| {
                f.ceil()
            })
        }
        "FLOOR" => math::eval_math_unary(args, row, ctx, catalog, storage, clock, "FLOOR", |f| {
            f.floor()
        }),
        "ABS" => math::eval_abs(args, row, ctx, catalog, storage, clock),
        "POWER" => math::eval_power(args, row, ctx, catalog, storage, clock),
        "SQRT" => math::eval_sqrt(args, row, ctx, catalog, storage, clock),
        "SIGN" => math::eval_sign(args, row, ctx, catalog, storage, clock),
        "LEFT" => string::eval_left(args, row, ctx, catalog, storage, clock),
        "RIGHT" => string::eval_right(args, row, ctx, catalog, storage, clock),
        "CHARINDEX" => string::eval_charindex(args, row, ctx, catalog, storage, clock),
        "UNISTR" => string::eval_unistr(args, row, ctx, catalog, storage, clock),
        "NEWID" => {
            if !args.is_empty() {
                return Err(DbError::Execution("NEWID expects no arguments".into()));
            }
            let uuid = system::deterministic_uuid(&mut *ctx.random_state);
            Ok(Value::UniqueIdentifier(uuid))
        }
        "RAND" => {
            let val = system::deterministic_rand(&mut *ctx.random_state);
            Ok(Value::Decimal((val * 1_000_000_000.0) as i128, 9))
        }
        "OBJECT_ID" => system::eval_object_id(args, row, ctx, catalog, storage, clock),
        "COLUMNPROPERTY" => system::eval_columnproperty(args, row, ctx, catalog, storage, clock),
        "SCOPE_IDENTITY" => {
            if !args.is_empty() {
                return Err(DbError::Execution(
                    "SCOPE_IDENTITY expects no arguments".into(),
                ));
            }
            Ok(match ctx.current_scope_identity() {
                Some(v) => Value::BigInt(v),
                None => Value::Null,
            })
        }
        "@@IDENTITY" => {
            if !args.is_empty() {
                return Err(DbError::Execution("@@IDENTITY expects no arguments".into()));
            }
            Ok(match *ctx.session_last_identity {
                Some(v) => Value::BigInt(v),
                None => Value::Null,
            })
        }
        "IDENT_CURRENT" => system::eval_ident_current(args, row, ctx, catalog, storage, clock),
        "JSON_VALUE" => eval_json_value(args, row, ctx, catalog, storage, clock),
        "JSON_QUERY" => eval_json_query(args, row, ctx, catalog, storage, clock),
        "JSON_MODIFY" => eval_json_modify(args, row, ctx, catalog, storage, clock),
        "ISJSON" => eval_isjson(args, row, ctx, catalog, storage, clock),
        "JSON_ARRAY_LENGTH" => eval_json_array_length(args, row, ctx, catalog, storage, clock),
        "JSON_KEYS" => eval_json_keys(args, row, ctx, catalog, storage, clock),
        "REGEXP_LIKE" => eval_regexp_like(args, row, ctx, catalog, storage, clock),
        "REGEXP_REPLACE" => eval_regexp_replace(args, row, ctx, catalog, storage, clock),
        "REGEXP_SUBSTR" => eval_regexp_substr(args, row, ctx, catalog, storage, clock),
        "REGEXP_INSTR" => eval_regexp_instr(args, row, ctx, catalog, storage, clock),
        "REGEXP_COUNT" => eval_regexp_count(args, row, ctx, catalog, storage, clock),
        "EDIT_DISTANCE" => eval_edit_distance(args, row, ctx, catalog, storage, clock),
        "EDIT_DISTANCE_SIMILARITY" => {
            eval_edit_distance_similarity(args, row, ctx, catalog, storage, clock)
        }
        "JARO_WINKLER_DISTANCE" => {
            eval_jaro_winkler_distance(args, row, ctx, catalog, storage, clock)
        }
        "JARO_WINKLER_SIMILARITY" => {
            eval_jaro_winkler_similarity(args, row, ctx, catalog, storage, clock)
        }
        "CURRENT_TIMESTAMP" => {
            if !args.is_empty() {
                return Err(DbError::Execution(
                    "CURRENT_TIMESTAMP expects no arguments".into(),
                ));
            }
            Ok(Value::DateTime(clock.now_datetime_literal()))
        }
        "CURRENT_DATE" => {
            if !args.is_empty() {
                return Err(DbError::Execution(
                    "CURRENT_DATE expects no arguments".into(),
                ));
            }
            let dt = clock.now_datetime_literal();
            let date_str = if dt.len() >= 10 {
                &dt[..10]
            } else {
                "1970-01-01"
            };
            Ok(Value::Date(date_str.to_string()))
        }
        "@@VERSION" => Ok(Value::NVarChar(
            "Microsoft SQL Server 2022 (RTM) - 16.0.1000.6 (tsql_wasm emulator)".into(),
        )),
        "@@SERVERNAME" => Ok(Value::NVarChar("localhost".into())),
        "@@SERVICENAME" => Ok(Value::NVarChar("MSSQLSERVER".into())),
        "@@SPID" => Ok(Value::SmallInt(1)),
        "@@TRANCOUNT" => Ok(Value::Int(ctx.trancount as i32)),
        "XACT_STATE" => {
            if !args.is_empty() {
                return Err(DbError::Execution("XACT_STATE expects no arguments".into()));
            }
            Ok(Value::Int(ctx.xact_state as i32))
        }
        "@@ERROR" => Ok(Value::Int(0)),
        "@@FETCH_STATUS" => Ok(Value::Int(*ctx.fetch_status)),
        "@@LANGUAGE" => Ok(Value::NVarChar("us_english".into())),
        "@@TEXTSIZE" => Ok(Value::Int(2147483647)),
        "@@MAX_PRECISION" => Ok(Value::TinyInt(38)),
        "@@DATEFIRST" => Ok(Value::TinyInt(ctx.datefirst as u8)),
        "ERROR_MESSAGE" => system::eval_error_message(ctx),
        "ERROR_NUMBER" => system::eval_error_number(ctx),
        "ERROR_SEVERITY" => system::eval_error_severity(ctx),
        "ERROR_STATE" => system::eval_error_state(ctx),
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

fn eval_user_scalar_function(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
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
        ctx.variables.insert(param.name.clone(), (ty, coerced));
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
