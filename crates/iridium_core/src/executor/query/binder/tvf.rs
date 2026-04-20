use crate::ast::RoutineParamType;
use crate::ast::TableFactor;
use crate::ast::TableRef;
use crate::catalog::{Catalog, ColumnDef, RoutineKind, TableDef};
use crate::error::DbError;
use crate::parser::parse_expr_subquery_aware;
use crate::storage::{Storage, StoredRow};
use crate::types::Value;

use super::super::QueryExecutor;
use super::query_result_to_bound_table;
use crate::executor::clock::Clock;
use crate::executor::context::{ExecutionContext, ModuleFrame, ModuleKind};
use crate::executor::evaluator::eval_expr;
use crate::executor::model::BoundTable;
use crate::executor::{type_mapping, value_ops};

pub(super) fn bind_builtin_tvf(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    tref: &TableRef,
    ctx: &mut ExecutionContext,
) -> Result<Option<BoundTable>, DbError> {
    let name = match &tref.factor {
        TableFactor::Named(o) => &o.name,
        TableFactor::Derived(_) => return Ok(None),
        TableFactor::Values { .. } => return Ok(None),
    };
    let upper = name.to_uppercase();

    if upper.starts_with("OPENJSON(") {
        return bind_openjson(catalog, storage, clock, tref, ctx, name);
    }

    if !upper.starts_with("STRING_SPLIT(") {
        return Ok(None);
    }

    let inner = name
        .strip_prefix("STRING_SPLIT(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| {
            DbError::Parse("STRING_SPLIT requires (string, separator[, enable_ordinal])".into())
        })?;

    let parts = crate::parser::utils::split_csv_top_level(inner);
    if parts.len() < 2 || parts.len() > 3 {
        return Err(DbError::Parse(
            "STRING_SPLIT requires 2 or 3 arguments".into(),
        ));
    }

    let string_expr = parse_expr_subquery_aware(&parts[0])?;
    let separator_expr = parse_expr_subquery_aware(&parts[1])?;

    let enable_ordinal = if parts.len() == 3 {
        let ordinal_expr = parse_expr_subquery_aware(&parts[2])?;
        match eval_expr(&ordinal_expr, &[], ctx, catalog, storage, clock)? {
            Value::Int(v) => v != 0,
            Value::BigInt(v) => v != 0,
            Value::TinyInt(v) => v != 0,
            Value::SmallInt(v) => v != 0,
            Value::Bit(v) => v,
            _ => {
                return Err(DbError::Execution(
                    "STRING_SPLIT third argument (enable_ordinal) must be an integer".into(),
                ))
            }
        }
    } else {
        false
    };

    let string_val = eval_expr(&string_expr, &[], ctx, catalog, storage, clock)?;
    let separator_val = eval_expr(&separator_expr, &[], ctx, catalog, storage, clock)?;

    let string_str = match &string_val {
        Value::VarChar(s) | Value::NVarChar(s) | Value::Char(s) | Value::NChar(s) => s.clone(),
        _ => {
            return Err(DbError::Execution(
                "STRING_SPLIT first argument must be a string".into(),
            ))
        }
    };

    let separator_str = match &separator_val {
        Value::VarChar(s) | Value::NVarChar(s) | Value::Char(s) | Value::NChar(s) => s.clone(),
        _ => {
            return Err(DbError::Execution(
                "STRING_SPLIT second argument must be a string".into(),
            ))
        }
    };

    let split_parts: Vec<&str> = string_str.split(&separator_str).collect();

    let mut columns = vec![ColumnDef {
        id: 1,
        name: "value".to_string(),
        data_type: crate::types::DataType::VarChar { max_len: 4000 },
        nullable: false,
        primary_key: false,
        unique: false,
        identity: None,
        default: None,
        default_constraint_name: None,
        check: None,
        check_constraint_name: None,
        computed_expr: None,
        collation: None,
        is_clustered: false,
        ansi_padding_on: true,
    }];

    if enable_ordinal {
        columns.push(ColumnDef {
            id: 2,
            name: "ordinal".to_string(),
            data_type: crate::types::DataType::Int,
            nullable: false,
            primary_key: false,
            unique: false,
            identity: None,
            default: None,
            default_constraint_name: None,
            check: None,
            check_constraint_name: None,
            computed_expr: None,
            collation: None,
            is_clustered: false,
            ansi_padding_on: true,
        });
    }

    let rows: Vec<StoredRow> = split_parts
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let mut values = vec![Value::VarChar(s.to_string())];
            if enable_ordinal {
                values.push(Value::Int((i + 1) as i32));
            }
            StoredRow {
                values,
                deleted: false,
            }
        })
        .collect();

    let table_def = TableDef {
        id: 0,
        schema_id: 1,
        schema_name: "dbo".to_string(),
        name: "STRING_SPLIT".to_string(),
        columns,
        check_constraints: vec![],
        foreign_keys: vec![],
    };

    Ok(Some(BoundTable {
        table: table_def,
        alias: tref.alias.clone().unwrap_or_else(|| "STRING_SPLIT".into()),
        virtual_rows: Some(rows),
    }))
}

pub(super) fn bind_inline_tvf(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    tref: &TableRef,
    ctx: &mut ExecutionContext,
    executor: &QueryExecutor<'_>,
) -> Result<Option<BoundTable>, DbError> {
    let name = match &tref.factor {
        TableFactor::Named(o) => &o.name,
        TableFactor::Derived(_) => return Ok(None),
        TableFactor::Values { .. } => return Ok(None),
    };
    let Some(open) = name.find('(') else {
        return Ok(None);
    };
    if !name.ends_with(')') {
        return Ok(None);
    }
    let fname = name[..open].trim();
    let args_raw = &name[open + 1..name.len() - 1];
    let schema = tref
        .factor
        .as_object_name()
        .map(|o| o.schema_or_dbo())
        .unwrap_or("dbo");
    let Some(routine) = catalog.find_routine(schema, fname).cloned() else {
        return Ok(None);
    };
    let crate::catalog::RoutineDef {
        object_id,
        schema: routine_schema,
        name: routine_name,
        params,
        kind,
        ..
    } = routine;
    let RoutineKind::Function { body, .. } = kind else {
        return Ok(None);
    };
    let crate::ast::FunctionBody::InlineTable(query) = body else {
        return Ok(None);
    };
    let arg_exprs = split_csv_top_level_local(args_raw);
    if arg_exprs.len() != params.len() {
        return Err(DbError::Execution(format!(
            "TVF '{}.{}' expected {} args, got {}",
            schema,
            fname,
            params.len(),
            arg_exprs.len()
        )));
    }

    ctx.push_module(ModuleFrame {
        object_id,
        schema: routine_schema.clone(),
        name: routine_name.clone(),
        kind: ModuleKind::Function,
    });
    let scope_depth = ctx.frame.scope_vars.len();
    let result = (|| {
        ctx.enter_scope();
        for (param, arg_raw) in params.iter().zip(arg_exprs.iter()) {
            let RoutineParamType::Scalar(dt) = &param.param_type else {
                return Err(DbError::Execution(format!(
                    "TVF '{}.{}' has unsupported non-scalar parameter '{}'",
                    schema, fname, param.name
                )));
            };
            let expr = parse_expr_subquery_aware(arg_raw)?;
            let val = eval_expr(&expr, &[], ctx, catalog, storage, clock)?;
            let ty = type_mapping::data_type_spec_to_runtime(dt);
            let coerced =
                value_ops::coerce_value_to_type_with_dateformat(val, &ty, &ctx.options.dateformat)?;
            ctx.session
                .variables
                .insert(param.name.clone(), (ty, coerced));
            ctx.register_declared_var(&param.name);
        }

        let result = executor.execute_select(query.into(), ctx)?;
        ctx.leave_scope();
        Ok(result)
    })();
    while ctx.frame.scope_vars.len() > scope_depth {
        ctx.leave_scope();
    }
    ctx.pop_module();
    let result = result?;

    Ok(Some(query_result_to_bound_table(
        tref.alias.clone().unwrap_or_else(|| fname.to_string()),
        fname.to_string(),
        result,
    )))
}

fn bind_openjson(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    tref: &TableRef,
    ctx: &mut ExecutionContext,
    name: &str,
) -> Result<Option<BoundTable>, DbError> {
    let inner = name
        .find('(')
        .and_then(|start| {
            let s = &name[start + 1..];
            s.strip_suffix(')').map(|s| s.to_string())
        })
        .ok_or_else(|| DbError::Parse("OPENJSON requires (json_expr[, path])".into()))?;

    let parts = crate::parser::utils::split_csv_top_level(&inner);
    if parts.is_empty() || parts.len() > 2 {
        return Err(DbError::Parse("OPENJSON requires 1 or 2 arguments".into()));
    }

    let json_expr = parse_expr_subquery_aware(&parts[0])?;
    let json_val = eval_expr(&json_expr, &[], ctx, catalog, storage, clock)?;

    let json_str = match &json_val {
        Value::VarChar(s) | Value::NVarChar(s) | Value::Char(s) | Value::NChar(s) => s.clone(),
        _ => {
            return Err(DbError::Execution(
                "OPENJSON first argument must be a string".into(),
            ))
        }
    };

    let parsed: serde_json::Value = serde_json::from_str(&json_str)
        .map_err(|e| DbError::Execution(format!("OPENJSON: invalid JSON: {}", e)))?;

    // If a path argument is provided, navigate to that path
    let target = if parts.len() == 2 {
        let path_expr = parse_expr_subquery_aware(&parts[1])?;
        let path_val = eval_expr(&path_expr, &[], ctx, catalog, storage, clock)?;
        let path_str = match &path_val {
            Value::VarChar(s) | Value::NVarChar(s) | Value::Char(s) | Value::NChar(s) => s.clone(),
            _ => {
                return Err(DbError::Execution(
                    "OPENJSON second argument (path) must be a string".into(),
                ))
            }
        };
        navigate_json_path(&parsed, &path_str)?
    } else {
        parsed
    };

    let rows = openjson_to_rows(&target);

    let columns = vec![
        ColumnDef {
            id: 1,
            name: "key".to_string(),
            data_type: crate::types::DataType::VarChar { max_len: 4000 },
            nullable: false,
            primary_key: false,
            unique: false,
            identity: None,
            default: None,
            default_constraint_name: None,
            check: None,
            check_constraint_name: None,
            computed_expr: None,
            collation: None,
            is_clustered: false,
            ansi_padding_on: true,
        },
        ColumnDef {
            id: 2,
            name: "value".to_string(),
            data_type: crate::types::DataType::VarChar { max_len: 4000 },
            nullable: true,
            primary_key: false,
            unique: false,
            identity: None,
            default: None,
            default_constraint_name: None,
            check: None,
            check_constraint_name: None,
            computed_expr: None,
            collation: None,
            is_clustered: false,
            ansi_padding_on: true,
        },
        ColumnDef {
            id: 3,
            name: "type".to_string(),
            data_type: crate::types::DataType::Int,
            nullable: false,
            primary_key: false,
            unique: false,
            identity: None,
            default: None,
            default_constraint_name: None,
            check: None,
            check_constraint_name: None,
            computed_expr: None,
            collation: None,
            is_clustered: false,
            ansi_padding_on: true,
        },
    ];

    let table_def = TableDef {
        id: 0,
        schema_id: 1,
        schema_name: "dbo".to_string(),
        name: "OPENJSON".to_string(),
        columns,
        check_constraints: vec![],
        foreign_keys: vec![],
    };

    Ok(Some(BoundTable {
        table: table_def,
        alias: tref.alias.clone().unwrap_or_else(|| "OPENJSON".into()),
        virtual_rows: Some(rows),
    }))
}

/// Returns the MSSQL OPENJSON type code for a JSON value.
/// 0=null, 1=string, 2=number, 3=bool, 4=array, 5=object
fn openjson_type_code(v: &serde_json::Value) -> i32 {
    match v {
        serde_json::Value::Null => 0,
        serde_json::Value::String(_) => 1,
        serde_json::Value::Number(_) => 2,
        serde_json::Value::Bool(_) => 3,
        serde_json::Value::Array(_) => 4,
        serde_json::Value::Object(_) => 5,
    }
}

/// Converts a JSON value (object or array) into OPENJSON result rows.
fn openjson_to_rows(val: &serde_json::Value) -> Vec<StoredRow> {
    match val {
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(k, v)| {
                let type_code = openjson_type_code(v);
                let value_str = match v {
                    serde_json::Value::Null => Value::Null,
                    serde_json::Value::String(s) => Value::NVarChar(s.clone()),
                    serde_json::Value::Number(n) => Value::NVarChar(n.to_string()),
                    serde_json::Value::Bool(b) => {
                        Value::NVarChar(if *b { "true" } else { "false" }.to_string())
                    }
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                        Value::NVarChar(v.to_string())
                    }
                };
                StoredRow {
                    values: vec![Value::NVarChar(k.clone()), value_str, Value::Int(type_code)],
                    deleted: false,
                }
            })
            .collect(),
        serde_json::Value::Array(arr) => arr
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let type_code = openjson_type_code(v);
                let value_str = match v {
                    serde_json::Value::Null => Value::Null,
                    serde_json::Value::String(s) => Value::NVarChar(s.clone()),
                    serde_json::Value::Number(n) => Value::NVarChar(n.to_string()),
                    serde_json::Value::Bool(b) => {
                        Value::NVarChar(if *b { "true" } else { "false" }.to_string())
                    }
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                        Value::NVarChar(v.to_string())
                    }
                };
                StoredRow {
                    values: vec![
                        Value::NVarChar(i.to_string()),
                        value_str,
                        Value::Int(type_code),
                    ],
                    deleted: false,
                }
            })
            .collect(),
        _ => vec![StoredRow {
            values: vec![
                Value::Null,
                match val {
                    serde_json::Value::Null => Value::Null,
                    serde_json::Value::String(s) => Value::NVarChar(s.clone()),
                    serde_json::Value::Number(n) => Value::NVarChar(n.to_string()),
                    serde_json::Value::Bool(b) => {
                        Value::NVarChar(if *b { "true" } else { "false" }.to_string())
                    }
                    _ => Value::Null,
                },
                Value::Int(openjson_type_code(val)),
            ],
            deleted: false,
        }],
    }
}

/// Navigate a JSON value using a SQL Server JSON path like '$.key.subkey' or '$[0]'.
fn navigate_json_path(root: &serde_json::Value, path: &str) -> Result<serde_json::Value, DbError> {
    let path = path.trim();
    if path == "$" {
        return Ok(root.clone());
    }

    let rest = path.strip_prefix('$').unwrap_or(path);
    let mut current = root.clone();

    let mut chars = rest.chars().peekable();
    while chars.peek().is_some() {
        match chars.peek() {
            Some('.') => {
                chars.next(); // consume '.'
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                if key.is_empty() {
                    return Err(DbError::Execution(
                        "OPENJSON: empty key in JSON path".into(),
                    ));
                }
                current = current
                    .get(&key)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
            }
            Some('[') => {
                chars.next(); // consume '['
                let mut idx_str = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        chars.next();
                        break;
                    }
                    idx_str.push(c);
                    chars.next();
                }
                let idx: usize = idx_str.parse().map_err(|_| {
                    DbError::Execution(format!(
                        "OPENJSON: invalid array index '{}' in path",
                        idx_str
                    ))
                })?;
                current = current.get(idx).cloned().unwrap_or(serde_json::Value::Null);
            }
            _ => {
                return Err(DbError::Execution(format!(
                    "OPENJSON: unexpected character in path '{}'",
                    path
                )));
            }
        }
    }

    Ok(current)
}

pub(super) fn split_csv_top_level_local(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut depth = 0usize;
    let mut in_string = false;
    for ch in input.chars() {
        match ch {
            '\'' => {
                in_string = !in_string;
                buf.push(ch);
            }
            '(' if !in_string => {
                depth += 1;
                buf.push(ch);
            }
            ')' if !in_string => {
                depth = depth.saturating_sub(1);
                buf.push(ch);
            }
            ',' if !in_string && depth == 0 => {
                if !buf.trim().is_empty() {
                    out.push(buf.trim().to_string());
                }
                buf.clear();
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() {
        out.push(buf.trim().to_string());
    }
    out
}
