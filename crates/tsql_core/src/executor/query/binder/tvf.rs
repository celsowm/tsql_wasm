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
        alias: "STRING_SPLIT".to_string(),
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
