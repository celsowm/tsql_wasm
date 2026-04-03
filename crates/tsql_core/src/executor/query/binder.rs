use crate::ast::RoutineParamType;
use crate::ast::{SelectStmt, TableFactor, TableRef};
use crate::catalog::{Catalog, ColumnDef, RoutineKind, TableDef};
use crate::error::DbError;
use crate::parser::parse_expr_subquery_aware;
use crate::storage::{Storage, StoredRow};
use crate::types::Value;

use super::super::clock::Clock;
use super::super::context::{ExecutionContext, ModuleFrame, ModuleKind};
use super::super::evaluator::eval_expr;
use super::super::model::BoundTable;
use super::super::query_planner::bind_table as planner_bind_table;
use super::super::result::QueryResult;

pub(crate) fn bind_table(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    tref: TableRef,
    ctx: &mut ExecutionContext,
    query_executor_proxy: impl Fn(SelectStmt, &mut ExecutionContext) -> Result<QueryResult, DbError>,
) -> Result<BoundTable, DbError> {
    if let TableFactor::Derived(ref select) = tref.factor {
        let alias = tref
            .alias
            .clone()
            .ok_or_else(|| DbError::Semantic("subquery in FROM must have an alias".into()))?;
        let result = query_executor_proxy(*select.clone(), ctx)?;

        let table_def = TableDef {
            id: 0,
            schema_id: 1,
            name: alias.clone(),
            columns: result
                .columns
                .iter()
                .enumerate()
                .map(|(i, cname)| ColumnDef {
                    id: (i + 1) as u32,
                    name: cname.clone(),
                    data_type: result.column_types[i].clone(),
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    identity: None,
                    default: None,
                    default_constraint_name: None,
                    check: None,
                    check_constraint_name: None,
                    computed_expr: None,
                })
                .collect(),
            check_constraints: vec![],
            foreign_keys: vec![],
        };

        let rows = result
            .rows
            .into_iter()
            .map(|values| StoredRow {
                values,
                deleted: false,
            })
            .collect();

        return Ok(BoundTable {
            alias,
            table: table_def,
            virtual_rows: Some(rows),
        });
    }

    if let Some(bound_tvf) = bind_builtin_tvf(catalog, storage, clock, &tref, ctx)? {
        return Ok(bound_tvf);
    }
    if let Some(bound_tvf) =
        bind_inline_tvf(catalog, storage, clock, &tref, ctx, &query_executor_proxy)?
    {
        return Ok(bound_tvf);
    }
    if let Some(bound_view) = bind_view(catalog, storage, clock, &tref, ctx, &query_executor_proxy)?
    {
        return Ok(bound_view);
    }
    planner_bind_table(tref, catalog, ctx)
}

fn bind_builtin_tvf(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    tref: &TableRef,
    ctx: &mut ExecutionContext,
) -> Result<Option<BoundTable>, DbError> {
    let name = match &tref.factor {
        TableFactor::Named(o) => &o.name,
        TableFactor::Derived(_) => return Ok(None),
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

fn bind_view(
    catalog: &dyn Catalog,
    _storage: &dyn Storage,
    _clock: &dyn Clock,
    tref: &TableRef,
    ctx: &mut ExecutionContext,
    query_executor_proxy: &impl Fn(SelectStmt, &mut ExecutionContext) -> Result<QueryResult, DbError>,
) -> Result<Option<BoundTable>, DbError> {
    let schema = tref.factor.as_object_name().map(|o| o.schema_or_dbo()).unwrap_or("dbo");
    let name = match &tref.factor {
        TableFactor::Named(o) => &o.name,
        TableFactor::Derived(_) => return Ok(None),
    };

    let Some(view) = catalog.find_view(schema, name).cloned() else {
        return Ok(None);
    };

    let view_query = match view.query {
        crate::ast::Statement::Dml(crate::ast::DmlStatement::Select(s)) => s,
        _ => return Err(DbError::Execution("view query must be SELECT".into())),
    };

    let result = query_executor_proxy(view_query, ctx)?;

    let table_def = TableDef {
        id: 0,
        schema_id: 1,
        name: name.clone(),
        columns: result
            .columns
            .iter()
            .enumerate()
            .map(|(i, cname)| ColumnDef {
                id: (i + 1) as u32,
                name: cname.clone(),
                data_type: result.column_types[i].clone(),
                nullable: true,
                primary_key: false,
                unique: false,
                identity: None,
                default: None,
                default_constraint_name: None,
                check: None,
                check_constraint_name: None,
                computed_expr: None,
            })
            .collect(),
        check_constraints: vec![],
        foreign_keys: vec![],
    };
    let rows = result
        .rows
        .into_iter()
        .map(|values| StoredRow {
            values,
            deleted: false,
        })
        .collect::<Vec<_>>();
    Ok(Some(BoundTable {
        alias: tref.alias.clone().unwrap_or_else(|| name.clone()),
        table: table_def,
        virtual_rows: Some(rows),
    }))
}

fn bind_inline_tvf(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    tref: &TableRef,
    ctx: &mut ExecutionContext,
    query_executor_proxy: &impl Fn(SelectStmt, &mut ExecutionContext) -> Result<QueryResult, DbError>,
) -> Result<Option<BoundTable>, DbError> {
    let name = match &tref.factor {
        TableFactor::Named(o) => &o.name,
        TableFactor::Derived(_) => return Ok(None),
    };
    let Some(open) = name.find('(') else {
        return Ok(None);
    };
    if !name.ends_with(')') {
        return Ok(None);
    }
    let fname = name[..open].trim();
    let args_raw = &name[open + 1..name.len() - 1];
    let schema = tref.factor.as_object_name().map(|o| o.schema_or_dbo()).unwrap_or("dbo");
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
    let scope_depth = ctx.scope_vars.len();
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
            let ty = super::super::type_mapping::data_type_spec_to_runtime(dt);
            let coerced = super::super::value_ops::coerce_value_to_type(val, &ty)?;
            ctx.variables.insert(param.name.clone(), (ty, coerced));
            ctx.register_declared_var(&param.name);
        }

        let result = query_executor_proxy(query, ctx)?;
        ctx.leave_scope();
        Ok(result)
    })();
    while ctx.scope_vars.len() > scope_depth {
        ctx.leave_scope();
    }
    ctx.pop_module();
    let result = result?;

    let table_def = TableDef {
        id: 0,
        schema_id: 1,
        name: fname.to_string(),
        columns: result
            .columns
            .iter()
            .enumerate()
            .map(|(i, cname)| ColumnDef {
                id: (i + 1) as u32,
                name: cname.clone(),
                data_type: result.column_types[i].clone(),
                nullable: true,
                primary_key: false,
                unique: false,
                identity: None,
                default: None,
                default_constraint_name: None,
                check: None,
                check_constraint_name: None,
                computed_expr: None,
            })
            .collect(),
        check_constraints: vec![],
        foreign_keys: vec![],
    };
    let rows = result
        .rows
        .into_iter()
        .map(|values| StoredRow {
            values,
            deleted: false,
        })
        .collect::<Vec<_>>();
    Ok(Some(BoundTable {
        alias: tref.alias.clone().unwrap_or_else(|| fname.to_string()),
        table: table_def,
        virtual_rows: Some(rows),
    }))
}

pub(crate) fn split_csv_top_level_local(input: &str) -> Vec<String> {
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
