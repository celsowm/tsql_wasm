use super::super::ScriptExecutor;
use crate::ast::ExecProcedureStmt;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::metadata::{type_max_length, type_name};
use crate::executor::result::QueryResult;
use crate::types::{DataType, Value};

const SYSTEM_PROCEDURES: &[&str] = &[
    "sp_rename",
    "sp_help",
    "sp_helptext",
    "sp_columns",
    "sp_tables",
    "sp_helpindex",
    "sp_set_session_context",
];

pub(crate) fn is_system_procedure(name: &str) -> bool {
    SYSTEM_PROCEDURES
        .iter()
        .any(|sp| name.eq_ignore_ascii_case(sp))
}

pub(crate) fn execute_system_procedure(
    exec: &mut ScriptExecutor<'_>,
    stmt: &ExecProcedureStmt,
    ctx: &mut ExecutionContext<'_>,
) -> Result<Option<QueryResult>, DbError> {
    let name = &stmt.name.name;
    let args = eval_args(exec, &stmt.args, ctx)?;

    let result = if name.eq_ignore_ascii_case("sp_rename") {
        execute_sp_rename(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_help") {
        execute_sp_help(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_helptext") {
        execute_sp_helptext(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_columns") {
        execute_sp_columns(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_tables") {
        execute_sp_tables(exec)?
    } else if name.eq_ignore_ascii_case("sp_helpindex") {
        execute_sp_helpindex(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_set_session_context") {
        execute_sp_set_session_context(stmt, ctx, exec)?
    } else {
        return Err(DbError::Execution(format!(
            "unknown system procedure '{}'",
            name
        )));
    };

    let mut res = result;
    res.return_status = Some(0);
    res.is_procedure = true;
    Ok(Some(res))
}

fn eval_args(
    exec: &mut ScriptExecutor<'_>,
    args: &[crate::ast::ExecArgument],
    ctx: &mut ExecutionContext<'_>,
) -> Result<Vec<String>, DbError> {
    let mut result = Vec::new();
    for arg in args {
        let val = eval_expr(&arg.expr, &[], ctx, exec.catalog, exec.storage, exec.clock)?;
        result.push(val.to_string_value());
    }
    Ok(result)
}

fn execute_sp_rename(
    exec: &mut ScriptExecutor<'_>,
    args: &[String],
) -> Result<QueryResult, DbError> {
    if args.len() < 2 {
        return Err(DbError::Execution(
            "sp_rename requires at least 2 arguments: @objname, @newname".into(),
        ));
    }
    let objname = &args[0];
    let newname = &args[1];
    let objtype = args
        .get(2)
        .map(|s| s.as_str())
        .unwrap_or("OBJECT");

    if objtype.eq_ignore_ascii_case("COLUMN") {
        let parts: Vec<&str> = objname.splitn(2, '.').collect();
        if parts.len() != 2 {
            return Err(DbError::Execution(
                "sp_rename with @objtype='COLUMN' expects @objname as 'table.column'".into(),
            ));
        }
        let table_name = parts[0];
        let old_col = parts[1];
        let table = exec
            .catalog
            .find_table_mut("dbo", table_name)
            .ok_or_else(|| DbError::object_not_found(format!("table '{}'", table_name)))?;
        let col = table
            .columns
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(old_col))
            .ok_or_else(|| {
                DbError::object_not_found(format!("column '{}.{}'", table_name, old_col))
            })?;
        col.name = newname.clone();
    } else {
        let table = exec
            .catalog
            .find_table_mut("dbo", objname)
            .ok_or_else(|| DbError::object_not_found(format!("object '{}'", objname)))?;
        table.name = newname.clone();
        exec.catalog.rebuild_maps();
    }

    Ok(QueryResult::default())
}

fn execute_sp_help(
    exec: &mut ScriptExecutor<'_>,
    args: &[String],
) -> Result<QueryResult, DbError> {
    if args.is_empty() {
        let mut rows = Vec::new();
        for t in exec.catalog.get_tables() {
            rows.push(vec![
                Value::VarChar(t.name.clone()),
                Value::VarChar("dbo".into()),
                Value::VarChar("user table".into()),
                Value::VarChar(String::new()),
            ]);
        }
        for v in exec.catalog.get_views() {
            rows.push(vec![
                Value::VarChar(v.name.clone()),
                Value::VarChar("dbo".into()),
                Value::VarChar("view".into()),
                Value::VarChar(String::new()),
            ]);
        }
        for r in exec.catalog.get_routines() {
            let kind_str = match &r.kind {
                crate::catalog::RoutineKind::Procedure { .. } => "stored procedure",
                crate::catalog::RoutineKind::Function { .. } => "function",
            };
            rows.push(vec![
                Value::VarChar(r.name.clone()),
                Value::VarChar("dbo".into()),
                Value::VarChar(kind_str.into()),
                Value::VarChar(String::new()),
            ]);
        }
        Ok(QueryResult {
            columns: vec![
                "Name".into(),
                "Owner".into(),
                "Object_type".into(),
                "Created_datetime".into(),
            ],
            column_types: vec![
                DataType::NVarChar { max_len: 128 },
                DataType::NVarChar { max_len: 128 },
                DataType::NVarChar { max_len: 128 },
                DataType::NVarChar { max_len: 128 },
            ],
            column_nullabilities: vec![false, false, false, true],
            rows,
            ..Default::default()
        })
    } else {
        let table_name = &args[0];
        let table = exec
            .catalog
            .find_table("dbo", table_name)
            .ok_or_else(|| DbError::object_not_found(format!("object '{}'", table_name)))?;
        let rows: Vec<Vec<Value>> = table
            .columns
            .iter()
            .map(|c| {
                vec![
                    Value::VarChar(c.name.clone()),
                    Value::VarChar(type_name(&c.data_type)),
                    Value::VarChar(
                        if c.computed_expr.is_some() {
                            "yes"
                        } else {
                            "no"
                        }
                        .into(),
                    ),
                    Value::Int(type_max_length(&c.data_type) as i32),
                    Value::VarChar(if c.nullable { "yes" } else { "no" }.into()),
                ]
            })
            .collect();
        Ok(QueryResult {
            columns: vec![
                "Column_name".into(),
                "Type".into(),
                "Computed".into(),
                "Length".into(),
                "Nullable".into(),
            ],
            column_types: vec![
                DataType::NVarChar { max_len: 128 },
                DataType::NVarChar { max_len: 128 },
                DataType::NVarChar { max_len: 3 },
                DataType::Int,
                DataType::NVarChar { max_len: 3 },
            ],
            column_nullabilities: vec![false, false, false, false, false],
            rows,
            ..Default::default()
        })
    }
}

fn execute_sp_helptext(
    exec: &mut ScriptExecutor<'_>,
    args: &[String],
) -> Result<QueryResult, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution(
            "sp_helptext requires 1 argument: object name".into(),
        ));
    }
    let obj_name = &args[0];

    if let Some(view) = exec.catalog.find_view("dbo", obj_name) {
        let text = view.definition_sql.clone();
        return Ok(QueryResult {
            columns: vec!["Text".into()],
            column_types: vec![DataType::NVarChar { max_len: 4000 }],
            column_nullabilities: vec![false],
            rows: vec![vec![Value::NVarChar(text)]],
            ..Default::default()
        });
    }

    if let Some(routine) = exec.catalog.find_routine("dbo", obj_name).cloned() {
        let text = routine.definition_sql.clone();
        return Ok(QueryResult {
            columns: vec!["Text".into()],
            column_types: vec![DataType::NVarChar { max_len: 4000 }],
            column_nullabilities: vec![false],
            rows: vec![vec![Value::NVarChar(text)]],
            ..Default::default()
        });
    }

    Err(DbError::object_not_found(format!(
        "object '{}' does not exist or is not a valid object for sp_helptext",
        obj_name
    )))
}

fn execute_sp_columns(
    exec: &mut ScriptExecutor<'_>,
    args: &[String],
) -> Result<QueryResult, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution(
            "sp_columns requires 1 argument: table name".into(),
        ));
    }
    let table_name = &args[0];
    let table = exec
        .catalog
        .find_table("dbo", table_name)
        .ok_or_else(|| DbError::object_not_found(format!("table '{}'", table_name)))?;

    let rows: Vec<Vec<Value>> = table
        .columns
        .iter()
        .map(|c| {
            let tn = type_name(&c.data_type);
            let max_len = type_max_length(&c.data_type);
            let (precision, scale) = match &c.data_type {
                DataType::Decimal { precision, scale } => (*precision as i32, *scale as i32),
                _ => (0, 0),
            };
            vec![
                Value::VarChar(c.name.clone()),
                Value::Int(crate::executor::metadata::system_type_id(&c.data_type)),
                Value::VarChar(tn),
                Value::Int(precision),
                Value::Int(max_len as i32),
                Value::Int(scale),
                Value::Int(if c.nullable { 1 } else { 0 }),
            ]
        })
        .collect();

    Ok(QueryResult {
        columns: vec![
            "COLUMN_NAME".into(),
            "DATA_TYPE".into(),
            "TYPE_NAME".into(),
            "PRECISION".into(),
            "LENGTH".into(),
            "SCALE".into(),
            "NULLABLE".into(),
        ],
        column_types: vec![
            DataType::NVarChar { max_len: 128 },
            DataType::Int,
            DataType::NVarChar { max_len: 128 },
            DataType::Int,
            DataType::Int,
            DataType::Int,
            DataType::Int,
        ],
        column_nullabilities: vec![false, false, false, true, true, true, false],
        rows,
        ..Default::default()
    })
}

fn execute_sp_helpindex(
    exec: &mut ScriptExecutor<'_>,
    args: &[String],
) -> Result<QueryResult, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution(
            "sp_helpindex requires 1 argument: table name".into(),
        ));
    }
    let table_name_raw = &args[0];
    let parts: Vec<&str> = table_name_raw.rsplitn(2, '.').collect();
    let (schema, table_name) = if parts.len() == 2 {
        (parts[1], parts[0])
    } else {
        ("dbo", parts[0])
    };

    let table = exec
        .catalog
        .find_table(schema, table_name)
        .ok_or_else(|| DbError::object_not_found(format!("table '{}.{}'", schema, table_name)))?;

    let indexes = exec.catalog.get_indexes();
    let table_indexes: Vec<_> = indexes.iter().filter(|idx| idx.table_id == table.id).collect();

    let mut rows = Vec::new();
    for idx in table_indexes {
        let mut desc = Vec::new();
        if idx.is_clustered {
            desc.push("clustered");
        } else {
            desc.push("nonclustered");
        }
        if idx.is_unique {
            desc.push("unique");
        }
        desc.push("located on PRIMARY");

        let column_names: Vec<String> = idx
            .column_ids
            .iter()
            .map(|&cid| {
                table
                    .columns
                    .iter()
                    .find(|c| c.id == cid)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| "unknown".to_string())
            })
            .collect();

        rows.push(vec![
            Value::NVarChar(idx.name.clone()),
            Value::NVarChar(desc.join(", ")),
            Value::NVarChar(column_names.join(", ")),
        ]);
    }

    Ok(QueryResult {
        columns: vec![
            "index_name".into(),
            "index_description".into(),
            "index_keys".into(),
        ],
        column_types: vec![
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 256 },
            DataType::NVarChar { max_len: 2048 },
        ],
        column_nullabilities: vec![false, false, false],
        rows,
        ..Default::default()
    })
}

fn execute_sp_set_session_context(
    stmt: &ExecProcedureStmt,
    ctx: &mut ExecutionContext<'_>,
    exec: &mut ScriptExecutor<'_>,
) -> Result<QueryResult, DbError> {
    let mut key = String::new();
    let mut value = Value::Null;
    let mut read_only = false;

    for arg in &stmt.args {
        let val = eval_expr(&arg.expr, &[], ctx, exec.catalog, exec.storage, exec.clock)?;
        match arg.name.as_ref().map(|s| s.to_ascii_lowercase()) {
            Some(ref n) if n == "@key" => key = val.to_string_value(),
            Some(ref n) if n == "@value" => value = val,
            Some(ref n) if n == "@read_only" => read_only = val.to_bool().unwrap_or(false),
            _ => {
                // Positional arguments fallback if needed, but MSSQL usually uses named for this
            }
        }
    }

    if key.is_empty() {
        return Err(DbError::Execution(
            "sp_set_session_context: @key is required".into(),
        ));
    }

    if let Some((_, is_ro)) = ctx.session.session_context.get(&key) {
        if *is_ro {
            return Err(DbError::Execution(format!(
                "Cannot set value for read-only session context key '{}'",
                key
            )));
        }
    }

    ctx.session
        .session_context
        .insert(key, (value, read_only));

    Ok(QueryResult::default())
}

fn execute_sp_tables(exec: &mut ScriptExecutor<'_>) -> Result<QueryResult, DbError> {
    let rows: Vec<Vec<Value>> = exec
        .catalog
        .get_tables()
        .iter()
        .map(|t| {
            vec![
                Value::VarChar("iridium_sql".into()),
                Value::VarChar("dbo".into()),
                Value::VarChar(t.name.clone()),
                Value::VarChar("TABLE".into()),
            ]
        })
        .collect();

    Ok(QueryResult {
        columns: vec![
            "TABLE_QUALIFIER".into(),
            "TABLE_OWNER".into(),
            "TABLE_NAME".into(),
            "TABLE_TYPE".into(),
        ],
        column_types: vec![
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 128 },
        ],
        column_nullabilities: vec![false, false, false, false],
        rows,
        ..Default::default()
    })
}
