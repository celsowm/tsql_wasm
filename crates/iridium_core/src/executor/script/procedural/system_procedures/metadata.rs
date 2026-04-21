use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use crate::executor::script::ScriptExecutor;
use crate::types::{DataType, Value};
use crate::executor::metadata::{type_name, type_max_length};

pub(crate) fn execute_sp_help(exec: &mut ScriptExecutor<'_>, args: &[String]) -> Result<QueryResult, DbError> {
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

pub(crate) fn execute_sp_helptext(
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

pub(crate) fn execute_sp_columns(
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

pub(crate) fn execute_sp_helpdb(
    _exec: &mut ScriptExecutor<'_>,
    args: &[String],
) -> Result<QueryResult, DbError> {
    let mut rows = Vec::new();
    let filter_name = args.first();

    for db in crate::executor::database_catalog::builtin_databases() {
        if let Some(name) = filter_name {
            if !db.name.eq_ignore_ascii_case(name) {
                continue;
            }
        }
        rows.push(vec![
            Value::NVarChar(db.name.to_string()),
            Value::NVarChar("0 MB".into()), // db_size
            Value::NVarChar("sa".into()),   // owner
            Value::Int(db.id),             // dbid
            Value::NVarChar("2025-01-01 00:00:00".into()), // created
            Value::NVarChar("Status=ONLINE, Updateability=READ_WRITE, UserAccess=MULTI_USER, Recovery=FULL, Version=904, Collation=SQL_Latin1_General_CP1_CI_AS, SQLSortOrder=52, IsAutoCreateStatistics, IsAutoUpdateStatistics".into()), // status
            Value::TinyInt(db.compatibility_level), // compatibility_level
        ]);
    }
    Ok(QueryResult {
        columns: vec![
            "name".into(),
            "db_size".into(),
            "owner".into(),
            "dbid".into(),
            "created".into(),
            "status".into(),
            "compatibility_level".into(),
        ],
        column_types: vec![
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 13 },
            DataType::NVarChar { max_len: 128 },
            DataType::Int,
            DataType::NVarChar { max_len: 18 },
            DataType::NVarChar { max_len: 600 },
            DataType::TinyInt,
        ],
        column_nullabilities: vec![false, true, true, false, false, true, false],
        rows,
        ..Default::default()
    })
}

pub(crate) fn execute_sp_helpindex(
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
    let table_indexes: Vec<_> = indexes
        .iter()
        .filter(|idx| idx.table_id == table.id)
        .collect();

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

pub(crate) fn execute_sp_helpconstraint(
    exec: &mut ScriptExecutor<'_>,
    args: &[String],
) -> Result<QueryResult, DbError> {
    if args.is_empty() {
        return Err(DbError::Execution(
            "sp_helpconstraint requires 1 argument: @objname".into(),
        ));
    }
    let objname = &args[0];
    let parts: Vec<&str> = objname.splitn(2, '.').collect();
    let (schema, table_name) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("dbo", parts[0])
    };

    let table = exec
        .catalog
        .find_table(schema, table_name)
        .ok_or_else(|| DbError::object_not_found(format!("table '{}'", table_name)))?;

    let mut rows = Vec::new();

    // Check constraints
    for ck in &table.check_constraints {
        rows.push(vec![
            Value::NVarChar("Check".into()),
            Value::NVarChar(ck.name.clone()),
            Value::NVarChar(format!("CHECK {}", crate::executor::tooling::formatting::format_expr(&ck.expr))),
        ]);
    }

    // Foreign keys
    for fk in &table.foreign_keys {
        rows.push(vec![
            Value::NVarChar("Foreign Key".into()),
            Value::NVarChar(fk.name.clone()),
            Value::NVarChar(format!(
                "REFERENCES {}.{} ({})",
                fk.referenced_table.schema.as_deref().unwrap_or("dbo"),
                fk.referenced_table.name,
                fk.referenced_columns.join(", ")
            )),
        ]);
    }

    Ok(QueryResult {
        columns: vec![
            "constraint_type".into(),
            "constraint_name".into(),
            "constraint_keys".into(),
        ],
        column_types: vec![
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 128 },
            DataType::NVarChar { max_len: 2048 },
        ],
        column_nullabilities: vec![false, false, false],
        rows,
        ..Default::default()
    })
}

pub(crate) fn execute_sp_tables(exec: &mut ScriptExecutor<'_>) -> Result<QueryResult, DbError> {
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

pub(crate) fn execute_sp_helpfile(
    exec: &mut ScriptExecutor<'_>,
    ctx: &mut ExecutionContext<'_>,
) -> Result<QueryResult, DbError> {
    let sql = "SELECT name, file_id, physical_name, type_desc AS usage, size FROM sys.database_files";
    let batch = crate::parser::parse_batch(sql)?;
    match exec.execute_batch(&batch, ctx)? {
        crate::error::StmtOutcome::Ok(Some(res)) => Ok(res),
        _ => Err(DbError::Execution("Failed to execute sp_helpfile query".into())),
    }
}

pub(crate) fn execute_sp_helpfilegroup(
    exec: &mut ScriptExecutor<'_>,
    ctx: &mut ExecutionContext<'_>,
) -> Result<QueryResult, DbError> {
    let sql = "SELECT name, data_space_id AS groupid, type_desc AS groupname FROM sys.filegroups";
    let batch = crate::parser::parse_batch(sql)?;
    match exec.execute_batch(&batch, ctx)? {
        crate::error::StmtOutcome::Ok(Some(res)) => Ok(res),
        _ => Err(DbError::Execution("Failed to execute sp_helpfilegroup query".into())),
    }
}

pub(crate) fn execute_sp_rename(
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
    let objtype = args.get(2).map(|s| s.as_str()).unwrap_or("OBJECT");

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

