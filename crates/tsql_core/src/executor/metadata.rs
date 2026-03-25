use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::storage::StoredRow;
use crate::types::{DataType, Value};
use super::type_mapping::data_type_spec_to_runtime;

pub(crate) fn resolve_virtual_table(
    schema: &str,
    name: &str,
    catalog: &dyn Catalog,
) -> Option<(TableDef, Vec<StoredRow>)> {
    if schema.eq_ignore_ascii_case("sys") {
        return resolve_sys_table(name, catalog);
    }
    if schema.eq_ignore_ascii_case("INFORMATION_SCHEMA") {
        return resolve_information_schema_table(name, catalog);
    }
    None
}

fn resolve_sys_table(name: &str, catalog: &dyn Catalog) -> Option<(TableDef, Vec<StoredRow>)> {
    if name.eq_ignore_ascii_case("schemas") {
        let table = virtual_table_def(
            "schemas",
            vec![
                ("schema_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
            ],
        );
        let rows = catalog
            .get_schemas()
            .iter()
            .map(|s| StoredRow {
                values: vec![Value::Int(s.id as i32), Value::VarChar(s.name.clone())],
                deleted: false,
            })
            .collect();
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("tables") {
        let table = virtual_table_def(
            "tables",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("schema_id", DataType::Int, false),
            ],
        );
        let rows = catalog
            .get_tables()
            .iter()
            .map(|t| StoredRow {
                values: vec![
                    Value::Int(t.id as i32),
                    Value::VarChar(t.name.clone()),
                    Value::Int(t.schema_id as i32),
                ],
                deleted: false,
            })
            .collect();
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("columns") {
        let table = virtual_table_def(
            "columns",
            vec![
                ("object_id", DataType::Int, false),
                ("column_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("user_type_id", DataType::Int, false),
                ("max_length", DataType::SmallInt, false),
                ("is_nullable", DataType::Bit, false),
            ],
        );
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            for c in &t.columns {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(t.id as i32),
                        Value::Int(c.id as i32),
                        Value::VarChar(c.name.clone()),
                        Value::Int(system_type_id(&c.data_type)),
                        Value::SmallInt(type_max_length(&c.data_type)),
                        Value::Bit(c.nullable),
                    ],
                    deleted: false,
                });
            }
        }
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("types") {
        let table = virtual_table_def(
            "types",
            vec![
                ("user_type_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("max_length", DataType::SmallInt, false),
                ("precision", DataType::TinyInt, false),
                ("scale", DataType::TinyInt, false),
            ],
        );
        let rows = builtin_types_rows();
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("indexes") {
        let table = virtual_table_def(
            "indexes",
            vec![
                ("object_id", DataType::Int, false),
                ("index_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::TinyInt, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_unique", DataType::Bit, false),
            ],
        );
        let rows = catalog
            .get_indexes()
            .iter()
            .map(|idx| StoredRow {
                values: vec![
                    Value::Int(idx.table_id as i32),
                    Value::Int(idx.id as i32),
                    Value::VarChar(idx.name.clone()),
                    Value::TinyInt(if idx.is_clustered { 1 } else { 2 }),
                    Value::VarChar(if idx.is_clustered {
                        "CLUSTERED".to_string()
                    } else {
                        "NONCLUSTERED".to_string()
                    }),
                    Value::Bit(idx.is_unique),
                ],
                deleted: false,
            })
            .collect();
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("objects") {
        let table = virtual_table_def(
            "objects",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("schema_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
            ],
        );
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(t.id as i32),
                    Value::VarChar(t.name.clone()),
                    Value::Int(t.schema_id as i32),
                    Value::Char("U ".to_string()),
                    Value::VarChar("USER_TABLE".to_string()),
                ],
                deleted: false,
            });
        }
        for idx in catalog.get_indexes() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(idx.id as i32),
                    Value::VarChar(idx.name.clone()),
                    Value::Int(idx.schema_id as i32),
                    Value::Char("IX".to_string()),
                    Value::VarChar("SQL_INDEX".to_string()),
                ],
                deleted: false,
            });
        }
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("check_constraints") {
        let table = virtual_table_def(
            "check_constraints",
            vec![
                ("object_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
            ],
        );
        let mut rows = Vec::new();
        let mut object_id = 1_000_000i32;
        for t in catalog.get_tables() {
            for chk in &t.check_constraints {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(object_id),
                        Value::Int(t.id as i32),
                        Value::VarChar(chk.name.clone()),
                    ],
                    deleted: false,
                });
                object_id += 1;
            }
        }
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("routines") {
        let table = virtual_table_def(
            "routines",
            vec![
                ("object_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
            ],
        );
        let mut rows = Vec::new();
        for r in catalog.get_routines() {
            let schema_id = catalog.get_schema_id(&r.schema).unwrap_or(1);
            let object_id = catalog.object_id(&r.schema, &r.name).unwrap_or(-1);
            let (ty, desc) = match &r.kind {
                crate::catalog::RoutineKind::Procedure { .. } => {
                    ("P ".to_string(), "SQL_STORED_PROCEDURE".to_string())
                }
                crate::catalog::RoutineKind::Function { .. } => {
                    ("FN".to_string(), "SQL_SCALAR_FUNCTION".to_string())
                }
            };
            rows.push(StoredRow {
                values: vec![
                    Value::Int(object_id),
                    Value::Int(schema_id as i32),
                    Value::VarChar(r.name.clone()),
                    Value::Char(ty),
                    Value::VarChar(desc),
                ],
                deleted: false,
            });
        }
        return Some((table, rows));
    }

    None
}

const DB_CATALOG: &str = "tsql_wasm";

fn char_max_length(dt: &DataType) -> Value {
    match dt {
        DataType::Char { len } | DataType::NChar { len } => Value::Int(*len as i32),
        DataType::VarChar { max_len } | DataType::NVarChar { max_len } => Value::Int(*max_len as i32),
        DataType::Binary { len } => Value::Int(*len as i32),
        DataType::VarBinary { max_len } => Value::Int(*max_len as i32),
        _ => Value::Null,
    }
}

fn char_octet_length(dt: &DataType) -> Value {
    match dt {
        DataType::Char { len } | DataType::VarChar { max_len: len } => Value::Int(*len as i32),
        DataType::NChar { len } | DataType::NVarChar { max_len: len } => Value::Int(*len as i32 * 2),
        DataType::Binary { len } => Value::Int(*len as i32),
        DataType::VarBinary { max_len } => Value::Int(*max_len as i32),
        _ => Value::Null,
    }
}

fn numeric_precision(dt: &DataType) -> Value {
    match dt {
        DataType::Bit => Value::TinyInt(1),
        DataType::TinyInt => Value::TinyInt(3),
        DataType::SmallInt => Value::TinyInt(5),
        DataType::Int => Value::TinyInt(10),
        DataType::BigInt => Value::TinyInt(19),
        DataType::Float => Value::TinyInt(53),
        DataType::Decimal { precision, .. } => Value::TinyInt(*precision),
        DataType::Money => Value::TinyInt(19),
        DataType::SmallMoney => Value::TinyInt(10),
        _ => Value::Null,
    }
}

fn numeric_precision_radix(dt: &DataType) -> Value {
    match dt {
        DataType::Float => Value::SmallInt(2),
        DataType::Bit | DataType::TinyInt | DataType::SmallInt | DataType::Int
        | DataType::BigInt | DataType::Decimal { .. } | DataType::Money | DataType::SmallMoney => {
            Value::SmallInt(10)
        }
        _ => Value::Null,
    }
}

fn numeric_scale_val(dt: &DataType) -> Value {
    match dt {
        DataType::Bit | DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => {
            Value::Int(0)
        }
        DataType::Decimal { scale, .. } => Value::Int(*scale as i32),
        DataType::Money | DataType::SmallMoney => Value::Int(4),
        _ => Value::Null,
    }
}

fn datetime_precision_val(dt: &DataType) -> Value {
    match dt {
        DataType::Date => Value::SmallInt(0),
        DataType::DateTime => Value::SmallInt(3),
        DataType::DateTime2 => Value::SmallInt(7),
        DataType::Time => Value::SmallInt(7),
        _ => Value::Null,
    }
}

fn charset_name(dt: &DataType) -> Value {
    match dt {
        DataType::Char { .. } | DataType::VarChar { .. } => Value::VarChar("iso_1".to_string()),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Value::VarChar("UNICODE".to_string()),
        _ => Value::Null,
    }
}

fn collation_name_val(dt: &DataType) -> Value {
    match dt {
        DataType::Char { .. } | DataType::VarChar { .. } | DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Value::VarChar("SQL_Latin1_General_CP1_CI_AS".to_string())
        }
        _ => Value::Null,
    }
}

fn resolve_information_schema_table(
    name: &str,
    catalog: &dyn Catalog,
) -> Option<(TableDef, Vec<StoredRow>)> {
    // ── SCHEMATA ──────────────────────────────────────────────────────
    if name.eq_ignore_ascii_case("SCHEMATA") {
        let table = virtual_table_def("SCHEMATA", vec![
            ("CATALOG_NAME", DataType::VarChar { max_len: 128 }, false),
            ("SCHEMA_NAME", DataType::VarChar { max_len: 128 }, false),
            ("SCHEMA_OWNER", DataType::VarChar { max_len: 128 }, true),
            ("DEFAULT_CHARACTER_SET_CATALOG", DataType::VarChar { max_len: 6 }, true),
            ("DEFAULT_CHARACTER_SET_SCHEMA", DataType::VarChar { max_len: 3 }, true),
            ("DEFAULT_CHARACTER_SET_NAME", DataType::VarChar { max_len: 128 }, true),
        ]);
        let rows = catalog.get_schemas().iter().map(|s| StoredRow {
            values: vec![
                Value::VarChar(DB_CATALOG.to_string()),
                Value::VarChar(s.name.clone()),
                Value::VarChar("dbo".to_string()),
                Value::Null,
                Value::Null,
                Value::VarChar("iso_1".to_string()),
            ],
            deleted: false,
        }).collect();
        return Some((table, rows));
    }

    // ── TABLES ────────────────────────────────────────────────────────
    if name.eq_ignore_ascii_case("TABLES") {
        let table = virtual_table_def("TABLES", vec![
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_TYPE", DataType::VarChar { max_len: 10 }, false),
        ]);
        let mut rows: Vec<StoredRow> = catalog.get_tables().iter().map(|t| StoredRow {
            values: vec![
                Value::VarChar(DB_CATALOG.to_string()),
                Value::VarChar(schema_name_by_id(catalog, t.schema_id)),
                Value::VarChar(t.name.clone()),
                Value::VarChar("BASE TABLE".to_string()),
            ],
            deleted: false,
        }).collect();
        for v in catalog.get_views() {
            rows.push(StoredRow {
                values: vec![
                    Value::VarChar(DB_CATALOG.to_string()),
                    Value::VarChar(v.schema.clone()),
                    Value::VarChar(v.name.clone()),
                    Value::VarChar("VIEW".to_string()),
                ],
                deleted: false,
            });
        }
        return Some((table, rows));
    }

    // ── COLUMNS ───────────────────────────────────────────────────────
    if name.eq_ignore_ascii_case("COLUMNS") {
        let table = virtual_table_def("COLUMNS", vec![
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
            ("ORDINAL_POSITION", DataType::Int, false),
            ("COLUMN_DEFAULT", DataType::VarChar { max_len: 128 }, true),
            ("IS_NULLABLE", DataType::VarChar { max_len: 3 }, false),
            ("DATA_TYPE", DataType::VarChar { max_len: 128 }, false),
            ("CHARACTER_MAXIMUM_LENGTH", DataType::Int, true),
            ("CHARACTER_OCTET_LENGTH", DataType::Int, true),
            ("NUMERIC_PRECISION", DataType::TinyInt, true),
            ("NUMERIC_PRECISION_RADIX", DataType::SmallInt, true),
            ("NUMERIC_SCALE", DataType::Int, true),
            ("DATETIME_PRECISION", DataType::SmallInt, true),
            ("CHARACTER_SET_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_SET_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_SET_NAME", DataType::VarChar { max_len: 128 }, true),
            ("COLLATION_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("COLLATION_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("COLLATION_NAME", DataType::VarChar { max_len: 128 }, true),
            ("DOMAIN_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("DOMAIN_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("DOMAIN_NAME", DataType::VarChar { max_len: 128 }, true),
        ]);
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            for (ordinal, c) in t.columns.iter().enumerate() {
                let col_default = c.default.as_ref().map(|e| Value::VarChar(format!("{:?}", e))).unwrap_or(Value::Null);
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema.clone()),
                        Value::VarChar(t.name.clone()),
                        Value::VarChar(c.name.clone()),
                        Value::Int((ordinal + 1) as i32),
                        col_default,
                        Value::VarChar(if c.nullable { "YES" } else { "NO" }.to_string()),
                        Value::VarChar(type_name(&c.data_type)),
                        char_max_length(&c.data_type),
                        char_octet_length(&c.data_type),
                        numeric_precision(&c.data_type),
                        numeric_precision_radix(&c.data_type),
                        numeric_scale_val(&c.data_type),
                        datetime_precision_val(&c.data_type),
                        Value::Null, // CHARACTER_SET_CATALOG
                        Value::Null, // CHARACTER_SET_SCHEMA
                        charset_name(&c.data_type),
                        Value::Null, // COLLATION_CATALOG
                        Value::Null, // COLLATION_SCHEMA
                        collation_name_val(&c.data_type),
                        Value::Null, // DOMAIN_CATALOG
                        Value::Null, // DOMAIN_SCHEMA
                        Value::Null, // DOMAIN_NAME
                    ],
                    deleted: false,
                });
            }
        }
        return Some((table, rows));
    }

    // ── VIEWS ─────────────────────────────────────────────────────────
    if name.eq_ignore_ascii_case("VIEWS") {
        let table = virtual_table_def("VIEWS", vec![
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("VIEW_DEFINITION", DataType::VarChar { max_len: 128 }, true),
            ("CHECK_OPTION", DataType::VarChar { max_len: 7 }, false),
            ("IS_UPDATABLE", DataType::VarChar { max_len: 2 }, false),
        ]);
        let rows = catalog.get_views().iter().map(|v| StoredRow {
            values: vec![
                Value::VarChar(DB_CATALOG.to_string()),
                Value::VarChar(v.schema.clone()),
                Value::VarChar(v.name.clone()),
                Value::Null,
                Value::VarChar("NONE".to_string()),
                Value::VarChar("NO".to_string()),
            ],
            deleted: false,
        }).collect();
        return Some((table, rows));
    }

    // ── ROUTINES ──────────────────────────────────────────────────────
    if name.eq_ignore_ascii_case("ROUTINES") {
        let table = virtual_table_def("ROUTINES", vec![
            ("SPECIFIC_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("SPECIFIC_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("SPECIFIC_NAME", DataType::VarChar { max_len: 128 }, false),
            ("ROUTINE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("ROUTINE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("ROUTINE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("ROUTINE_TYPE", DataType::VarChar { max_len: 20 }, false),
            ("MODULE_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("MODULE_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("MODULE_NAME", DataType::VarChar { max_len: 128 }, true),
            ("UDT_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("UDT_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("UDT_NAME", DataType::VarChar { max_len: 128 }, true),
            ("DATA_TYPE", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_MAXIMUM_LENGTH", DataType::Int, true),
            ("CHARACTER_OCTET_LENGTH", DataType::Int, true),
            ("COLLATION_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("COLLATION_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("COLLATION_NAME", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_SET_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_SET_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_SET_NAME", DataType::VarChar { max_len: 128 }, true),
            ("NUMERIC_PRECISION", DataType::SmallInt, true),
            ("NUMERIC_PRECISION_RADIX", DataType::SmallInt, true),
            ("NUMERIC_SCALE", DataType::SmallInt, true),
            ("DATETIME_PRECISION", DataType::SmallInt, true),
            ("ROUTINE_BODY", DataType::VarChar { max_len: 30 }, false),
            ("ROUTINE_DEFINITION", DataType::VarChar { max_len: 128 }, true),
            ("EXTERNAL_NAME", DataType::VarChar { max_len: 128 }, true),
            ("EXTERNAL_LANGUAGE", DataType::VarChar { max_len: 30 }, true),
            ("PARAMETER_STYLE", DataType::VarChar { max_len: 30 }, true),
            ("IS_DETERMINISTIC", DataType::VarChar { max_len: 10 }, false),
            ("SQL_DATA_ACCESS", DataType::VarChar { max_len: 30 }, false),
            ("IS_NULL_CALL", DataType::VarChar { max_len: 10 }, true),
            ("SQL_PATH", DataType::VarChar { max_len: 128 }, true),
            ("SCHEMA_LEVEL_ROUTINE", DataType::VarChar { max_len: 10 }, false),
            ("MAX_DYNAMIC_RESULT_SETS", DataType::SmallInt, false),
            ("IS_USER_DEFINED_CAST", DataType::VarChar { max_len: 10 }, false),
            ("IS_IMPLICITLY_INVOCABLE", DataType::VarChar { max_len: 10 }, false),
            ("CREATED", DataType::VarChar { max_len: 30 }, true),
            ("LAST_ALTERED", DataType::VarChar { max_len: 30 }, true),
        ]);
        let rows = catalog.get_routines().iter().map(|r| {
            let (routine_type, sql_access) = match &r.kind {
                crate::catalog::RoutineKind::Procedure { .. } => ("PROCEDURE", "MODIFIES"),
                crate::catalog::RoutineKind::Function { .. } => ("FUNCTION", "READS"),
            };
            let (ret_type, ret_char_max, ret_char_oct) = match &r.kind {
                crate::catalog::RoutineKind::Function { returns: Some(dt_spec), .. } => {
                    let dt = data_type_spec_to_runtime(dt_spec);
                    (Value::VarChar(type_name(&dt)), char_max_length(&dt), char_octet_length(&dt))
                }
                _ => (Value::Null, Value::Null, Value::Null),
            };
            StoredRow {
                values: vec![
                    Value::VarChar(DB_CATALOG.to_string()), // SPECIFIC_CATALOG
                    Value::VarChar(r.schema.clone()),        // SPECIFIC_SCHEMA
                    Value::VarChar(r.name.clone()),          // SPECIFIC_NAME
                    Value::VarChar(DB_CATALOG.to_string()), // ROUTINE_CATALOG
                    Value::VarChar(r.schema.clone()),        // ROUTINE_SCHEMA
                    Value::VarChar(r.name.clone()),          // ROUTINE_NAME
                    Value::VarChar(routine_type.to_string()), // ROUTINE_TYPE
                    Value::Null, Value::Null, Value::Null,   // MODULE_*
                    Value::Null, Value::Null, Value::Null,   // UDT_*
                    ret_type,                                 // DATA_TYPE
                    ret_char_max,                             // CHARACTER_MAXIMUM_LENGTH
                    ret_char_oct,                             // CHARACTER_OCTET_LENGTH
                    Value::Null, Value::Null, Value::Null,   // COLLATION_*
                    Value::Null, Value::Null, Value::Null,   // CHARACTER_SET_*
                    Value::Null, Value::Null, Value::Null, Value::Null, // NUMERIC_*, DATETIME_*
                    Value::VarChar("SQL".to_string()),        // ROUTINE_BODY
                    Value::Null,                              // ROUTINE_DEFINITION
                    Value::Null,                              // EXTERNAL_NAME
                    Value::Null,                              // EXTERNAL_LANGUAGE
                    Value::Null,                              // PARAMETER_STYLE
                    Value::VarChar("NO".to_string()),         // IS_DETERMINISTIC
                    Value::VarChar(sql_access.to_string()),   // SQL_DATA_ACCESS
                    Value::Null,                              // IS_NULL_CALL
                    Value::Null,                              // SQL_PATH
                    Value::VarChar("YES".to_string()),        // SCHEMA_LEVEL_ROUTINE
                    Value::SmallInt(0),                       // MAX_DYNAMIC_RESULT_SETS
                    Value::VarChar("NO".to_string()),         // IS_USER_DEFINED_CAST
                    Value::VarChar("NO".to_string()),         // IS_IMPLICITLY_INVOCABLE
                    Value::Null,                              // CREATED
                    Value::Null,                              // LAST_ALTERED
                ],
                deleted: false,
            }
        }).collect();
        return Some((table, rows));
    }

    // ── PARAMETERS ────────────────────────────────────────────────────
    if name.eq_ignore_ascii_case("PARAMETERS") {
        let table = virtual_table_def("PARAMETERS", vec![
            ("SPECIFIC_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("SPECIFIC_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("SPECIFIC_NAME", DataType::VarChar { max_len: 128 }, false),
            ("ORDINAL_POSITION", DataType::Int, false),
            ("PARAMETER_MODE", DataType::VarChar { max_len: 10 }, false),
            ("IS_RESULT", DataType::VarChar { max_len: 10 }, false),
            ("AS_LOCATOR", DataType::VarChar { max_len: 10 }, false),
            ("PARAMETER_NAME", DataType::VarChar { max_len: 128 }, true),
            ("DATA_TYPE", DataType::VarChar { max_len: 128 }, false),
            ("CHARACTER_MAXIMUM_LENGTH", DataType::Int, true),
            ("CHARACTER_OCTET_LENGTH", DataType::Int, true),
            ("COLLATION_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("COLLATION_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("COLLATION_NAME", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_SET_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_SET_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("CHARACTER_SET_NAME", DataType::VarChar { max_len: 128 }, true),
            ("NUMERIC_PRECISION", DataType::TinyInt, true),
            ("NUMERIC_PRECISION_RADIX", DataType::SmallInt, true),
            ("NUMERIC_SCALE", DataType::TinyInt, true),
            ("DATETIME_PRECISION", DataType::SmallInt, true),
        ]);
        let mut rows = Vec::new();
        for r in catalog.get_routines() {
            for (i, p) in r.params.iter().enumerate() {
                let dt = data_type_spec_to_runtime(&p.data_type);
                let mode = if p.is_output { "INOUT" } else { "IN" };
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(r.schema.clone()),
                        Value::VarChar(r.name.clone()),
                        Value::Int((i + 1) as i32),
                        Value::VarChar(mode.to_string()),
                        Value::VarChar("NO".to_string()),
                        Value::VarChar("NO".to_string()),
                        Value::VarChar(p.name.clone()),
                        Value::VarChar(type_name(&dt)),
                        char_max_length(&dt),
                        char_octet_length(&dt),
                        Value::Null, Value::Null, // COLLATION_CATALOG, COLLATION_SCHEMA
                        collation_name_val(&dt),
                        Value::Null, Value::Null, // CHARACTER_SET_CATALOG, CHARACTER_SET_SCHEMA
                        charset_name(&dt),
                        numeric_precision(&dt),
                        numeric_precision_radix(&dt),
                        match numeric_scale_val(&dt) {
                            Value::Int(v) => Value::TinyInt(v as u8),
                            _ => Value::Null,
                        },
                        datetime_precision_val(&dt),
                    ],
                    deleted: false,
                });
            }
        }
        return Some((table, rows));
    }

    // ── TABLE_CONSTRAINTS ─────────────────────────────────────────────
    if name.eq_ignore_ascii_case("TABLE_CONSTRAINTS") {
        let table = virtual_table_def("TABLE_CONSTRAINTS", vec![
            ("CONSTRAINT_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_TYPE", DataType::VarChar { max_len: 16 }, false),
            ("IS_DEFERRABLE", DataType::VarChar { max_len: 2 }, false),
            ("INITIALLY_DEFERRED", DataType::VarChar { max_len: 2 }, false),
        ]);
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            let base = |cname: &str, ctype: &str| -> Vec<Value> {
                vec![
                    Value::VarChar(DB_CATALOG.to_string()),
                    Value::VarChar(schema.clone()),
                    Value::VarChar(cname.to_string()),
                    Value::VarChar(DB_CATALOG.to_string()),
                    Value::VarChar(schema.clone()),
                    Value::VarChar(t.name.clone()),
                    Value::VarChar(ctype.to_string()),
                    Value::VarChar("NO".to_string()),
                    Value::VarChar("NO".to_string()),
                ]
            };
            // PRIMARY KEY
            let pk_cols: Vec<&str> = t.columns.iter().filter(|c| c.primary_key).map(|c| c.name.as_str()).collect();
            if !pk_cols.is_empty() {
                let pk_name = format!("PK_{}", t.name);
                rows.push(StoredRow { values: base(&pk_name, "PRIMARY KEY"), deleted: false });
            }
            // UNIQUE (non-PK)
            for col in &t.columns {
                if col.unique && !col.primary_key {
                    let uq_name = format!("UQ_{}_{}", t.name, col.name);
                    rows.push(StoredRow { values: base(&uq_name, "UNIQUE"), deleted: false });
                }
            }
            // CHECK
            for chk in &t.check_constraints {
                rows.push(StoredRow { values: base(&chk.name, "CHECK"), deleted: false });
            }
            // FOREIGN KEY
            for fk in &t.foreign_keys {
                rows.push(StoredRow { values: base(&fk.name, "FOREIGN KEY"), deleted: false });
            }
            // DEFAULT
            for col in &t.columns {
                if let Some(cn) = &col.default_constraint_name {
                    rows.push(StoredRow { values: base(cn, "DEFAULT"), deleted: false });
                }
            }
        }
        return Some((table, rows));
    }

    // ── CHECK_CONSTRAINTS ─────────────────────────────────────────────
    if name.eq_ignore_ascii_case("CHECK_CONSTRAINTS") {
        let table = virtual_table_def("CHECK_CONSTRAINTS", vec![
            ("CONSTRAINT_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
            ("CHECK_CLAUSE", DataType::VarChar { max_len: 128 }, false),
        ]);
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            for chk in &t.check_constraints {
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema.clone()),
                        Value::VarChar(chk.name.clone()),
                        Value::VarChar(format!("{:?}", chk.expr)),
                    ],
                    deleted: false,
                });
            }
        }
        return Some((table, rows));
    }

    // ── REFERENTIAL_CONSTRAINTS ───────────────────────────────────────
    if name.eq_ignore_ascii_case("REFERENTIAL_CONSTRAINTS") {
        let table = virtual_table_def("REFERENTIAL_CONSTRAINTS", vec![
            ("CONSTRAINT_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
            ("UNIQUE_CONSTRAINT_CATALOG", DataType::VarChar { max_len: 128 }, true),
            ("UNIQUE_CONSTRAINT_SCHEMA", DataType::VarChar { max_len: 128 }, true),
            ("UNIQUE_CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, true),
            ("MATCH_OPTION", DataType::VarChar { max_len: 7 }, false),
            ("UPDATE_RULE", DataType::VarChar { max_len: 11 }, false),
            ("DELETE_RULE", DataType::VarChar { max_len: 11 }, false),
        ]);
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            for fk in &t.foreign_keys {
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema.clone()),
                        Value::VarChar(fk.name.clone()),
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(fk.referenced_table.schema_or_dbo().to_string()),
                        Value::Null,
                        Value::VarChar("SIMPLE".to_string()),
                        Value::VarChar("NO ACTION".to_string()),
                        Value::VarChar("NO ACTION".to_string()),
                    ],
                    deleted: false,
                });
            }
        }
        return Some((table, rows));
    }

    // ── KEY_COLUMN_USAGE ──────────────────────────────────────────────
    if name.eq_ignore_ascii_case("KEY_COLUMN_USAGE") {
        let table = virtual_table_def("KEY_COLUMN_USAGE", vec![
            ("CONSTRAINT_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
            ("ORDINAL_POSITION", DataType::Int, false),
        ]);
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            // PK columns
            let pk_cols: Vec<&ColumnDef> = t.columns.iter().filter(|c| c.primary_key).collect();
            if !pk_cols.is_empty() {
                let pk_name = format!("PK_{}", t.name);
                for (i, col) in pk_cols.iter().enumerate() {
                    rows.push(StoredRow {
                        values: vec![
                            Value::VarChar(DB_CATALOG.to_string()),
                            Value::VarChar(schema.clone()),
                            Value::VarChar(pk_name.clone()),
                            Value::VarChar(DB_CATALOG.to_string()),
                            Value::VarChar(schema.clone()),
                            Value::VarChar(t.name.clone()),
                            Value::VarChar(col.name.clone()),
                            Value::Int((i + 1) as i32),
                        ],
                        deleted: false,
                    });
                }
            }
            // FK columns
            for fk in &t.foreign_keys {
                for (i, col_name) in fk.columns.iter().enumerate() {
                    rows.push(StoredRow {
                        values: vec![
                            Value::VarChar(DB_CATALOG.to_string()),
                            Value::VarChar(schema.clone()),
                            Value::VarChar(fk.name.clone()),
                            Value::VarChar(DB_CATALOG.to_string()),
                            Value::VarChar(schema.clone()),
                            Value::VarChar(t.name.clone()),
                            Value::VarChar(col_name.clone()),
                            Value::Int((i + 1) as i32),
                        ],
                        deleted: false,
                    });
                }
            }
        }
        return Some((table, rows));
    }

    // ── CONSTRAINT_COLUMN_USAGE ───────────────────────────────────────
    if name.eq_ignore_ascii_case("CONSTRAINT_COLUMN_USAGE") {
        let table = virtual_table_def("CONSTRAINT_COLUMN_USAGE", vec![
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
        ]);
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            let row = |col_name: &str, cname: &str| -> StoredRow {
                StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema.clone()),
                        Value::VarChar(t.name.clone()),
                        Value::VarChar(col_name.to_string()),
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema.clone()),
                        Value::VarChar(cname.to_string()),
                    ],
                    deleted: false,
                }
            };
            // PK
            let pk_cols: Vec<&ColumnDef> = t.columns.iter().filter(|c| c.primary_key).collect();
            if !pk_cols.is_empty() {
                let pk_name = format!("PK_{}", t.name);
                for col in &pk_cols {
                    rows.push(row(&col.name, &pk_name));
                }
            }
            // Unique (non-PK)
            for col in &t.columns {
                if col.unique && !col.primary_key {
                    let uq_name = format!("UQ_{}_{}", t.name, col.name);
                    rows.push(row(&col.name, &uq_name));
                }
            }
            // FK
            for fk in &t.foreign_keys {
                for col_name in &fk.columns {
                    rows.push(row(col_name, &fk.name));
                }
            }
            // Check column-level
            for col in &t.columns {
                if let Some(cn) = &col.check_constraint_name {
                    rows.push(row(&col.name, cn));
                }
            }
        }
        return Some((table, rows));
    }

    // ── CONSTRAINT_TABLE_USAGE ────────────────────────────────────────
    if name.eq_ignore_ascii_case("CONSTRAINT_TABLE_USAGE") {
        let table = virtual_table_def("CONSTRAINT_TABLE_USAGE", vec![
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
        ]);
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            let row = |cname: &str| -> StoredRow {
                StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema.clone()),
                        Value::VarChar(t.name.clone()),
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema.clone()),
                        Value::VarChar(cname.to_string()),
                    ],
                    deleted: false,
                }
            };
            let pk_cols: Vec<&ColumnDef> = t.columns.iter().filter(|c| c.primary_key).collect();
            if !pk_cols.is_empty() {
                rows.push(row(&format!("PK_{}", t.name)));
            }
            for col in &t.columns {
                if col.unique && !col.primary_key {
                    rows.push(row(&format!("UQ_{}_{}", t.name, col.name)));
                }
            }
            for chk in &t.check_constraints {
                rows.push(row(&chk.name));
            }
            for fk in &t.foreign_keys {
                rows.push(row(&fk.name));
            }
            for col in &t.columns {
                if let Some(cn) = &col.default_constraint_name {
                    rows.push(row(cn));
                }
            }
        }
        return Some((table, rows));
    }

    // ── COLUMN_DOMAIN_USAGE (empty – no user-defined types) ───────────
    if name.eq_ignore_ascii_case("COLUMN_DOMAIN_USAGE") {
        let table = virtual_table_def("COLUMN_DOMAIN_USAGE", vec![
            ("DOMAIN_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("DOMAIN_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("DOMAIN_NAME", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
        ]);
        return Some((table, vec![]));
    }

    // ── DOMAINS (empty – no user-defined types) ───────────────────────
    if name.eq_ignore_ascii_case("DOMAINS") {
        let table = virtual_table_def("DOMAINS", vec![
            ("DOMAIN_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("DOMAIN_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("DOMAIN_NAME", DataType::VarChar { max_len: 128 }, false),
            ("DATA_TYPE", DataType::VarChar { max_len: 128 }, false),
            ("CHARACTER_MAXIMUM_LENGTH", DataType::Int, true),
            ("NUMERIC_PRECISION", DataType::TinyInt, true),
            ("NUMERIC_SCALE", DataType::Int, true),
            ("DOMAIN_DEFAULT", DataType::VarChar { max_len: 128 }, true),
        ]);
        return Some((table, vec![]));
    }

    // ── DOMAIN_CONSTRAINTS (empty) ────────────────────────────────────
    if name.eq_ignore_ascii_case("DOMAIN_CONSTRAINTS") {
        let table = virtual_table_def("DOMAIN_CONSTRAINTS", vec![
            ("CONSTRAINT_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
            ("DOMAIN_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("DOMAIN_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("DOMAIN_NAME", DataType::VarChar { max_len: 128 }, false),
            ("IS_DEFERRABLE", DataType::VarChar { max_len: 2 }, false),
            ("INITIALLY_DEFERRED", DataType::VarChar { max_len: 2 }, false),
        ]);
        return Some((table, vec![]));
    }

    // ── TABLE_PRIVILEGES (empty – no permission system) ───────────────
    if name.eq_ignore_ascii_case("TABLE_PRIVILEGES") {
        let table = virtual_table_def("TABLE_PRIVILEGES", vec![
            ("GRANTOR", DataType::VarChar { max_len: 128 }, false),
            ("GRANTEE", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("PRIVILEGE_TYPE", DataType::VarChar { max_len: 10 }, false),
            ("IS_GRANTABLE", DataType::VarChar { max_len: 3 }, false),
        ]);
        return Some((table, vec![]));
    }

    // ── COLUMN_PRIVILEGES (empty – no permission system) ──────────────
    if name.eq_ignore_ascii_case("COLUMN_PRIVILEGES") {
        let table = virtual_table_def("COLUMN_PRIVILEGES", vec![
            ("GRANTOR", DataType::VarChar { max_len: 128 }, false),
            ("GRANTEE", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
            ("PRIVILEGE_TYPE", DataType::VarChar { max_len: 10 }, false),
            ("IS_GRANTABLE", DataType::VarChar { max_len: 3 }, false),
        ]);
        return Some((table, vec![]));
    }

    // ── VIEW_COLUMN_USAGE (empty) ─────────────────────────────────────
    if name.eq_ignore_ascii_case("VIEW_COLUMN_USAGE") {
        let table = virtual_table_def("VIEW_COLUMN_USAGE", vec![
            ("VIEW_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("VIEW_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("VIEW_NAME", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
        ]);
        return Some((table, vec![]));
    }

    // ── VIEW_TABLE_USAGE (empty) ──────────────────────────────────────
    if name.eq_ignore_ascii_case("VIEW_TABLE_USAGE") {
        let table = virtual_table_def("VIEW_TABLE_USAGE", vec![
            ("VIEW_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("VIEW_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("VIEW_NAME", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
        ]);
        return Some((table, vec![]));
    }

    // ── ROUTINE_COLUMNS (empty – no table-valued functions) ───────────
    if name.eq_ignore_ascii_case("ROUTINE_COLUMNS") {
        let table = virtual_table_def("ROUTINE_COLUMNS", vec![
            ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
            ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
            ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
            ("ORDINAL_POSITION", DataType::Int, false),
            ("COLUMN_DEFAULT", DataType::VarChar { max_len: 128 }, true),
            ("IS_NULLABLE", DataType::VarChar { max_len: 3 }, false),
            ("DATA_TYPE", DataType::VarChar { max_len: 128 }, false),
            ("CHARACTER_MAXIMUM_LENGTH", DataType::Int, true),
        ]);
        return Some((table, vec![]));
    }

    None
}

fn virtual_table_def(name: &str, cols: Vec<(&str, DataType, bool)>) -> TableDef {
    TableDef {
        id: 0,
        schema_id: 0,
        name: name.to_string(),
        columns: cols
            .into_iter()
            .enumerate()
            .map(|(i, (name, ty, nullable))| ColumnDef {
                id: (i + 1) as u32,
                name: name.to_string(),
                data_type: ty,
                nullable,
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
        check_constraints: vec![], foreign_keys: vec![],

    }
}

fn schema_name_by_id(catalog: &dyn Catalog, id: u32) -> String {
    catalog
        .get_schemas()
        .iter()
        .find(|s| s.id == id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "dbo".to_string())
}

fn system_type_id(dt: &DataType) -> i32 {
    match dt {
        DataType::Bit => 104,
        DataType::TinyInt => 48,
        DataType::SmallInt => 52,
        DataType::Int => 56,
        DataType::BigInt => 127,
        DataType::Float => 62,
        DataType::Decimal { .. } => 106,
        DataType::Money => 60,
        DataType::SmallMoney => 59,
        DataType::Char { .. } => 175,
        DataType::VarChar { .. } => 167,
        DataType::NChar { .. } => 239,
        DataType::NVarChar { .. } => 231,
        DataType::Binary { .. } => 173,
        DataType::VarBinary { .. } => 165,
        DataType::Date => 40,
        DataType::Time => 41,
        DataType::DateTime => 61,
        DataType::DateTime2 => 42,
        DataType::UniqueIdentifier => 36,
        DataType::SqlVariant => 98,
    }
}

fn type_name(dt: &DataType) -> String {
    match dt {
        DataType::Bit => "bit".to_string(),
        DataType::TinyInt => "tinyint".to_string(),
        DataType::SmallInt => "smallint".to_string(),
        DataType::Int => "int".to_string(),
        DataType::BigInt => "bigint".to_string(),
        DataType::Float => "float".to_string(),
        DataType::Decimal { .. } => "decimal".to_string(),
        DataType::Money => "money".to_string(),
        DataType::SmallMoney => "smallmoney".to_string(),
        DataType::Char { .. } => "char".to_string(),
        DataType::VarChar { .. } => "varchar".to_string(),
        DataType::NChar { .. } => "nchar".to_string(),
        DataType::NVarChar { .. } => "nvarchar".to_string(),
        DataType::Binary { .. } => "binary".to_string(),
        DataType::VarBinary { .. } => "varbinary".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time => "time".to_string(),
        DataType::DateTime => "datetime".to_string(),
        DataType::DateTime2 => "datetime2".to_string(),
        DataType::UniqueIdentifier => "uniqueidentifier".to_string(),
        DataType::SqlVariant => "sql_variant".to_string(),
    }
}

fn type_max_length(dt: &DataType) -> i16 {
    match dt {
        DataType::Char { len } | DataType::NChar { len } => *len as i16,
        DataType::VarChar { max_len } | DataType::NVarChar { max_len } => *max_len as i16,
        DataType::Binary { len } => *len as i16,
        DataType::VarBinary { max_len } => *max_len as i16,
        DataType::Bit => 1,
        DataType::TinyInt => 1,
        DataType::SmallInt => 2,
        DataType::Int => 4,
        DataType::BigInt => 8,
        DataType::Float => 8,
        DataType::Decimal { .. } => 17,
        DataType::Money => 8,
        DataType::SmallMoney => 4,
        DataType::Date => 3,
        DataType::Time => 5,
        DataType::DateTime => 8,
        DataType::DateTime2 => 8,
        DataType::UniqueIdentifier => 16,
        DataType::SqlVariant => 8016,
    }
}

fn builtin_types_rows() -> Vec<StoredRow> {
    let types: Vec<(i32, &str, i16, u8, u8)> = vec![
        (104, "bit", 1, 1, 0),
        (48, "tinyint", 1, 3, 0),
        (52, "smallint", 2, 5, 0),
        (56, "int", 4, 10, 0),
        (127, "bigint", 8, 19, 0),
        (62, "float", 8, 53, 0),
        (106, "decimal", 17, 38, 18),
        (60, "money", 8, 19, 4),
        (59, "smallmoney", 4, 10, 4),
        (175, "char", 8000, 0, 0),
        (167, "varchar", 8000, 0, 0),
        (239, "nchar", 4000, 0, 0),
        (231, "nvarchar", 4000, 0, 0),
        (173, "binary", 8000, 0, 0),
        (165, "varbinary", 8000, 0, 0),
        (40, "date", 3, 10, 0),
        (41, "time", 5, 16, 7),
        (61, "datetime", 8, 23, 3),
        (42, "datetime2", 8, 27, 7),
        (36, "uniqueidentifier", 16, 0, 0),
        (98, "sql_variant", 8016, 0, 0),
    ];

    types
        .into_iter()
        .map(|(id, name, max_len, precision, scale)| StoredRow {
            values: vec![
                Value::Int(id),
                Value::VarChar(name.to_string()),
                Value::SmallInt(max_len),
                Value::TinyInt(precision),
                Value::TinyInt(scale),
            ],
            deleted: false,
        })
        .collect()
}
