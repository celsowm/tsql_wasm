use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

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

fn resolve_information_schema_table(
    name: &str,
    catalog: &dyn Catalog,
) -> Option<(TableDef, Vec<StoredRow>)> {
    if name.eq_ignore_ascii_case("TABLES") {
        let table = virtual_table_def(
            "TABLES",
            vec![
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_TYPE", DataType::VarChar { max_len: 20 }, false),
            ],
        );
        let rows = catalog
            .get_tables()
            .iter()
            .map(|t| StoredRow {
                values: vec![
                    Value::VarChar(schema_name_by_id(catalog, t.schema_id)),
                    Value::VarChar(t.name.clone()),
                    Value::VarChar("BASE TABLE".to_string()),
                ],
                deleted: false,
            })
            .collect();
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("COLUMNS") {
        let table = virtual_table_def(
            "COLUMNS",
            vec![
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
                ("ORDINAL_POSITION", DataType::Int, false),
                ("IS_NULLABLE", DataType::VarChar { max_len: 3 }, false),
                ("DATA_TYPE", DataType::VarChar { max_len: 128 }, false),
            ],
        );
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            for (ordinal, c) in t.columns.iter().enumerate() {
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(schema_name_by_id(catalog, t.schema_id)),
                        Value::VarChar(t.name.clone()),
                        Value::VarChar(c.name.clone()),
                        Value::Int((ordinal + 1) as i32),
                        Value::VarChar(if c.nullable { "YES" } else { "NO" }.to_string()),
                        Value::VarChar(type_name(&c.data_type)),
                    ],
                    deleted: false,
                });
            }
        }
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("ROUTINES") {
        let table = virtual_table_def(
            "ROUTINES",
            vec![
                ("ROUTINE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("ROUTINE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("ROUTINE_TYPE", DataType::VarChar { max_len: 16 }, false),
            ],
        );
        let rows = catalog
            .get_routines()
            .iter()
            .map(|r| {
                let kind = match &r.kind {
                    crate::catalog::RoutineKind::Procedure { .. } => "PROCEDURE",
                    crate::catalog::RoutineKind::Function { .. } => "FUNCTION",
                };
                StoredRow {
                    values: vec![
                        Value::VarChar(r.schema.clone()),
                        Value::VarChar(r.name.clone()),
                        Value::VarChar(kind.to_string()),
                    ],
                    deleted: false,
                }
            })
            .collect();
        return Some((table, rows));
    }

    if name.eq_ignore_ascii_case("TABLE_CONSTRAINTS") {
        let table = virtual_table_def(
            "TABLE_CONSTRAINTS",
            vec![
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
                ("CONSTRAINT_TYPE", DataType::VarChar { max_len: 16 }, false),
            ],
        );
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            for chk in &t.check_constraints {
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(schema.clone()),
                        Value::VarChar(t.name.clone()),
                        Value::VarChar(chk.name.clone()),
                        Value::VarChar("CHECK".to_string()),
                    ],
                    deleted: false,
                });
            }
            for col in &t.columns {
                if let Some(name) = &col.default_constraint_name {
                    rows.push(StoredRow {
                        values: vec![
                            Value::VarChar(schema.clone()),
                            Value::VarChar(t.name.clone()),
                            Value::VarChar(name.clone()),
                            Value::VarChar("DEFAULT".to_string()),
                        ],
                        deleted: false,
                    });
                }
            }
        }
        return Some((table, rows));
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
        check_constraints: vec![],
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
        DataType::Decimal { .. } => 106,
        DataType::Char { .. } => 175,
        DataType::VarChar { .. } => 167,
        DataType::NChar { .. } => 239,
        DataType::NVarChar { .. } => 231,
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
        DataType::Decimal { .. } => "decimal".to_string(),
        DataType::Char { .. } => "char".to_string(),
        DataType::VarChar { .. } => "varchar".to_string(),
        DataType::NChar { .. } => "nchar".to_string(),
        DataType::NVarChar { .. } => "nvarchar".to_string(),
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
        DataType::Bit => 1,
        DataType::TinyInt => 1,
        DataType::SmallInt => 2,
        DataType::Int => 4,
        DataType::BigInt => 8,
        DataType::Decimal { .. } => 17,
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
        (106, "decimal", 17, 38, 18),
        (175, "char", 8000, 0, 0),
        (167, "varchar", 8000, 0, 0),
        (239, "nchar", 4000, 0, 0),
        (231, "nvarchar", 4000, 0, 0),
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
