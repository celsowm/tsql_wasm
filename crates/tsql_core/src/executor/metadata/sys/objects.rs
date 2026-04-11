use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysObjects;
pub(crate) struct SysSystemViews;
pub(crate) struct SysCompatSysObjects;

impl VirtualTable for SysObjects {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "objects",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("schema_id", DataType::Int, false),
                ("principal_id", DataType::Int, true),
                ("parent_object_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_ms_shipped", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        for t in catalog.get_tables() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(t.id as i32),
                    Value::VarChar(t.name.clone()),
                    Value::Int(t.schema_id as i32),
                    Value::Null,
                    Value::Int(0), // user table has no parent
                    Value::Char("U ".to_string()),
                    Value::VarChar("USER_TABLE".to_string()),
                    created.clone(),
                    created.clone(),
                    Value::Bit(false),
                ],
                deleted: false,
            });

            // Primary Keys and Uniques
            let mut pk_uq_id = 2_000_000i32;
            for col in &t.columns {
                if col.primary_key {
                    rows.push(StoredRow {
                        values: vec![
                            Value::Int(pk_uq_id),
                            Value::VarChar(format!("PK_{}", t.name)),
                            Value::Int(t.schema_id as i32),
                            Value::Null,
                            Value::Int(t.id as i32),
                            Value::Char("PK".to_string()),
                            Value::VarChar("PRIMARY_KEY_CONSTRAINT".to_string()),
                            created.clone(),
                            created.clone(),
                            Value::Bit(false),
                        ],
                        deleted: false,
                    });
                    pk_uq_id += 1;
                } else if col.unique {
                    rows.push(StoredRow {
                        values: vec![
                            Value::Int(pk_uq_id),
                            Value::VarChar(format!("UQ_{}_{}", t.name, col.name)),
                            Value::Int(t.schema_id as i32),
                            Value::Null,
                            Value::Int(t.id as i32),
                            Value::Char("UQ".to_string()),
                            Value::VarChar("UNIQUE_CONSTRAINT".to_string()),
                            created.clone(),
                            created.clone(),
                            Value::Bit(false),
                        ],
                        deleted: false,
                    });
                    pk_uq_id += 1;
                }

                if let Some(_default_expr) = &col.default {
                    let name = col
                        .default_constraint_name
                        .clone()
                        .unwrap_or_else(|| format!("DF_{}_{}", t.name, col.name));
                    rows.push(StoredRow {
                        values: vec![
                            Value::Int(3_000_000 + col.id as i32),
                            Value::VarChar(name),
                            Value::Int(t.schema_id as i32),
                            Value::Null,
                            Value::Int(t.id as i32),
                            Value::Char("D ".to_string()),
                            Value::VarChar("DEFAULT_CONSTRAINT".to_string()),
                            created.clone(),
                            created.clone(),
                            Value::Bit(false),
                        ],
                        deleted: false,
                    });
                }
            }

            // Check Constraints
            let mut chk_id = 1_000_000i32;
            for chk in &t.check_constraints {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(chk_id),
                        Value::VarChar(chk.name.clone()),
                        Value::Int(t.schema_id as i32),
                        Value::Null,
                        Value::Int(t.id as i32),
                        Value::Char("C ".to_string()),
                        Value::VarChar("CHECK_CONSTRAINT".to_string()),
                        created.clone(),
                        created.clone(),
                        Value::Bit(false),
                    ],
                    deleted: false,
                });
                chk_id += 1;
            }

            // Foreign Keys
            let mut fk_id = 4_000_000i32;
            for fk in &t.foreign_keys {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(fk_id),
                        Value::VarChar(fk.name.clone()),
                        Value::Int(t.schema_id as i32),
                        Value::Null,
                        Value::Int(t.id as i32),
                        Value::Char("F ".to_string()),
                        Value::VarChar("FOREIGN_KEY_CONSTRAINT".to_string()),
                        created.clone(),
                        created.clone(),
                        Value::Bit(false),
                    ],
                    deleted: false,
                });
                fk_id += 1;
            }
        }
        for routine in catalog.get_routines() {
            let (ty, desc) = match &routine.kind {
                crate::catalog::RoutineKind::Procedure { .. } => {
                    ("P ".to_string(), "SQL_STORED_PROCEDURE".to_string())
                }
                crate::catalog::RoutineKind::Function {
                    body: crate::ast::FunctionBody::InlineTable(_),
                    ..
                } => (
                    "IF".to_string(),
                    "SQL_INLINE_TABLE_VALUED_FUNCTION".to_string(),
                ),
                crate::catalog::RoutineKind::Function { .. } => {
                    ("FN".to_string(), "SQL_SCALAR_FUNCTION".to_string())
                }
            };
            rows.push(StoredRow {
                values: vec![
                    Value::Int(routine.object_id),
                    Value::VarChar(routine.name.clone()),
                    Value::Int(catalog.get_schema_id(&routine.schema).unwrap_or(1) as i32),
                    Value::Null,
                    Value::Int(0),
                    Value::Char(ty),
                    Value::VarChar(desc),
                    created.clone(),
                    created.clone(),
                    Value::Bit(false),
                ],
                deleted: false,
            });
        }
        for view in catalog.get_views() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(view.object_id),
                    Value::VarChar(view.name.clone()),
                    Value::Int(catalog.get_schema_id(&view.schema).unwrap_or(1) as i32),
                    Value::Null,
                    Value::Int(0),
                    Value::Char("V ".to_string()),
                    Value::VarChar("VIEW".to_string()),
                    created.clone(),
                    created.clone(),
                    Value::Bit(false),
                ],
                deleted: false,
            });
        }
        for trigger in catalog.get_triggers() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(trigger.object_id),
                    Value::VarChar(trigger.name.clone()),
                    Value::Int(catalog.get_schema_id(&trigger.schema).unwrap_or(1) as i32),
                    Value::Null,
                    Value::Int(
                        catalog
                            .object_id(&trigger.table_schema, &trigger.table_name)
                            .unwrap_or(0),
                    ),
                    Value::Char("TR".to_string()),
                    Value::VarChar("SQL_TRIGGER".to_string()),
                    created.clone(),
                    created.clone(),
                    Value::Bit(false),
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
                    Value::Null,
                    Value::Int(idx.table_id as i32),
                    Value::Char("IX".to_string()),
                    Value::VarChar("SQL_INDEX".to_string()),
                    created.clone(),
                    created.clone(),
                    Value::Bit(false),
                ],
                deleted: false,
            });
        }
        rows
    }
}

impl VirtualTable for SysSystemViews {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "system_views",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("principal_id", DataType::Int, true),
                ("schema_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, true),
                ("type_desc", DataType::VarChar { max_len: 60 }, true),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_ms_shipped", DataType::Bit, false),
                ("is_published", DataType::Bit, false),
                ("is_schema_published", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        // Return empty for now so SSMS doesn't crash on extended_properties check
        Vec::new()
    }
}

impl VirtualTable for SysCompatSysObjects {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "sysobjects",
            vec![
                ("id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("xtype", DataType::Char { len: 2 }, false),
                ("uid", DataType::SmallInt, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let base = SysObjects;
        base.rows(catalog)
            .into_iter()
            .map(|r| {
                let object_id = r.values.first().cloned().unwrap_or(Value::Int(0));
                let name = r
                    .values
                    .get(1)
                    .cloned()
                    .unwrap_or(Value::VarChar(String::new()));
                let xtype = match r.values.get(5) {
                    Some(Value::Char(v)) => Value::Char(v.clone()),
                    Some(Value::VarChar(v)) => Value::Char(v.clone()),
                    _ => Value::Char("U ".to_string()),
                };
                StoredRow {
                    values: vec![object_id, name, xtype, Value::SmallInt(1)],
                    deleted: false,
                }
            })
            .collect()
    }
}
