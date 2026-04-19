use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::executor::tooling::formatting::format_expr;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysCheckConstraints;
pub(crate) struct SysForeignKeys;
pub(crate) struct SysKeyConstraints;
pub(crate) struct SysDefaultConstraints;

impl VirtualTable for SysCheckConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "check_constraints",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("definition", DataType::VarChar { max_len: 8000 }, true),
                ("is_not_for_replication", DataType::Bit, false),
                ("is_disabled", DataType::Bit, false),
                ("is_not_trusted", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        let mut object_id = 1_000_000i32;
        for t in catalog.get_tables() {
            for chk in &t.check_constraints {
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(chk.name.clone()),
                        Value::Int(object_id),
                        Value::Int(t.id as i32),
                        Value::VarChar(format!("({})", format_expr(&chk.expr))),
                        Value::Bit(false),
                        Value::Bit(false),
                        Value::Bit(false),
                    ],
                    deleted: false,
                });
                object_id += 1;
            }
        }
        rows
    }
}

impl VirtualTable for SysForeignKeys {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "foreign_keys",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 128 }, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_ms_shipped", DataType::Bit, false),
                ("is_disabled", DataType::Bit, false),
                ("referenced_object_id", DataType::Int, false),
                ("key_index_id", DataType::Int, false),
                ("delete_referential_action", DataType::TinyInt, false),
                (
                    "delete_referential_action_desc",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("update_referential_action", DataType::TinyInt, false),
                (
                    "update_referential_action_desc",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        let mut fk_idx = 0;

        for table in catalog.get_tables() {
            for fk in &table.foreign_keys {
                let object_id = 4_000_000 + fk_idx;
                fk_idx += 1;
                let parent_id = table.id as i32;
                let ref_schema = fk.referenced_table.schema_or_dbo();
                let referenced_object_id = catalog
                    .find_table(ref_schema, &fk.referenced_table.name)
                    .map(|t| t.id as i32)
                    .unwrap_or(0);

                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(fk.name.clone()),
                        Value::Int(object_id),
                        Value::Int(parent_id),
                        Value::Char("F ".to_string()),
                        Value::VarChar("FOREIGN_KEY_CONSTRAINT".to_string()),
                        Value::DateTime(
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        ),
                        Value::DateTime(
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        ),
                        Value::Bit(false),
                        Value::Bit(false),
                        Value::Int(referenced_object_id),
                        Value::Int(0),
                        Value::TinyInt(0),
                        Value::VarChar("NO_ACTION".to_string()),
                        Value::TinyInt(0),
                        Value::VarChar("NO_ACTION".to_string()),
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}

impl VirtualTable for SysKeyConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "key_constraints",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_system_named", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        let mut object_id = 2_000_000i32;

        for table in catalog.get_tables() {
            for col in &table.columns {
                if col.primary_key {
                    rows.push(StoredRow {
                        values: vec![
                            Value::VarChar(format!("PK_{}", table.name)),
                            Value::Int(object_id),
                            Value::Int(table.schema_id as i32),
                            Value::Int(table.id as i32),
                            Value::Char("PK".to_string()),
                            Value::VarChar("PRIMARY_KEY_CONSTRAINT".to_string()),
                            Value::Bit(true),
                        ],
                        deleted: false,
                    });
                    object_id += 1;
                } else if col.unique {
                    rows.push(StoredRow {
                        values: vec![
                            Value::VarChar(format!("UQ_{}_{}", table.name, col.name)),
                            Value::Int(object_id),
                            Value::Int(table.schema_id as i32),
                            Value::Int(table.id as i32),
                            Value::Char("UQ".to_string()),
                            Value::VarChar("UNIQUE_CONSTRAINT".to_string()),
                            Value::Bit(true),
                        ],
                        deleted: false,
                    });
                    object_id += 1;
                }
            }
        }
        rows
    }
}

impl VirtualTable for SysDefaultConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "default_constraints",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("create_date", DataType::DateTime, false),
                ("parent_column_id", DataType::Int, false),
                ("definition", DataType::VarChar { max_len: 8000 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();

        for table in catalog.get_tables() {
            for col in &table.columns {
                if let Some(default_expr) = &col.default {
                    let name = col
                        .default_constraint_name
                        .clone()
                        .unwrap_or_else(|| format!("DF_{}_{}", table.name, col.name));

                    rows.push(StoredRow {
                        values: vec![
                            Value::VarChar(name),
                            Value::Int({
                                let table_bucket = (table.id % 100_000) as i32;
                                3_000_000 + table_bucket * 1_000 + col.id as i32
                            }),
                            Value::Int(table.schema_id as i32),
                            Value::Int(table.id as i32),
                            Value::Char("D ".to_string()),
                            Value::VarChar("DEFAULT_CONSTRAINT".to_string()),
                            Value::DateTime(
                                chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .unwrap()
                                    .and_hms_opt(0, 0, 0)
                                    .unwrap(),
                            ),
                            Value::Int(col.id as i32),
                            Value::VarChar(format!("({})", format_expr(default_expr))),
                        ],
                        deleted: false,
                    });
                }
            }
        }
        rows
    }
}
