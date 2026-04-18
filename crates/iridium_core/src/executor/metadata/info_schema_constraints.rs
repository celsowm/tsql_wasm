use super::VirtualTable;
use super::{schema_name_by_id, virtual_table_def, DB_CATALOG};
use crate::catalog::{Catalog, ColumnDef};
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(super) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    match name {
        n if n.eq_ignore_ascii_case("TABLE_CONSTRAINTS") => Some(Box::new(TableConstraints)),
        n if n.eq_ignore_ascii_case("CHECK_CONSTRAINTS") => Some(Box::new(CheckConstraints)),
        n if n.eq_ignore_ascii_case("REFERENTIAL_CONSTRAINTS") => {
            Some(Box::new(ReferentialConstraints))
        }
        n if n.eq_ignore_ascii_case("KEY_COLUMN_USAGE") => Some(Box::new(KeyColumnUsage)),
        n if n.eq_ignore_ascii_case("CONSTRAINT_COLUMN_USAGE") => {
            Some(Box::new(ConstraintColumnUsage))
        }
        n if n.eq_ignore_ascii_case("CONSTRAINT_TABLE_USAGE") => {
            Some(Box::new(ConstraintTableUsage))
        }
        _ => None,
    }
}

struct TableConstraints;
struct CheckConstraints;
struct ReferentialConstraints;
struct KeyColumnUsage;
struct ConstraintColumnUsage;
struct ConstraintTableUsage;

impl VirtualTable for TableConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "TABLE_CONSTRAINTS",
            vec![
                (
                    "CONSTRAINT_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                (
                    "CONSTRAINT_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("CONSTRAINT_TYPE", DataType::VarChar { max_len: 16 }, false),
                ("IS_DEFERRABLE", DataType::VarChar { max_len: 2 }, false),
                (
                    "INITIALLY_DEFERRED",
                    DataType::VarChar { max_len: 2 },
                    false,
                ),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
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
            let pk_cols: Vec<&str> = t
                .columns
                .iter()
                .filter(|c| c.primary_key)
                .map(|c| c.name.as_str())
                .collect();
            if !pk_cols.is_empty() {
                let pk_name = format!("PK_{}", t.name);
                rows.push(StoredRow {
                    values: base(&pk_name, "PRIMARY KEY"),
                    deleted: false,
                });
            }
            for col in &t.columns {
                if col.unique && !col.primary_key {
                    let uq_name = format!("UQ_{}_{}", t.name, col.name);
                    rows.push(StoredRow {
                        values: base(&uq_name, "UNIQUE"),
                        deleted: false,
                    });
                }
            }
            for chk in &t.check_constraints {
                rows.push(StoredRow {
                    values: base(&chk.name, "CHECK"),
                    deleted: false,
                });
            }
            for fk in &t.foreign_keys {
                rows.push(StoredRow {
                    values: base(&fk.name, "FOREIGN KEY"),
                    deleted: false,
                });
            }
            for col in &t.columns {
                if let Some(cn) = &col.default_constraint_name {
                    rows.push(StoredRow {
                        values: base(cn, "DEFAULT"),
                        deleted: false,
                    });
                }
            }
        }
        rows
    }
}

impl VirtualTable for CheckConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "CHECK_CONSTRAINTS",
            vec![
                (
                    "CONSTRAINT_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                (
                    "CONSTRAINT_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
                ("CHECK_CLAUSE", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
            for chk in &t.check_constraints {
                rows.push(StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(schema.clone()),
                        Value::VarChar(chk.name.clone()),
                        Value::VarChar(format!("({})", crate::executor::tooling::formatting::format_expr(&chk.expr))),
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}

impl VirtualTable for ReferentialConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "REFERENTIAL_CONSTRAINTS",
            vec![
                (
                    "CONSTRAINT_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                (
                    "CONSTRAINT_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
                (
                    "UNIQUE_CONSTRAINT_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                (
                    "UNIQUE_CONSTRAINT_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                (
                    "UNIQUE_CONSTRAINT_NAME",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("MATCH_OPTION", DataType::VarChar { max_len: 7 }, false),
                ("UPDATE_RULE", DataType::VarChar { max_len: 11 }, false),
                ("DELETE_RULE", DataType::VarChar { max_len: 11 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
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
        rows
    }
}

impl VirtualTable for KeyColumnUsage {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "KEY_COLUMN_USAGE",
            vec![
                (
                    "CONSTRAINT_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                (
                    "CONSTRAINT_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
                ("ORDINAL_POSITION", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let schema = schema_name_by_id(catalog, t.schema_id);
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
        rows
    }
}

impl VirtualTable for ConstraintColumnUsage {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "CONSTRAINT_COLUMN_USAGE",
            vec![
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
                (
                    "CONSTRAINT_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                (
                    "CONSTRAINT_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
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
            let pk_cols: Vec<&ColumnDef> = t.columns.iter().filter(|c| c.primary_key).collect();
            if !pk_cols.is_empty() {
                let pk_name = format!("PK_{}", t.name);
                for col in &pk_cols {
                    rows.push(row(&col.name, &pk_name));
                }
            }
            for col in &t.columns {
                if col.unique && !col.primary_key {
                    let uq_name = format!("UQ_{}_{}", t.name, col.name);
                    rows.push(row(&col.name, &uq_name));
                }
            }
            for fk in &t.foreign_keys {
                for col_name in &fk.columns {
                    rows.push(row(col_name, &fk.name));
                }
            }
            for col in &t.columns {
                if let Some(cn) = &col.check_constraint_name {
                    rows.push(row(&col.name, cn));
                }
            }
        }
        rows
    }
}

impl VirtualTable for ConstraintTableUsage {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "CONSTRAINT_TABLE_USAGE",
            vec![
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                (
                    "CONSTRAINT_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                (
                    "CONSTRAINT_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
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
        rows
    }
}
