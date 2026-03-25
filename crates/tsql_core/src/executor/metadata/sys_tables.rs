use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};
use super::VirtualTable;
use super::{builtin_types_rows, virtual_table_def, system_type_id, type_max_length};

pub(crate) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    if name.eq_ignore_ascii_case("schemas") {
        Some(Box::new(SysSchemas))
    } else if name.eq_ignore_ascii_case("tables") {
        Some(Box::new(SysTables))
    } else if name.eq_ignore_ascii_case("columns") {
        Some(Box::new(SysColumns))
    } else if name.eq_ignore_ascii_case("types") {
        Some(Box::new(SysTypes))
    } else if name.eq_ignore_ascii_case("indexes") {
        Some(Box::new(SysIndexes))
    } else if name.eq_ignore_ascii_case("objects") {
        Some(Box::new(SysObjects))
    } else if name.eq_ignore_ascii_case("check_constraints") {
        Some(Box::new(SysCheckConstraints))
    } else if name.eq_ignore_ascii_case("routines") {
        Some(Box::new(SysRoutines))
    } else {
        None
    }
}

struct SysSchemas;
struct SysTables;
struct SysColumns;
struct SysTypes;
struct SysIndexes;
struct SysObjects;
struct SysCheckConstraints;
struct SysRoutines;

impl VirtualTable for SysSchemas {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "schemas",
            vec![
                ("schema_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog
            .get_schemas()
            .iter()
            .map(|s| StoredRow {
                values: vec![Value::Int(s.id as i32), Value::VarChar(s.name.clone())],
                deleted: false,
            })
            .collect()
    }
}

impl VirtualTable for SysTables {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "tables",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("schema_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog
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
            .collect()
    }
}

impl VirtualTable for SysColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "columns",
            vec![
                ("object_id", DataType::Int, false),
                ("column_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("user_type_id", DataType::Int, false),
                ("max_length", DataType::SmallInt, false),
                ("is_nullable", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
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
        rows
    }
}

impl VirtualTable for SysTypes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "types",
            vec![
                ("user_type_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("max_length", DataType::SmallInt, false),
                ("precision", DataType::TinyInt, false),
                ("scale", DataType::TinyInt, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        builtin_types_rows()
    }
}

impl VirtualTable for SysIndexes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "indexes",
            vec![
                ("object_id", DataType::Int, false),
                ("index_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::TinyInt, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_unique", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog
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
            .collect()
    }
}

impl VirtualTable for SysObjects {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "objects",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("schema_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
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
        rows
    }
}

impl VirtualTable for SysCheckConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "check_constraints",
            vec![
                ("object_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
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
        rows
    }
}

impl VirtualTable for SysRoutines {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "routines",
            vec![
                ("object_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
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
        rows
    }
}
