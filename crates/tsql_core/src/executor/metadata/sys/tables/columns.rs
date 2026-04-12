use super::super::super::VirtualTable;
use super::super::super::{system_type_id, type_max_length, virtual_table_def};
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysColumns;
pub(crate) struct SysAllColumns;

impl VirtualTable for SysColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        column_table_def("columns", false)
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        column_rows(catalog, false)
    }
}

impl VirtualTable for SysAllColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        column_table_def("all_columns", true)
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        column_rows(catalog, true)
    }
}

fn column_table_def(name: &str, include_sparse: bool) -> crate::catalog::TableDef {
    let mut cols = vec![
        ("object_id", DataType::Int, false),
        ("column_id", DataType::Int, false),
        ("name", DataType::VarChar { max_len: 128 }, false),
        ("user_type_id", DataType::Int, false),
        ("system_type_id", DataType::TinyInt, false),
        ("max_length", DataType::SmallInt, false),
        ("precision", DataType::TinyInt, false),
        ("scale", DataType::TinyInt, false),
        ("is_nullable", DataType::Bit, false),
        ("is_computed", DataType::Bit, false),
        ("is_xml_document", DataType::Bit, false),
        ("is_column_set", DataType::Bit, false),
        ("xml_collection_id", DataType::Int, true),
        ("generated_always_type", DataType::TinyInt, false),
        ("graph_type", DataType::Int, true),
        ("default_object_id", DataType::Int, false),
        ("is_dropped_ledger_column", DataType::Bit, false),
    ];
    if include_sparse {
        cols.push(("is_sparse", DataType::Bit, false));
    }
    virtual_table_def(name, cols)
}

fn column_rows(catalog: &dyn Catalog, include_sparse: bool) -> Vec<StoredRow> {
    fn precision_scale(dt: &DataType) -> (u8, u8) {
        match dt {
            DataType::Bit => (1, 0),
            DataType::TinyInt => (3, 0),
            DataType::SmallInt => (5, 0),
            DataType::Int => (10, 0),
            DataType::BigInt => (19, 0),
            DataType::Float => (53, 0),
            DataType::Decimal { precision, scale } => (*precision, *scale),
            DataType::Money => (19, 4),
            DataType::SmallMoney => (10, 4),
            _ => (0, 0),
        }
    }

    let mut rows = Vec::new();
    for t in catalog.get_tables() {
        for c in &t.columns {
            let (precision, scale) = precision_scale(&c.data_type);
            let mut values = vec![
                Value::Int(t.id as i32),
                Value::Int(c.id as i32),
                Value::VarChar(c.name.clone()),
                Value::TinyInt(system_type_id(&c.data_type) as u8),
                Value::TinyInt(system_type_id(&c.data_type) as u8),
                Value::SmallInt(type_max_length(&c.data_type)),
                Value::TinyInt(precision),
                Value::TinyInt(scale),
                Value::Bit(c.nullable),
                Value::Bit(c.computed_expr.is_some()),
                Value::Bit(false),
                Value::Bit(false),
                Value::Null,
                Value::TinyInt(0),
                Value::Null,
                Value::Int(if c.default.is_some() {
                    let table_bucket = (t.id % 100_000) as i32;
                    3_000_000 + table_bucket * 1_000 + c.id as i32
                } else {
                    0
                }),
                Value::Bit(false),
            ];
            if include_sparse {
                values.push(Value::Bit(false));
            }
            rows.push(StoredRow {
                values,
                deleted: false,
            });
        }
    }

    for tt in catalog.get_table_types() {
        for (i, c) in tt.columns.iter().enumerate() {
            let dt = crate::executor::type_mapping::data_type_spec_to_runtime(&c.data_type);
            let (precision, scale) = precision_scale(&dt);
            let mut values = vec![
                Value::Int(tt.object_id),
                Value::Int((i + 1) as i32),
                Value::VarChar(c.name.clone()),
                Value::TinyInt(system_type_id(&dt) as u8),
                Value::TinyInt(system_type_id(&dt) as u8),
                Value::SmallInt(type_max_length(&dt)),
                Value::TinyInt(precision),
                Value::TinyInt(scale),
                Value::Bit(c.nullable),
                Value::Bit(c.computed_expr.is_some()),
                Value::Bit(false),
                Value::Bit(false),
                Value::Null,
                Value::TinyInt(0),
                Value::Null,
                Value::Int(0),
                Value::Bit(false),
            ];
            if include_sparse {
                values.push(Value::Bit(false));
            }
            rows.push(StoredRow {
                values,
                deleted: false,
            });
        }
    }
    rows
}
