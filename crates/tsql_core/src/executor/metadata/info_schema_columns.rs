use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};
use super::VirtualTable;
use super::{
    char_max_length, char_octet_length, charset_name, collation_name_val,
    datetime_precision_val, numeric_precision, numeric_precision_radix, numeric_scale_val,
    schema_name_by_id, type_name, virtual_table_def, DB_CATALOG,
};

pub(super) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    if name.eq_ignore_ascii_case("COLUMNS") {
        Some(Box::new(Columns))
    } else {
        None
    }
}

struct Columns;

impl VirtualTable for Columns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def("COLUMNS", vec![
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
        ])
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
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
                        Value::Null,
                        Value::Null,
                        charset_name(&c.data_type),
                        Value::Null,
                        Value::Null,
                        collation_name_val(&c.data_type),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}
