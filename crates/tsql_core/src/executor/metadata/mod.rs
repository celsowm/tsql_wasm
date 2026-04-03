mod sys;
mod info_schema_tables;
mod info_schema_columns;
mod info_schema_routines;
mod info_schema_constraints;
mod info_schema_empty;
mod info_schema_dispatch;
pub(crate) mod system_vars;

use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) trait VirtualTable {
    fn definition(&self) -> TableDef;
    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow>;
}

pub(crate) fn resolve_virtual_table(
    schema: &str,
    name: &str,
    catalog: &dyn Catalog,
) -> Option<(TableDef, Vec<StoredRow>)> {
    let vt: Option<Box<dyn VirtualTable>> = if schema.eq_ignore_ascii_case("sys") {
        sys::lookup(name)
    } else if schema.eq_ignore_ascii_case("INFORMATION_SCHEMA") {
        info_schema_dispatch::lookup(name)
    } else {
        None
    };
    vt.map(|v| (v.definition(), v.rows(catalog)))
}

pub(crate) const DB_CATALOG: &str = "tsql_wasm";

pub(super) fn virtual_table_def(name: &str, cols: Vec<(&str, DataType, bool)>) -> TableDef {
    TableDef {
        id: 0,
        schema_id: 0,
        schema_name: "dbo".to_string(),
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
        foreign_keys: vec![],
    }
}

pub(super) fn schema_name_by_id(catalog: &dyn Catalog, id: u32) -> String {
    catalog
        .get_schemas()
        .iter()
        .find(|s| s.id == id)
        .map(|s| s.name.clone())
        .unwrap_or_else(|| "dbo".to_string())
}

pub(super) fn system_type_id(dt: &DataType) -> i32 {
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
        DataType::Xml => 241,
    }
}

pub(super) fn type_name(dt: &DataType) -> String {
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
        DataType::Xml => "xml".to_string(),
    }
}

pub(super) fn type_max_length(dt: &DataType) -> i16 {
    match dt {
        DataType::Char { len } | DataType::NChar { len } => *len as i16,
        DataType::VarChar { max_len } | DataType::NVarChar { max_len } => *max_len as i16,
        DataType::Binary { len } => *len as i16,
        DataType::VarBinary { max_len } => *max_len as i16,
        DataType::Xml => -1,
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

pub(super) fn char_max_length(dt: &DataType) -> Value {
    match dt {
        DataType::Char { len } | DataType::NChar { len } => Value::Int(*len as i32),
        DataType::VarChar { max_len } | DataType::NVarChar { max_len } => {
            Value::Int(*max_len as i32)
        }
        DataType::Binary { len } => Value::Int(*len as i32),
        DataType::VarBinary { max_len } => Value::Int(*max_len as i32),
        DataType::Xml => Value::Int(-1),
        _ => Value::Null,
    }
}

pub(super) fn char_octet_length(dt: &DataType) -> Value {
    match dt {
        DataType::Char { len } | DataType::VarChar { max_len: len } => Value::Int(*len as i32),
        DataType::NChar { len } | DataType::NVarChar { max_len: len } => {
            Value::Int(*len as i32 * 2)
        }
        DataType::Binary { len } => Value::Int(*len as i32),
        DataType::VarBinary { max_len } => Value::Int(*max_len as i32),
        DataType::Xml => Value::Int(-1),
        _ => Value::Null,
    }
}

pub(super) fn numeric_precision(dt: &DataType) -> Value {
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

pub(super) fn numeric_precision_radix(dt: &DataType) -> Value {
    match dt {
        DataType::Float => Value::SmallInt(2),
        DataType::Bit
        | DataType::TinyInt
        | DataType::SmallInt
        | DataType::Int
        | DataType::BigInt
        | DataType::Decimal { .. }
        | DataType::Money
        | DataType::SmallMoney => Value::SmallInt(10),
        _ => Value::Null,
    }
}

pub(super) fn numeric_scale_val(dt: &DataType) -> Value {
    match dt {
        DataType::Bit | DataType::TinyInt | DataType::SmallInt | DataType::Int | DataType::BigInt => {
            Value::Int(0)
        }
        DataType::Decimal { scale, .. } => Value::Int(*scale as i32),
        DataType::Money | DataType::SmallMoney => Value::Int(4),
        _ => Value::Null,
    }
}

pub(super) fn datetime_precision_val(dt: &DataType) -> Value {
    match dt {
        DataType::Date => Value::SmallInt(0),
        DataType::DateTime => Value::SmallInt(3),
        DataType::DateTime2 => Value::SmallInt(7),
        DataType::Time => Value::SmallInt(7),
        _ => Value::Null,
    }
}

pub(super) fn charset_name(dt: &DataType) -> Value {
    match dt {
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Value::VarChar("iso_1".to_string())
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Value::VarChar("UNICODE".to_string())
        }
        _ => Value::Null,
    }
}

pub(super) fn collation_name_val(dt: &DataType) -> Value {
    match dt {
        DataType::Char { .. }
        | DataType::VarChar { .. }
        | DataType::NChar { .. }
        | DataType::NVarChar { .. } => Value::VarChar("SQL_Latin1_General_CP1_CI_AS".to_string()),
        _ => Value::Null,
    }
}

pub(super) fn builtin_types_rows() -> Vec<StoredRow> {
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
        (241, "xml", -1, 0, 0),
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
