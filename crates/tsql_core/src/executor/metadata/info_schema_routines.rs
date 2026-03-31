use super::VirtualTable;
use super::{
    char_max_length, char_octet_length, charset_name, collation_name_val, datetime_precision_val,
    numeric_precision, numeric_precision_radix, numeric_scale_val, type_name, virtual_table_def,
    DB_CATALOG,
};
use crate::ast::RoutineParamType;
use crate::catalog::Catalog;
use crate::executor::type_mapping::data_type_spec_to_runtime;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(super) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    match name {
        n if n.eq_ignore_ascii_case("ROUTINES") => Some(Box::new(Routines)),
        n if n.eq_ignore_ascii_case("PARAMETERS") => Some(Box::new(Parameters)),
        _ => None,
    }
}

struct Routines;
struct Parameters;

impl VirtualTable for Routines {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "ROUTINES",
            vec![
                (
                    "SPECIFIC_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
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
                (
                    "COLLATION_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("COLLATION_SCHEMA", DataType::VarChar { max_len: 128 }, true),
                ("COLLATION_NAME", DataType::VarChar { max_len: 128 }, true),
                (
                    "CHARACTER_SET_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                (
                    "CHARACTER_SET_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                (
                    "CHARACTER_SET_NAME",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("NUMERIC_PRECISION", DataType::SmallInt, true),
                ("NUMERIC_PRECISION_RADIX", DataType::SmallInt, true),
                ("NUMERIC_SCALE", DataType::SmallInt, true),
                ("DATETIME_PRECISION", DataType::SmallInt, true),
                ("ROUTINE_BODY", DataType::VarChar { max_len: 30 }, false),
                (
                    "ROUTINE_DEFINITION",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("EXTERNAL_NAME", DataType::VarChar { max_len: 128 }, true),
                ("EXTERNAL_LANGUAGE", DataType::VarChar { max_len: 30 }, true),
                ("PARAMETER_STYLE", DataType::VarChar { max_len: 30 }, true),
                ("IS_DETERMINISTIC", DataType::VarChar { max_len: 10 }, false),
                ("SQL_DATA_ACCESS", DataType::VarChar { max_len: 30 }, false),
                ("IS_NULL_CALL", DataType::VarChar { max_len: 10 }, true),
                ("SQL_PATH", DataType::VarChar { max_len: 128 }, true),
                (
                    "SCHEMA_LEVEL_ROUTINE",
                    DataType::VarChar { max_len: 10 },
                    false,
                ),
                ("MAX_DYNAMIC_RESULT_SETS", DataType::SmallInt, false),
                (
                    "IS_USER_DEFINED_CAST",
                    DataType::VarChar { max_len: 10 },
                    false,
                ),
                (
                    "IS_IMPLICITLY_INVOCABLE",
                    DataType::VarChar { max_len: 10 },
                    false,
                ),
                ("CREATED", DataType::VarChar { max_len: 30 }, true),
                ("LAST_ALTERED", DataType::VarChar { max_len: 30 }, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog
            .get_routines()
            .iter()
            .map(|r| {
                let (routine_type, sql_access) = match &r.kind {
                    crate::catalog::RoutineKind::Procedure { .. } => ("PROCEDURE", "MODIFIES"),
                    crate::catalog::RoutineKind::Function { .. } => ("FUNCTION", "READS"),
                };
                let (ret_type, ret_char_max, ret_char_oct) = match &r.kind {
                    crate::catalog::RoutineKind::Function {
                        returns: Some(dt_spec),
                        ..
                    } => {
                        let dt = data_type_spec_to_runtime(dt_spec);
                        (
                            Value::VarChar(type_name(&dt)),
                            char_max_length(&dt),
                            char_octet_length(&dt),
                        )
                    }
                    _ => (Value::Null, Value::Null, Value::Null),
                };
                StoredRow {
                    values: vec![
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(r.schema.clone()),
                        Value::VarChar(r.name.clone()),
                        Value::VarChar(DB_CATALOG.to_string()),
                        Value::VarChar(r.schema.clone()),
                        Value::VarChar(r.name.clone()),
                        Value::VarChar(routine_type.to_string()),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        ret_type,
                        ret_char_max,
                        ret_char_oct,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::VarChar("SQL".to_string()),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::VarChar("NO".to_string()),
                        Value::VarChar(sql_access.to_string()),
                        Value::Null,
                        Value::Null,
                        Value::VarChar("YES".to_string()),
                        Value::SmallInt(0),
                        Value::VarChar("NO".to_string()),
                        Value::VarChar("NO".to_string()),
                        Value::Null,
                        Value::Null,
                    ],
                    deleted: false,
                }
            })
            .collect()
    }
}

impl VirtualTable for Parameters {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "PARAMETERS",
            vec![
                (
                    "SPECIFIC_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
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
                (
                    "COLLATION_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("COLLATION_SCHEMA", DataType::VarChar { max_len: 128 }, true),
                ("COLLATION_NAME", DataType::VarChar { max_len: 128 }, true),
                (
                    "CHARACTER_SET_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                (
                    "CHARACTER_SET_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                (
                    "CHARACTER_SET_NAME",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("NUMERIC_PRECISION", DataType::TinyInt, true),
                ("NUMERIC_PRECISION_RADIX", DataType::SmallInt, true),
                ("NUMERIC_SCALE", DataType::TinyInt, true),
                ("DATETIME_PRECISION", DataType::SmallInt, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for r in catalog.get_routines() {
            for (i, p) in r.params.iter().enumerate() {
                let (
                    data_type_name,
                    char_max,
                    char_oct,
                    coll,
                    charset,
                    num_prec,
                    num_radix,
                    num_scale,
                    dt_prec,
                ) = match &p.param_type {
                    RoutineParamType::Scalar(dt_spec) => {
                        let dt = data_type_spec_to_runtime(dt_spec);
                        (
                            type_name(&dt),
                            char_max_length(&dt),
                            char_octet_length(&dt),
                            collation_name_val(&dt),
                            charset_name(&dt),
                            numeric_precision(&dt),
                            numeric_precision_radix(&dt),
                            match numeric_scale_val(&dt) {
                                Value::Int(v) => Value::TinyInt(v as u8),
                                _ => Value::Null,
                            },
                            datetime_precision_val(&dt),
                        )
                    }
                    RoutineParamType::TableType(obj) => (
                        format!("{}.{}", obj.schema_or_dbo(), obj.name).to_lowercase(),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ),
                };
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
                        Value::VarChar(data_type_name),
                        char_max,
                        char_oct,
                        Value::Null,
                        Value::Null,
                        coll,
                        Value::Null,
                        Value::Null,
                        charset,
                        num_prec,
                        num_radix,
                        num_scale,
                        dt_prec,
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}
