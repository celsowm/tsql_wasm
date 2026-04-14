use super::super::{numeric_precision, numeric_scale_val, virtual_table_def, VirtualTable};
use crate::ast::RoutineParamType;
use crate::catalog::Catalog;
use crate::executor::type_mapping::data_type_spec_to_runtime;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysParameters;

impl VirtualTable for SysParameters {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "parameters",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, true),
                ("parameter_id", DataType::Int, false),
                ("system_type_id", DataType::TinyInt, false),
                ("user_type_id", DataType::Int, false),
                ("max_length", DataType::SmallInt, false),
                ("precision", DataType::TinyInt, false),
                ("scale", DataType::TinyInt, false),
                ("is_output", DataType::Bit, false),
                ("is_readonly", DataType::Bit, false),
                ("is_xml_document", DataType::Bit, false),
                ("xml_collection_id", DataType::Int, false),
                ("default_value", DataType::VarChar { max_len: 128 }, true),
                ("has_default_value", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for r in catalog.get_routines() {
            for (i, p) in r.params.iter().enumerate() {
                let (dt, is_user_defined) = match &p.param_type {
                    RoutineParamType::Scalar(dt_spec) => {
                        (data_type_spec_to_runtime(dt_spec), false)
                    }
                    RoutineParamType::TableType(_) => {
                        // For table types, we use a placeholder or look up its base type if applicable.
                        // SQL Server sys.parameters for TVP shows system_type_id = 243 (table type).
                        (DataType::SqlVariant, true) // Simplified
                    }
                };

                let type_id = if is_user_defined {
                    match &p.param_type {
                        RoutineParamType::TableType(obj) => catalog
                            .find_table_type(obj.schema_or_dbo(), &obj.name)
                            .map(|tt| tt.object_id)
                            .unwrap_or(0),
                        _ => 0,
                    }
                } else {
                    super::super::system_type_id(&dt)
                };

                rows.push(StoredRow {
                    values: vec![
                        Value::Int(r.object_id),
                        Value::VarChar(p.name.clone()),
                        Value::Int((i + 1) as i32),
                        Value::TinyInt(if is_user_defined { 243 } else { type_id as u8 }),
                        Value::Int(type_id),
                        Value::SmallInt(super::super::type_max_length(&dt)),
                        match numeric_precision(&dt) {
                            Value::TinyInt(v) => Value::TinyInt(v),
                            _ => Value::TinyInt(0),
                        },
                        match numeric_scale_val(&dt) {
                            Value::Int(v) => Value::TinyInt(v as u8),
                            _ => Value::TinyInt(0),
                        },
                        Value::Bit(p.is_output),
                        Value::Bit(p.is_readonly),
                        Value::Bit(false),
                        Value::Int(0),
                        Value::Null,
                        Value::Bit(p.default.is_some()),
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}
