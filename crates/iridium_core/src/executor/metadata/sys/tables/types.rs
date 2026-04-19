use super::super::super::VirtualTable;
use super::super::super::{builtin_types_rows, virtual_table_def};
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysTypes;
pub(crate) struct SysTableTypes;
pub(crate) struct SysAssemblyTypes;

impl VirtualTable for SysTypes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "types",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("system_type_id", DataType::TinyInt, false),
                ("user_type_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("max_length", DataType::SmallInt, false),
                ("precision", DataType::TinyInt, false),
                ("scale", DataType::TinyInt, false),
                ("is_user_defined", DataType::Bit, false),
                ("is_assembly_type", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows: Vec<StoredRow> = builtin_types_rows()
            .into_iter()
            .map(|row| {
                let id = match row.values.first() {
                    Some(Value::Int(v)) => *v,
                    _ => 0,
                };
                let name = row
                    .values
                    .get(1)
                    .cloned()
                    .unwrap_or(Value::VarChar(String::new()));
                let max_length = row.values.get(2).cloned().unwrap_or(Value::SmallInt(0));
                let precision = row.values.get(3).cloned().unwrap_or(Value::TinyInt(0));
                let scale = row.values.get(4).cloned().unwrap_or(Value::TinyInt(0));
                StoredRow {
                    values: vec![
                        name,
                        Value::TinyInt(id as u8),
                        Value::Int(id),
                        Value::Int(4), // sys schema
                        max_length,
                        precision,
                        scale,
                        Value::Bit(false),
                        Value::Bit(false),
                    ],
                    deleted: false,
                }
            })
            .collect();

        for tt in catalog.get_table_types() {
            let schema_id = catalog.get_schema_id(&tt.schema).unwrap_or(1);
            rows.push(StoredRow {
                values: vec![
                    Value::VarChar(tt.name.clone()),
                    Value::TinyInt(243), // table type
                    Value::Int(tt.object_id),
                    Value::Int(schema_id as i32),
                    Value::SmallInt(-1),
                    Value::TinyInt(0),
                    Value::TinyInt(0),
                    Value::Bit(true),
                    Value::Bit(false),
                ],
                deleted: false,
            });
        }
        rows
    }
}

impl VirtualTable for SysTableTypes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "table_types",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("user_type_id", DataType::Int, false),
                ("system_type_id", DataType::TinyInt, false),
                ("schema_id", DataType::Int, false),
                ("type_table_object_id", DataType::Int, false),
                ("is_memory_optimized", DataType::Bit, false),
                ("is_user_defined", DataType::Bit, false),
                ("is_table_type", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        catalog
            .get_table_types()
            .iter()
            .map(|tt| {
                let schema_id = catalog.get_schema_id(&tt.schema).unwrap_or(1);
                StoredRow {
                    values: vec![
                        Value::VarChar(tt.name.clone()),
                        Value::Int(tt.object_id),
                        Value::TinyInt(243), // table type
                        Value::Int(schema_id as i32),
                        Value::Int(tt.object_id),
                        Value::Bit(false),
                        Value::Bit(true),
                        Value::Bit(true),
                    ],
                    deleted: false,
                }
            })
            .collect()
    }
}

impl VirtualTable for SysAssemblyTypes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "assembly_types",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("system_type_id", DataType::TinyInt, false),
                ("user_type_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("principal_id", DataType::Int, true),
                ("assembly_id", DataType::Int, false),
                ("assembly_class", DataType::NVarChar { max_len: 128 }, true),
                ("is_binary_ordered", DataType::Bit, false),
                ("is_fixed_length", DataType::Bit, false),
                ("max_length", DataType::SmallInt, false),
                ("precision", DataType::TinyInt, false),
                ("scale", DataType::TinyInt, false),
                ("collation_name", DataType::VarChar { max_len: 128 }, true),
                ("is_nullable", DataType::Bit, false),
                ("is_user_defined", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}
