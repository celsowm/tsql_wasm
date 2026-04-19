use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysSptValues;

impl VirtualTable for SysSptValues {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "spt_values",
            vec![
                ("name", DataType::NVarChar { max_len: 35 }, true),
                ("number", DataType::Int, false),
                ("type", DataType::NChar { len: 3 }, false),
                ("low", DataType::Int, true),
                ("high", DataType::Int, true),
                ("status", DataType::Int, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        // Return a minimal set of values often queried by SSMS
        // In this case, SSMS was querying for @PageSize = v.low/1024.0 where v.number=1 and v.type='E'
        vec![
            StoredRow {
                values: vec![
                    Value::NVarChar("MASTER".to_string()),
                    Value::Int(1),
                    Value::NChar("E  ".to_string()),
                    Value::Int(8192), // 8KB pages
                    Value::Null,
                    Value::Null,
                ],
                deleted: false,
            },
        ]
    }
}
