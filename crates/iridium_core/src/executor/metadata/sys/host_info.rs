use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysHostInfo;

impl VirtualTable for SysHostInfo {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_os_host_info",
            vec![
                ("host_platform", DataType::VarChar { max_len: 128 }, false),
                (
                    "host_distribution",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("host_release", DataType::VarChar { max_len: 128 }, false),
                (
                    "host_service_pack_level",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("host_sku", DataType::Int, false),
                ("os_language_version", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![StoredRow {
            values: vec![
                Value::VarChar("Windows".to_string()),
                Value::VarChar("Windows".to_string()),
                Value::VarChar("10.0".to_string()),
                Value::VarChar(String::new()),
                Value::Int(7),
                Value::Int(1033),
            ],
            deleted: false,
        }]
    }
}
