use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysDmOsSysInfo;

impl VirtualTable for SysDmOsSysInfo {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_os_sys_info",
            vec![
                ("cpu_count", DataType::Int, false),
                ("hyperthread_ratio", DataType::Int, false),
                ("physical_memory_kb", DataType::BigInt, false),
                ("virtual_memory_kb", DataType::BigInt, false),
                ("committed_kb", DataType::BigInt, false),
                ("committed_target_kb", DataType::BigInt, false),
                ("visible_target_kb", DataType::BigInt, false),
                ("stack_size_in_bytes", DataType::Int, false),
                ("os_quantum", DataType::BigInt, false),
                ("os_error_mode", DataType::Int, false),
                ("os_priority_class", DataType::Int, false),
                ("max_workers_count", DataType::Int, false),
                ("scheduler_count", DataType::Int, false),
                ("scheduler_total_count", DataType::Int, false),
                ("deadline_priority_offset", DataType::Int, false),
                ("sqlserver_start_time_ms", DataType::BigInt, false),
                ("sqlserver_start_time", DataType::DateTime, false),
                ("socket_count", DataType::Int, false),
                ("cores_per_socket", DataType::Int, false),
                ("numa_node_count", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let start_time = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        vec![StoredRow {
            values: vec![
                Value::Int(4),               // cpu_count
                Value::Int(1),               // hyperthread_ratio
                Value::BigInt(16777216),     // physical_memory_kb (16GB)
                Value::BigInt(33554432),     // virtual_memory_kb (32GB)
                Value::BigInt(1048576),      // committed_kb (1GB)
                Value::BigInt(8388608),      // committed_target_kb (8GB)
                Value::BigInt(8388608),      // visible_target_kb (8GB)
                Value::Int(2097152),         // stack_size_in_bytes (2MB)
                Value::BigInt(100),          // os_quantum
                Value::Int(0),               // os_error_mode
                Value::Int(32),              // os_priority_class
                Value::Int(512),             // max_workers_count
                Value::Int(4),               // scheduler_count
                Value::Int(4),               // scheduler_total_count
                Value::Int(0),               // deadline_priority_offset
                Value::BigInt(0),            // sqlserver_start_time_ms
                start_time,                  // sqlserver_start_time
                Value::Int(1),               // socket_count
                Value::Int(4),               // cores_per_socket
                Value::Int(1),               // numa_node_count
            ],
            deleted: false,
        }]
    }
}
