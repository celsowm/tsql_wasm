use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysDmDbIndexUsageStats;
pub(crate) struct SysDmDbPartitionStats;
pub(crate) struct SysDmDbIndexPhysicalStats;

impl VirtualTable for SysDmDbIndexUsageStats {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_db_index_usage_stats",
            vec![
                ("database_id", DataType::Int, false),
                ("object_id", DataType::Int, false),
                ("index_id", DataType::Int, false),
                ("user_seeks", DataType::BigInt, false),
                ("user_scans", DataType::BigInt, false),
                ("user_lookups", DataType::BigInt, false),
                ("user_updates", DataType::BigInt, false),
                ("last_user_seek", DataType::DateTime, true),
                ("last_user_scan", DataType::DateTime, true),
                ("last_user_lookup", DataType::DateTime, true),
                ("last_user_update", DataType::DateTime, true),
                ("system_seeks", DataType::BigInt, false),
                ("system_scans", DataType::BigInt, false),
                ("system_lookups", DataType::BigInt, false),
                ("system_updates", DataType::BigInt, false),
                ("last_system_seek", DataType::DateTime, true),
                ("last_system_scan", DataType::DateTime, true),
                ("last_system_lookup", DataType::DateTime, true),
                ("last_system_update", DataType::DateTime, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        // Return zeros for all indexes for now to satisfy tools
        for t in catalog.get_tables() {
            let table_indexes: Vec<_> = catalog
                .get_indexes()
                .iter()
                .filter(|idx| idx.table_id == t.id)
                .collect();

            let mut add_usage = |index_id: i32| {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(5), // iridium_sql
                        Value::Int(t.id as i32),
                        Value::Int(index_id),
                        Value::BigInt(0), Value::BigInt(0), Value::BigInt(0), Value::BigInt(0),
                        Value::Null, Value::Null, Value::Null, Value::Null,
                        Value::BigInt(0), Value::BigInt(0), Value::BigInt(0), Value::BigInt(0),
                        Value::Null, Value::Null, Value::Null, Value::Null,
                    ],
                    deleted: false,
                });
            };

            add_usage(0); // Heap/Clustered index
            for idx in table_indexes {
                if idx.id != 0 {
                    add_usage(idx.id as i32);
                }
            }
        }
        rows
    }
}

impl VirtualTable for SysDmDbPartitionStats {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_db_partition_stats",
            vec![
                ("partition_id", DataType::BigInt, false),
                ("object_id", DataType::Int, false),
                ("index_id", DataType::Int, false),
                ("partition_number", DataType::Int, false),
                ("in_row_data_page_count", DataType::BigInt, false),
                ("in_row_used_page_count", DataType::BigInt, false),
                ("in_row_reserved_page_count", DataType::BigInt, false),
                ("lob_used_page_count", DataType::BigInt, false),
                ("lob_reserved_page_count", DataType::BigInt, false),
                ("row_overflow_used_page_count", DataType::BigInt, false),
                ("row_overflow_reserved_page_count", DataType::BigInt, false),
                ("used_page_count", DataType::BigInt, false),
                ("reserved_page_count", DataType::BigInt, false),
                ("row_count", DataType::BigInt, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for t in catalog.get_tables() {
            let table_indexes: Vec<_> = catalog
                .get_indexes()
                .iter()
                .filter(|idx| idx.table_id == t.id)
                .collect();

            let mut add_stats = |index_id: i32, partition_id: i64| {
                rows.push(StoredRow {
                    values: vec![
                        Value::BigInt(partition_id),
                        Value::Int(t.id as i32),
                        Value::Int(index_id),
                        Value::Int(1), // partition_number
                        Value::BigInt(0), Value::BigInt(0), Value::BigInt(0),
                        Value::BigInt(0), Value::BigInt(0),
                        Value::BigInt(0), Value::BigInt(0),
                        Value::BigInt(0), Value::BigInt(0),
                        Value::BigInt(0), // row_count
                    ],
                    deleted: false,
                });
            };

            if table_indexes.is_empty() {
                let partition_id = 72057594040000000 + (t.id as i64);
                add_stats(0, partition_id);
            } else {
                for idx in table_indexes {
                    let partition_id = 72057594040000000 + (idx.id as i64);
                    add_stats(idx.id as i32, partition_id);
                }
            }
        }
        rows
    }
}

impl VirtualTable for SysDmDbIndexPhysicalStats {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_db_index_physical_stats",
            vec![
                ("database_id", DataType::SmallInt, false),
                ("object_id", DataType::Int, false),
                ("index_id", DataType::Int, false),
                ("partition_number", DataType::Int, false),
                ("index_type_desc", DataType::NVarChar { max_len: 60 }, false),
                ("alloc_unit_type_desc", DataType::NVarChar { max_len: 60 }, false),
                ("index_depth", DataType::TinyInt, false),
                ("index_level", DataType::TinyInt, false),
                ("avg_fragmentation_in_percent", DataType::Float, false),
                ("fragment_count", DataType::BigInt, false),
                ("avg_fragment_size_in_pages", DataType::Float, false),
                ("page_count", DataType::BigInt, false),
                ("avg_page_space_used_in_percent", DataType::Float, false),
                ("record_count", DataType::BigInt, false),
                ("ghost_record_count", DataType::BigInt, false),
                ("version_record_count", DataType::BigInt, false),
                ("min_record_size_in_bytes", DataType::Int, false),
                ("max_record_size_in_bytes", DataType::Int, false),
                ("avg_record_size_in_bytes", DataType::Float, false),
                ("forwarded_record_count", DataType::BigInt, true),
                ("compressed_page_count", DataType::BigInt, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        // Return empty for now as it is usually used as a TVF
        vec![]
    }
}
