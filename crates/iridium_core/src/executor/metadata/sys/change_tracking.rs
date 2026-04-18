use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::DataType;

pub(crate) struct SysChangeTrackingTables;

impl VirtualTable for SysChangeTrackingTables {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "change_tracking_tables",
            vec![
                ("object_id", DataType::Int, false),
                ("begin_version", DataType::BigInt, true),
                ("valid_version", DataType::BigInt, true),
                ("min_valid_version", DataType::BigInt, true),
                ("cleanup_version", DataType::BigInt, true),
                ("is_track_columns_updated_on", DataType::Bit, true),
                ("is_auto_cleanup_on", DataType::Bit, true),
                ("retention_period", DataType::Int, true),
                ("retention_period_units", DataType::TinyInt, true),
                ("retention_period_units_desc", DataType::VarChar { max_len: 60 }, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        // Change tracking is not supported, return empty result set
        Vec::new()
    }
}
