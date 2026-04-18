use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::DataType;

pub(crate) struct SysFullTextIndexes;
pub(crate) struct SysFullTextCatalogs;

impl VirtualTable for SysFullTextIndexes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "fulltext_indexes",
            vec![
                ("object_id", DataType::Int, false),
                ("fulltext_catalog_id", DataType::Int, true),
                ("data_space_id", DataType::Int, true),
                ("unique_index_id", DataType::Int, false),
                ("is_enabled", DataType::Bit, false),
                ("has_columnstore", DataType::Bit, false),
                ("stoplist_id", DataType::Int, true),
                ("change_tracking_state", DataType::TinyInt, true),
                ("change_tracking_state_desc", DataType::VarChar { max_len: 60 }, true),
                ("has_active_crawl", DataType::Bit, true),
                ("cached_column_count", DataType::Int, true),
                ("incremental_timestamp", DataType::BigInt, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        // Full-text indexes is not supported, return empty result set
        Vec::new()
    }
}

impl VirtualTable for SysFullTextCatalogs {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "fulltext_catalogs",
            vec![
                ("fulltext_catalog_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("data_space_id", DataType::Int, true),
                ("is_default", DataType::Bit, true),
                ("is_importing", DataType::Bit, false),
                ("is_paused", DataType::Bit, false),
                ("status", DataType::TinyInt, false),
                ("status_desc", DataType::VarChar { max_len: 60 }, false),
                ("number_of_fulltext_docs", DataType::Int, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        // Full-text catalogs are not supported, return empty result set
        Vec::new()
    }
}
