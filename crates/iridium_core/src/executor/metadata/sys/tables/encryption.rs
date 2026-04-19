use super::super::super::VirtualTable;
use super::super::super::virtual_table_def;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::DataType;

pub(crate) struct SysColumnEncryptionKeys;
pub(crate) struct SysColumnMasterKeys;
pub(crate) struct SysColumnEncryptionKeyValues;

impl VirtualTable for SysColumnEncryptionKeys {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "column_encryption_keys",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("column_encryption_key_id", DataType::Int, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_ms_shipped", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysColumnMasterKeys {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "column_master_keys",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("column_master_key_id", DataType::Int, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("key_store_provider_name", DataType::VarChar { max_len: 128 }, false),
                ("key_path", DataType::VarChar { max_len: 512 }, false),
                ("allow_enclave_computations", DataType::Bit, false),
                ("signature", DataType::VarBinary { max_len: 8000 }, true),
                ("is_ms_shipped", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysColumnEncryptionKeyValues {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "column_encryption_key_values",
            vec![
                ("column_encryption_key_id", DataType::Int, false),
                ("column_master_key_id", DataType::Int, false),
                ("encrypted_value", DataType::VarBinary { max_len: 8000 }, false),
                ("encryption_algorithm_name", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}
