use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysPartitionFunctions;
pub(crate) struct SysPartitionParameters;
pub(crate) struct SysPartitionSchemes;
pub(crate) struct SysDestinationDataSpaces;
pub(crate) struct SysFilegroups;

impl VirtualTable for SysPartitionFunctions {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "partition_functions",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("function_id", DataType::Int, false),
                ("type", DataType::TinyInt, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("fanout", DataType::Int, false),
                ("boundary_value_on_right", DataType::Bit, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![]
    }
}

impl VirtualTable for SysPartitionParameters {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "partition_parameters",
            vec![
                ("parameter_id", DataType::Int, false),
                ("function_id", DataType::Int, false),
                ("system_type_id", DataType::Int, false),
                ("max_length", DataType::SmallInt, false),
                ("precision", DataType::TinyInt, false),
                ("scale", DataType::TinyInt, false),
                ("collation_name", DataType::VarChar { max_len: 128 }, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![]
    }
}

impl VirtualTable for SysPartitionSchemes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "partition_schemes",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("data_space_id", DataType::Int, false),
                ("function_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![]
    }
}

impl VirtualTable for SysDestinationDataSpaces {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "destination_data_spaces",
            vec![
                ("partition_scheme_id", DataType::Int, false),
                ("destination_id", DataType::Int, false),
                ("data_space_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![]
    }
}

impl VirtualTable for SysFilegroups {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "filegroups",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_read_only", DataType::Bit, false),
                ("is_default", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![StoredRow {
            values: vec![
                Value::VarChar("PRIMARY".to_string()),
                Value::Char("FG".to_string()),
                Value::VarChar("ROWS_FILEGROUP".to_string()),
                Value::Bit(false),
                Value::Bit(true),
            ],
            deleted: false,
        }]
    }
}
