use super::VirtualTable;
use super::{virtual_table_def, DB_CATALOG};
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct Domains;
pub(crate) struct ColumnDomainUsage;

impl VirtualTable for Domains {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "DOMAINS",
            vec![
                ("DOMAIN_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("DOMAIN_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("DOMAIN_NAME", DataType::VarChar { max_len: 128 }, false),
                ("DATA_TYPE", DataType::VarChar { max_len: 128 }, false),
                ("CHARACTER_MAXIMUM_LENGTH", DataType::Int, true),
                ("NUMERIC_PRECISION", DataType::TinyInt, true),
                ("NUMERIC_SCALE", DataType::Int, true),
                ("DOMAIN_DEFAULT", DataType::VarChar { max_len: 128 }, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        catalog
            .get_table_types()
            .iter()
            .map(|tt| StoredRow {
                values: vec![
                    Value::VarChar(DB_CATALOG.to_string()),
                    Value::VarChar(tt.schema.clone()),
                    Value::VarChar(tt.name.clone()),
                    Value::VarChar("TABLE TYPE".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ],
                deleted: false,
            })
            .collect()
    }
}

impl VirtualTable for ColumnDomainUsage {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "COLUMN_DOMAIN_USAGE",
            vec![
                ("DOMAIN_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("DOMAIN_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("DOMAIN_NAME", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("TABLE_NAME", DataType::VarChar { max_len: 128 }, false),
                ("COLUMN_NAME", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        // Since we only support table types as domains for now, and they don't really
        // "usage" columns in the traditional SQL sense, we return empty or
        // we could eventually map columns that use these types.
        Vec::new()
    }
}
