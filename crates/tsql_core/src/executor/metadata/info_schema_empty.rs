use super::virtual_table_def;
use super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::DataType;

pub(super) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    match name {
        n if n.eq_ignore_ascii_case("COLUMN_DOMAIN_USAGE") => Some(Box::new(ColumnDomainUsage)),
        n if n.eq_ignore_ascii_case("DOMAINS") => Some(Box::new(Domains)),
        n if n.eq_ignore_ascii_case("DOMAIN_CONSTRAINTS") => Some(Box::new(DomainConstraints)),
        _ => None,
    }
}

struct ColumnDomainUsage;
struct Domains;
struct DomainConstraints;

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

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}

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

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}

impl VirtualTable for DomainConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "DOMAIN_CONSTRAINTS",
            vec![
                (
                    "CONSTRAINT_CATALOG",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                (
                    "CONSTRAINT_SCHEMA",
                    DataType::VarChar { max_len: 128 },
                    false,
                ),
                ("CONSTRAINT_NAME", DataType::VarChar { max_len: 128 }, false),
                ("DOMAIN_CATALOG", DataType::VarChar { max_len: 128 }, false),
                ("DOMAIN_SCHEMA", DataType::VarChar { max_len: 128 }, false),
                ("DOMAIN_NAME", DataType::VarChar { max_len: 128 }, false),
                ("IS_DEFERRABLE", DataType::VarChar { max_len: 2 }, false),
                (
                    "INITIALLY_DEFERRED",
                    DataType::VarChar { max_len: 2 },
                    false,
                ),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}
