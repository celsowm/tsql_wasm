use super::virtual_table_def;
use super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::DataType;

pub(super) fn lookup(name: &str) -> Option<Box<dyn VirtualTable>> {
    match name {
        n if n.eq_ignore_ascii_case("DOMAIN_CONSTRAINTS") => Some(Box::new(DomainConstraints)),
        _ => None,
    }
}

struct DomainConstraints;

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
