use super::super::super::virtual_table_def;
use super::super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysIdentityColumns;

impl VirtualTable for SysIdentityColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "identity_columns",
            vec![
                ("object_id", DataType::Int, false),
                ("column_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                (
                    "seed_value",
                    DataType::Decimal {
                        precision: 38,
                        scale: 0,
                    },
                    false,
                ),
                (
                    "increment_value",
                    DataType::Decimal {
                        precision: 38,
                        scale: 0,
                    },
                    false,
                ),
                (
                    "last_value",
                    DataType::Decimal {
                        precision: 38,
                        scale: 0,
                    },
                    true,
                ),
                ("is_not_for_replication", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for table in catalog.get_tables() {
            for col in &table.columns {
                if let Some(identity) = &col.identity {
                    rows.push(StoredRow {
                        values: vec![
                            Value::Int(table.id as i32),
                            Value::Int(col.id as i32),
                            Value::VarChar(col.name.clone()),
                            Value::Decimal(identity.seed as i128, 0),
                            Value::Decimal(identity.increment as i128, 0),
                            Value::Decimal(identity.current as i128, 0),
                            Value::Bit(false),
                        ],
                        deleted: false,
                    });
                }
            }
        }
        rows
    }
}
