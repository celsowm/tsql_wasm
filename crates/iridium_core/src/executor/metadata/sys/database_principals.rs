use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysDatabasePrincipals;
pub(crate) struct SysDatabasePermissions;
pub(crate) struct SysDatabaseRoleMembers;

impl VirtualTable for SysDatabasePrincipals {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "database_principals",
            vec![
                ("principal_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                (
                    "default_schema_name",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );

        vec![
            StoredRow {
                values: vec![
                    Value::Int(1),
                    Value::VarChar("dbo".to_string()),
                    Value::Char("S".to_string()),
                    Value::VarChar("SQL_USER".to_string()),
                    Value::VarChar("dbo".to_string()),
                    created.clone(),
                    created.clone(),
                ],
                deleted: false,
            },
            StoredRow {
                values: vec![
                    Value::Int(2),
                    Value::VarChar("guest".to_string()),
                    Value::Char("S".to_string()),
                    Value::VarChar("SQL_USER".to_string()),
                    Value::Null,
                    created.clone(),
                    created.clone(),
                ],
                deleted: false,
            },
        ]
    }
}

impl VirtualTable for SysDatabasePermissions {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "database_permissions",
            vec![
                ("class", DataType::Int, false),
                ("major_id", DataType::Int, false),
                ("minor_id", DataType::Int, false),
                ("grantee_principal_id", DataType::Int, false),
                ("grantor_principal_id", DataType::Int, false),
                ("type", DataType::VarChar { max_len: 60 }, false),
                ("permission_name", DataType::VarChar { max_len: 128 }, false),
                ("state", DataType::Char { len: 2 }, false),
                ("state_desc", DataType::VarChar { max_len: 60 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}

impl VirtualTable for SysDatabaseRoleMembers {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "database_role_members",
            vec![
                ("role_principal_id", DataType::Int, false),
                ("member_principal_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}
