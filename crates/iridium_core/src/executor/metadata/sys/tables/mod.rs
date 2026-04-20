mod columns;
mod databases;
mod encryption;
mod identity_columns;
mod objects_misc;
mod tables;
mod types;

pub(crate) use columns::{SysAllColumns, SysColumns, SysComputedColumns, SysViewColumns};
pub(crate) use databases::{SysConfigurations, SysDatabases, SysSysDatabases};
pub(crate) use encryption::{
    SysColumnEncryptionKeyValues, SysColumnEncryptionKeys, SysColumnMasterKeys,
};
pub(crate) use identity_columns::SysIdentityColumns;
pub(crate) use objects_misc::{
    SysAssemblyModules, SysDataSpaces, SysEdgeConstraints, SysExtendedProperties,
    SysForeignKeyColumns, SysIndexColumns, SysInternalTables, SysPeriods, SysSequences,
    SysServerPrincipals, SysServerRoleMembers, SysSqlExpressionDependencies, SysSqlModules, SysAllSqlModules, SysStats, SysStatsColumns,
    SysSynonyms, SysSystemSqlModules, SysTriggerEvents, SysTriggers, SysXmlIndexes,
    SysXmlSchemaCollections,
};
pub(crate) use tables::{SysFileTables, SysTables};
pub(crate) use types::{SysAssemblyTypes, SysTableTypes, SysTypes};

use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysSchemas;

impl VirtualTable for SysSchemas {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "schemas",
            vec![
                ("schema_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("principal_id", DataType::Int, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        catalog
            .get_schemas()
            .iter()
            .map(|s| StoredRow {
                values: vec![
                    Value::Int(s.id as i32),
                    Value::VarChar(s.name.clone()),
                    Value::Null,
                ],
                deleted: false,
            })
            .collect()
    }
}
