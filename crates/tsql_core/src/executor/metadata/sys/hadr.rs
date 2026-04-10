use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

/// Stub for sys.availability_replicas (always returns empty).
pub(crate) struct SysAvailabilityReplicas;

impl VirtualTable for SysAvailabilityReplicas {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "availability_replicas",
            vec![
                ("replica_id", DataType::UniqueIdentifier, true),
                ("group_id", DataType::UniqueIdentifier, true),
                (
                    "replica_server_name",
                    DataType::NVarChar { max_len: 256 },
                    true,
                ),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}

/// Stub for sys.availability_groups (always returns empty).
pub(crate) struct SysAvailabilityGroups;

impl VirtualTable for SysAvailabilityGroups {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "availability_groups",
            vec![
                ("group_id", DataType::UniqueIdentifier, true),
                ("name", DataType::NVarChar { max_len: 256 }, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}

/// Stub for sys.dm_hadr_database_replica_states (always returns empty).
pub(crate) struct SysDmHadrDatabaseReplicaStates;

impl VirtualTable for SysDmHadrDatabaseReplicaStates {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "dm_hadr_database_replica_states",
            vec![
                ("group_database_id", DataType::UniqueIdentifier, true),
                ("synchronization_state", DataType::TinyInt, true),
                ("is_local", DataType::Bit, true),
                ("group_id", DataType::UniqueIdentifier, true),
                ("database_id", DataType::Int, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        vec![]
    }
}

/// Stub for sys.master_files — returns one data file per database.
pub(crate) struct SysMasterFiles;

impl VirtualTable for SysMasterFiles {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "master_files",
            vec![
                ("database_id", DataType::Int, false),
                ("file_id", DataType::Int, false),
                ("type", DataType::TinyInt, false),
                ("type_desc", DataType::NVarChar { max_len: 60 }, false),
                ("name", DataType::NVarChar { max_len: 128 }, false),
                ("physical_name", DataType::NVarChar { max_len: 260 }, false),
                ("state", DataType::TinyInt, false),
                ("state_desc", DataType::NVarChar { max_len: 60 }, false),
                ("size", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        DATABASE_IDS
            .iter()
            .map(|&db_id| StoredRow {
                values: vec![
                    Value::Int(db_id),
                    Value::Int(1),
                    Value::TinyInt(0), // ROWS
                    Value::NVarChar("ROWS".to_string()),
                    Value::NVarChar(format!("db_{}", db_id)),
                    Value::NVarChar(format!("C:\\data\\db_{}.mdf", db_id)),
                    Value::TinyInt(0), // ONLINE
                    Value::NVarChar("ONLINE".to_string()),
                    Value::Int(1024),
                ],
                deleted: false,
            })
            .collect()
    }
}

/// Stub for sys.database_mirroring — one row per database, all mirroring
/// columns NULL (mirroring not configured).
pub(crate) struct SysDatabaseMirroring;

/// Database IDs that match sys.databases (master=1, tempdb=2, model=3, msdb=4, tsql_wasm=5).
const DATABASE_IDS: &[i32] = &[1, 2, 3, 4, 5];

impl VirtualTable for SysDatabaseMirroring {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "database_mirroring",
            vec![
                ("database_id", DataType::Int, false),
                ("mirroring_guid", DataType::UniqueIdentifier, true),
                ("mirroring_state", DataType::TinyInt, true),
                ("mirroring_role", DataType::TinyInt, true),
                ("mirroring_role_desc", DataType::NVarChar { max_len: 60 }, true),
                ("mirroring_state_desc", DataType::NVarChar { max_len: 60 }, true),
                ("mirroring_safety_level", DataType::TinyInt, true),
                ("mirroring_safety_level_desc", DataType::NVarChar { max_len: 60 }, true),
                ("mirroring_partner_name", DataType::NVarChar { max_len: 128 }, true),
                ("mirroring_partner_instance", DataType::NVarChar { max_len: 128 }, true),
                ("mirroring_witness_name", DataType::NVarChar { max_len: 128 }, true),
                ("mirroring_witness_state", DataType::TinyInt, true),
                ("mirroring_witness_state_desc", DataType::NVarChar { max_len: 60 }, true),
                ("mirroring_failover_lsn", DataType::BigInt, true),
                ("mirroring_connection_timeout", DataType::Int, true),
                ("mirroring_redo_queue", DataType::Int, true),
                ("mirroring_end_of_log_lsn", DataType::BigInt, true),
                ("mirroring_replication_lsn", DataType::BigInt, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        DATABASE_IDS
            .iter()
            .map(|&db_id| StoredRow {
                values: vec![
                    Value::Int(db_id),
                    Value::Null, // mirroring_guid
                    Value::Null, // mirroring_state
                    Value::Null, // mirroring_role
                    Value::Null, // mirroring_role_desc
                    Value::Null, // mirroring_state_desc
                    Value::Null, // mirroring_safety_level
                    Value::Null, // mirroring_safety_level_desc
                    Value::Null, // mirroring_partner_name
                    Value::Null, // mirroring_partner_instance
                    Value::Null, // mirroring_witness_name
                    Value::Null, // mirroring_witness_state
                    Value::Null, // mirroring_witness_state_desc
                    Value::Null, // mirroring_failover_lsn
                    Value::Null, // mirroring_connection_timeout
                    Value::Null, // mirroring_redo_queue
                    Value::Null, // mirroring_end_of_log_lsn
                    Value::Null, // mirroring_replication_lsn
                ],
                deleted: false,
            })
            .collect()
    }
}
