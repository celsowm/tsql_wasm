use super::super::virtual_table_def;
use super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::DataType;

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

/// Stub for sys.database_mirroring (always returns empty).
pub(crate) struct SysDatabaseMirroring;

impl VirtualTable for SysDatabaseMirroring {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "database_mirroring",
            vec![
                ("database_id", DataType::Int, false),
                ("mirroring_guid", DataType::UniqueIdentifier, true),
                ("mirroring_state", DataType::TinyInt, true),
                (
                    "mirroring_role",
                    DataType::TinyInt,
                    true,
                ),
                (
                    "mirroring_role_desc",
                    DataType::NVarChar { max_len: 60 },
                    true,
                ),
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
        vec![]
    }
}
