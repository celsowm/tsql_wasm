mod constraints;
mod database_principals;
mod hadr;
mod host_info;
mod indexes;
mod objects;
mod parameters;
mod partition;
mod policy_configuration;
mod routines;
mod tables;

use super::VirtualTable;

pub(crate) fn lookup(schema: &str, name: &str) -> Option<Box<dyn VirtualTable>> {
    if schema.eq_ignore_ascii_case("dbo") && name.eq_ignore_ascii_case("syspolicy_configuration") {
        Some(Box::new(policy_configuration::SysPolicyConfiguration))
    } else if schema.eq_ignore_ascii_case("dbo") && name.eq_ignore_ascii_case("sysobjects") {
        Some(Box::new(objects::SysCompatSysObjects))
    } else if !schema.eq_ignore_ascii_case("sys") {
        None
    } else if name.eq_ignore_ascii_case("schemas") {
        Some(Box::new(tables::SysSchemas))
    } else if name.eq_ignore_ascii_case("databases") {
        Some(Box::new(tables::SysDatabases))
    } else if name.eq_ignore_ascii_case("sysdatabases") {
        Some(Box::new(tables::SysSysDatabases))
    } else if name.eq_ignore_ascii_case("configurations") {
        Some(Box::new(tables::SysConfigurations))
    } else if name.eq_ignore_ascii_case("tables") {
        Some(Box::new(tables::SysTables))
    } else if name.eq_ignore_ascii_case("columns") {
        Some(Box::new(tables::SysColumns))
    } else if name.eq_ignore_ascii_case("all_columns") {
        Some(Box::new(tables::SysAllColumns))
    } else if name.eq_ignore_ascii_case("view_columns") {
        Some(Box::new(tables::SysViewColumns))
    } else if name.eq_ignore_ascii_case("data_spaces") {
        Some(Box::new(tables::SysDataSpaces))
    } else if name.eq_ignore_ascii_case("extended_properties") {
        Some(Box::new(tables::SysExtendedProperties))
    } else if name.eq_ignore_ascii_case("index_columns") {
        Some(Box::new(tables::SysIndexColumns))
    } else if name.eq_ignore_ascii_case("foreign_key_columns") {
        Some(Box::new(tables::SysForeignKeyColumns))
    } else if name.eq_ignore_ascii_case("xml_schema_collections") {
        Some(Box::new(tables::SysXmlSchemaCollections))
    } else if name.eq_ignore_ascii_case("xml_indexes") {
        Some(Box::new(tables::SysXmlIndexes))
    } else if name.eq_ignore_ascii_case("table_types") {
        Some(Box::new(tables::SysTableTypes))
    } else if name.eq_ignore_ascii_case("partition_functions") {
        Some(Box::new(partition::SysPartitionFunctions))
    } else if name.eq_ignore_ascii_case("partition_parameters") {
        Some(Box::new(partition::SysPartitionParameters))
    } else if name.eq_ignore_ascii_case("partition_schemes") {
        Some(Box::new(partition::SysPartitionSchemes))
    } else if name.eq_ignore_ascii_case("destination_data_spaces") {
        Some(Box::new(partition::SysDestinationDataSpaces))
    } else if name.eq_ignore_ascii_case("filegroups") {
        Some(Box::new(partition::SysFilegroups))
    } else if name.eq_ignore_ascii_case("edge_constraints") {
        Some(Box::new(tables::SysEdgeConstraints))
    } else if name.eq_ignore_ascii_case("assembly_modules") {
        Some(Box::new(tables::SysAssemblyModules))
    } else if name.eq_ignore_ascii_case("triggers") {
        Some(Box::new(tables::SysTriggers))
    } else if name.eq_ignore_ascii_case("sql_modules") {
        Some(Box::new(tables::SysSqlModules))
    } else if name.eq_ignore_ascii_case("system_sql_modules") {
        Some(Box::new(tables::SysSystemSqlModules))
    } else if name.eq_ignore_ascii_case("stats") {
        Some(Box::new(tables::SysStats))
    } else if name.eq_ignore_ascii_case("types") {
        Some(Box::new(tables::SysTypes))
    } else if name.eq_ignore_ascii_case("parameters") {
        Some(Box::new(parameters::SysParameters))
    } else if name.eq_ignore_ascii_case("procedures") {
        Some(Box::new(routines::SysProcedures))
    } else if name.eq_ignore_ascii_case("functions") {
        Some(Box::new(routines::SysFunctions))
    } else if name.eq_ignore_ascii_case("indexes") {
        Some(Box::new(indexes::SysIndexes))
    } else if name.eq_ignore_ascii_case("objects") {
        Some(Box::new(objects::SysObjects))
    } else if name.eq_ignore_ascii_case("sysobjects") {
        Some(Box::new(objects::SysCompatSysObjects))
    } else if name.eq_ignore_ascii_case("system_views") {
        Some(Box::new(objects::SysSystemViews))
    } else if name.eq_ignore_ascii_case("views") {
        Some(Box::new(objects::SysViews))
    } else if name.eq_ignore_ascii_case("dm_os_host_info") {
        Some(Box::new(host_info::SysHostInfo))
    } else if name.eq_ignore_ascii_case("check_constraints") {
        Some(Box::new(constraints::SysCheckConstraints))
    } else if name.eq_ignore_ascii_case("routines") {
        Some(Box::new(routines::SysRoutines))
    } else if name.eq_ignore_ascii_case("foreign_keys") {
        Some(Box::new(constraints::SysForeignKeys))
    } else if name.eq_ignore_ascii_case("key_constraints") {
        Some(Box::new(constraints::SysKeyConstraints))
    } else if name.eq_ignore_ascii_case("default_constraints") {
        Some(Box::new(constraints::SysDefaultConstraints))
    } else if name.eq_ignore_ascii_case("server_principals") {
        Some(Box::new(tables::SysServerPrincipals))
    } else if name.eq_ignore_ascii_case("availability_replicas") {
        Some(Box::new(hadr::SysAvailabilityReplicas))
    } else if name.eq_ignore_ascii_case("availability_groups") {
        Some(Box::new(hadr::SysAvailabilityGroups))
    } else if name.eq_ignore_ascii_case("dm_hadr_database_replica_states") {
        Some(Box::new(hadr::SysDmHadrDatabaseReplicaStates))
    } else if name.eq_ignore_ascii_case("database_mirroring") {
        Some(Box::new(hadr::SysDatabaseMirroring))
    } else if name.eq_ignore_ascii_case("master_files") {
        Some(Box::new(hadr::SysMasterFiles))
    } else if name.eq_ignore_ascii_case("database_principals") {
        Some(Box::new(database_principals::SysDatabasePrincipals))
    } else if name.eq_ignore_ascii_case("database_permissions") {
        Some(Box::new(database_principals::SysDatabasePermissions))
    } else if name.eq_ignore_ascii_case("database_role_members") {
        Some(Box::new(database_principals::SysDatabaseRoleMembers))
    } else {
        None
    }
}
