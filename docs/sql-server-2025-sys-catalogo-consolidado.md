# SQL Server 2025 — `sys.*` (catálogo consolidado em Markdown)

Fonte base: documentação oficial da Microsoft Learn / repositório oficial `MicrosoftDocs/sql-docs`, consultada em 2026-04-18.

> Escopo deste arquivo: objetos `sys.*` documentados nas páginas de **system catalog views**.
>
> Incluí explicitamente o par pedido:
> - `sys.change_tracking_databases`
> - `sys.change_tracking_tables`
>
> Também consolidei famílias correlatas que aparecem nas páginas oficiais por categoria.
>
> Não entram aqui:
> - `sys.dm_*` (DMVs/DMFs),
> - `INFORMATION_SCHEMA.*`,
> - compat views,
> - system base tables.

## Resumo

- Famílias / seções: 14
- Objetos `sys.*` únicos consolidados neste arquivo: 194


## Hub e pares solicitados (7)

```text
sys.change_tracking_databases
sys.change_tracking_tables
sys.database_mirroring_witnesses
sys.extended_properties
sys.messages
sys.remote_data_archive_databases
sys.schemas
```

## Object catalog views (49)

```text
sys.objects
sys.tables
sys.views
sys.procedures
sys.numbered_procedures
sys.numbered_procedure_parameters
sys.table_types
sys.synonyms
sys.sequences
sys.columns
sys.computed_columns
sys.identity_columns
sys.masked_columns
sys.parameters
sys.function_order_columns
sys.check_constraints
sys.default_constraints
sys.key_constraints
sys.foreign_keys
sys.foreign_key_columns
sys.index_columns
sys.hash_indexes
sys.stats
sys.stats_columns
sys.partitions
sys.allocation_units
sys.sql_modules
sys.assembly_modules
sys.sql_expression_dependencies
sys.sql_dependencies
sys.triggers
sys.trigger_events
sys.trigger_event_types
sys.event_notifications
sys.events
sys.service_queues
sys.periods
sys.memory_optimized_tables_internal_attributes
sys.extended_procedures
sys.all_columns
sys.all_views
sys.all_parameters
sys.all_sql_modules
sys.assembly_types
sys.system_columns
sys.system_parameters
sys.system_sql_modules
sys.system_views
sys.types
```

## Always On Availability Groups (7)

```text
sys.availability_databases_cluster
sys.availability_group_listener_ip_addresses
sys.availability_group_listeners
sys.availability_groups
sys.availability_groups_cluster
sys.availability_read_only_routing_lists
sys.availability_replicas
```

## Databases and files (9)

```text
sys.backup_devices
sys.database_connection_stats
sys.databases
sys.database_files
sys.database_mirroring
sys.database_recovery_status
sys.database_scoped_configurations
sys.master_files
sys.filegroups
```

## Security, audit e ledger (37)

```text
sys.database_permissions
sys.database_scoped_credentials
sys.database_principals
sys.master_key_passwords
sys.database_role_members
sys.user_token
sys.credentials
sys.server_principals
sys.login_token
sys.server_role_members
sys.securable_classes
sys.sql_logins
sys.server_permissions
sys.system_components_surface_area_configuration
sys.asymmetric_keys
sys.cryptographic_providers
sys.certificates
sys.key_encryptions
sys.column_encryption_key_values
sys.openkeys
sys.column_encryption_keys
sys.security_policies
sys.column_master_keys
sys.security_predicates
sys.crypt_properties
sys.symmetric_keys
sys.server_audits
sys.server_file_audits
sys.server_audit_specifications
sys.server_audit_specification_details
sys.database_audit_specifications
sys.database_audit_specification_details
sys.database_ledger_transactions
sys.database_ledger_blocks
sys.ledger_table_history
sys.ledger_column_history
sys.database_ledger_digest_locations
```

## Service Broker (15)

```text
sys.conversation_endpoints
sys.conversation_groups
sys.conversation_priorities
sys.message_type_xml_schema_collection_usages
sys.remote_service_bindings
sys.routes
sys.service_contract_message_usages
sys.service_contract_usages
sys.service_contracts
sys.service_message_types
sys.service_queue_usages
sys.service_queues
sys.services
sys.transmission_queue
sys.service_broker_endpoints
```

## Linked servers e endpoints (9)

```text
sys.linked_logins
sys.remote_logins
sys.servers
sys.database_mirroring_endpoints
sys.endpoints
sys.endpoint_webmethods
sys.http_endpoints
sys.soap_endpoints
sys.tcp_endpoints
```

## Server-wide configuration e trace (8)

```text
sys.configurations
sys.time_zone_info
sys.trace_categories
sys.trace_columns
sys.trace_event_bindings
sys.trace_events
sys.trace_subclass_values
sys.traces
```

## Query Store (11)

```text
sys.database_query_store_options
sys.query_context_settings
sys.query_store_plan
sys.query_store_query
sys.query_store_query_text
sys.query_store_wait_stats
sys.query_store_runtime_stats
sys.query_store_runtime_stats_interval
sys.query_store_query_hints
sys.database_query_store_internal_state
sys.query_store_replicas
```

## Full-Text e Semantic Search (14)

```text
sys.fulltext_catalogs
sys.fulltext_document_types
sys.fulltext_index_catalog_usages
sys.fulltext_index_columns
sys.fulltext_index_fragments
sys.fulltext_indexes
sys.fulltext_languages
sys.fulltext_stoplists
sys.fulltext_stopwords
sys.fulltext_system_stopwords
sys.registered_search_properties
sys.registered_search_property_lists
sys.fulltext_semantic_language_statistics_database
sys.fulltext_semantic_languages
```

## Resource Governor (4)

```text
sys.resource_governor_configuration
sys.resource_governor_external_resource_pools
sys.resource_governor_resource_pools
sys.resource_governor_workload_groups
```

## Extended Events, external, JSON e outros (16)

```text
sys.database_event_session_events
sys.database_event_session_fields
sys.database_event_session_targets
sys.database_service_objectives
sys.event_log
sys.external_language_files
sys.external_languages
sys.external_tables
sys.firewall_rules
sys.index_resumable_operations
sys.json_indexes
sys.spatial_indexes
sys.server_trigger_events
sys.type_assembly_usages
sys.xml_schema_wildcards
sys.workload_management_workload_classifier_details
```

## PDW / Synapse / APS específicos documentados no mesmo diretório (7)

```text
sys.pdw_database_mappings
sys.pdw_health_components
sys.pdw_health_component_status_mappings
sys.pdw_materialized_view_column_distribution_properties
sys.pdw_materialized_view_distribution_properties
sys.pdw_materialized_view_mappings
sys.pdw_nodes_column_store_row_groups
```

## Columnstore e correlatos (3)

```text
sys.column_store_row_groups
sys.column_store_segments
sys.index_resumable_operations
```

## Lista única consolidada (194)

```text
sys.all_columns
sys.all_parameters
sys.all_sql_modules
sys.all_views
sys.allocation_units
sys.assembly_modules
sys.assembly_types
sys.asymmetric_keys
sys.availability_databases_cluster
sys.availability_group_listener_ip_addresses
sys.availability_group_listeners
sys.availability_groups
sys.availability_groups_cluster
sys.availability_read_only_routing_lists
sys.availability_replicas
sys.backup_devices
sys.certificates
sys.change_tracking_databases
sys.change_tracking_tables
sys.check_constraints
sys.column_encryption_key_values
sys.column_encryption_keys
sys.column_master_keys
sys.column_store_row_groups
sys.column_store_segments
sys.columns
sys.computed_columns
sys.configurations
sys.conversation_endpoints
sys.conversation_groups
sys.conversation_priorities
sys.credentials
sys.crypt_properties
sys.cryptographic_providers
sys.database_audit_specification_details
sys.database_audit_specifications
sys.database_connection_stats
sys.database_event_session_events
sys.database_event_session_fields
sys.database_event_session_targets
sys.database_files
sys.database_ledger_blocks
sys.database_ledger_digest_locations
sys.database_ledger_transactions
sys.database_mirroring
sys.database_mirroring_endpoints
sys.database_mirroring_witnesses
sys.database_permissions
sys.database_principals
sys.database_query_store_internal_state
sys.database_query_store_options
sys.database_recovery_status
sys.database_role_members
sys.database_scoped_configurations
sys.database_scoped_credentials
sys.database_service_objectives
sys.databases
sys.default_constraints
sys.endpoint_webmethods
sys.endpoints
sys.event_log
sys.event_notifications
sys.events
sys.extended_procedures
sys.extended_properties
sys.external_language_files
sys.external_languages
sys.external_tables
sys.filegroups
sys.firewall_rules
sys.foreign_key_columns
sys.foreign_keys
sys.fulltext_catalogs
sys.fulltext_document_types
sys.fulltext_index_catalog_usages
sys.fulltext_index_columns
sys.fulltext_index_fragments
sys.fulltext_indexes
sys.fulltext_languages
sys.fulltext_semantic_language_statistics_database
sys.fulltext_semantic_languages
sys.fulltext_stoplists
sys.fulltext_stopwords
sys.fulltext_system_stopwords
sys.function_order_columns
sys.hash_indexes
sys.http_endpoints
sys.identity_columns
sys.index_columns
sys.index_resumable_operations
sys.json_indexes
sys.key_constraints
sys.key_encryptions
sys.ledger_column_history
sys.ledger_table_history
sys.linked_logins
sys.login_token
sys.masked_columns
sys.master_files
sys.master_key_passwords
sys.memory_optimized_tables_internal_attributes
sys.message_type_xml_schema_collection_usages
sys.messages
sys.numbered_procedure_parameters
sys.numbered_procedures
sys.objects
sys.openkeys
sys.parameters
sys.partitions
sys.pdw_database_mappings
sys.pdw_health_component_status_mappings
sys.pdw_health_components
sys.pdw_materialized_view_column_distribution_properties
sys.pdw_materialized_view_distribution_properties
sys.pdw_materialized_view_mappings
sys.pdw_nodes_column_store_row_groups
sys.periods
sys.procedures
sys.query_context_settings
sys.query_store_plan
sys.query_store_query
sys.query_store_query_hints
sys.query_store_query_text
sys.query_store_replicas
sys.query_store_runtime_stats
sys.query_store_runtime_stats_interval
sys.query_store_wait_stats
sys.registered_search_properties
sys.registered_search_property_lists
sys.remote_data_archive_databases
sys.remote_logins
sys.remote_service_bindings
sys.resource_governor_configuration
sys.resource_governor_external_resource_pools
sys.resource_governor_resource_pools
sys.resource_governor_workload_groups
sys.routes
sys.schemas
sys.securable_classes
sys.security_policies
sys.security_predicates
sys.sequences
sys.server_audit_specification_details
sys.server_audit_specifications
sys.server_audits
sys.server_file_audits
sys.server_permissions
sys.server_principals
sys.server_role_members
sys.server_trigger_events
sys.servers
sys.service_broker_endpoints
sys.service_contract_message_usages
sys.service_contract_usages
sys.service_contracts
sys.service_message_types
sys.service_queue_usages
sys.service_queues
sys.services
sys.soap_endpoints
sys.spatial_indexes
sys.sql_dependencies
sys.sql_expression_dependencies
sys.sql_logins
sys.sql_modules
sys.stats
sys.stats_columns
sys.symmetric_keys
sys.synonyms
sys.system_columns
sys.system_components_surface_area_configuration
sys.system_parameters
sys.system_sql_modules
sys.system_views
sys.table_types
sys.tables
sys.tcp_endpoints
sys.time_zone_info
sys.trace_categories
sys.trace_columns
sys.trace_event_bindings
sys.trace_events
sys.trace_subclass_values
sys.traces
sys.transmission_queue
sys.trigger_event_types
sys.trigger_events
sys.triggers
sys.type_assembly_usages
sys.types
sys.user_token
sys.views
sys.workload_management_workload_classifier_details
sys.xml_schema_wildcards
```
