use super::super::super::virtual_table_def;
use super::super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::executor::context::ExecutionContext;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysDataSpaces;
pub(crate) struct SysExtendedProperties;
pub(crate) struct SysIndexColumns;
pub(crate) struct SysForeignKeyColumns;
pub(crate) struct SysXmlSchemaCollections;
pub(crate) struct SysPeriods;
pub(crate) struct SysXmlIndexes;
pub(crate) struct SysInternalTables;
pub(crate) struct SysEdgeConstraints;
pub(crate) struct SysAssemblyModules;
pub(crate) struct SysTriggers;
pub(crate) struct SysSqlModules;
pub(crate) struct SysSystemSqlModules;
pub(crate) struct SysStats;
pub(crate) struct SysStatsColumns;
pub(crate) struct SysServerPrincipals;
pub(crate) struct SysServerRoleMembers;
pub(crate) struct SysTriggerEvents;

impl VirtualTable for SysDataSpaces {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "data_spaces",
            vec![
                ("data_space_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_default", DataType::Bit, false),
                ("is_system", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![StoredRow {
            values: vec![
                Value::Int(1),
                Value::VarChar("PRIMARY".to_string()),
                Value::Char("FG".to_string()),
                Value::VarChar("ROWS_FILEGROUP".to_string()),
                Value::Bit(true),
                Value::Bit(false),
            ],
            deleted: false,
        }]
    }
}

impl VirtualTable for SysTriggerEvents {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "trigger_events",
            vec![
                ("object_id", DataType::Int, false),
                ("type", DataType::TinyInt, false),
                ("type_desc", DataType::NVarChar { max_len: 60 }, false),
                ("is_first", DataType::Bit, false),
                ("is_last", DataType::Bit, false),
                ("event_group_type", DataType::Int, true),
                (
                    "event_group_type_desc",
                    DataType::NVarChar { max_len: 60 },
                    true,
                ),
                ("is_trigger_event", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();

        for t in catalog.get_triggers() {
            for event in &t.events {
                let (ty, desc) = match event {
                    crate::ast::TriggerEvent::Insert => (1, "INSERT"),
                    crate::ast::TriggerEvent::Update => (2, "UPDATE"),
                    crate::ast::TriggerEvent::Delete => (3, "DELETE"),
                };

                rows.push(StoredRow {
                    values: vec![
                        Value::Int(t.object_id),
                        Value::TinyInt(ty),
                        Value::NVarChar(desc.to_string()),
                        Value::Bit(false),
                        Value::Bit(false),
                        Value::Null,
                        Value::Null,
                        Value::Bit(true),
                    ],
                    deleted: false,
                });
            }
        }

        rows
    }
}

impl VirtualTable for SysExtendedProperties {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "extended_properties",
            vec![
                ("major_id", DataType::Int, false),
                ("minor_id", DataType::Int, false),
                ("class", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysIndexColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "index_columns",
            vec![
                ("object_id", DataType::Int, false),
                ("index_id", DataType::Int, false),
                ("index_column_id", DataType::Int, false),
                ("column_id", DataType::Int, false),
                ("key_ordinal", DataType::TinyInt, false),
                ("partition_ordinal", DataType::Int, false),
                ("is_descending_key", DataType::Bit, false),
                ("is_included_column", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for idx in catalog.get_indexes() {
            for (ordinal, col_id) in idx.column_ids.iter().enumerate() {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(idx.table_id as i32),
                        Value::Int(idx.id as i32),
                        Value::Int((ordinal + 1) as i32),
                        Value::Int(*col_id as i32),
                        Value::TinyInt((ordinal + 1) as u8),
                        Value::Int(0),
                        Value::Bit(false),
                        Value::Bit(false),
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}

impl VirtualTable for SysForeignKeyColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "foreign_key_columns",
            vec![
                ("constraint_object_id", DataType::Int, false),
                ("constraint_column_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("parent_column_id", DataType::Int, false),
                ("referenced_object_id", DataType::Int, false),
                ("referenced_column_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        let mut fk_idx = 0;

        for table in catalog.get_tables() {
            for fk in &table.foreign_keys {
                let constraint_object_id = 4_000_000 + fk_idx;
                fk_idx += 1;

                let ref_schema = fk.referenced_table.schema_or_dbo();
                let Some(ref_table) = catalog.find_table(ref_schema, &fk.referenced_table.name)
                else {
                    continue;
                };
                for (i, parent_col_name) in fk.columns.iter().enumerate() {
                    let Some(parent_col) = table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(parent_col_name))
                    else {
                        continue;
                    };
                    let ref_col_name = fk.referenced_columns.get(i).unwrap_or(parent_col_name);
                    let Some(ref_col) = ref_table
                        .columns
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(ref_col_name))
                    else {
                        continue;
                    };
                    rows.push(StoredRow {
                        values: vec![
                            Value::Int(constraint_object_id),
                            Value::Int((i + 1) as i32),
                            Value::Int(table.id as i32),
                            Value::Int(parent_col.id as i32),
                            Value::Int(ref_table.id as i32),
                            Value::Int(ref_col.id as i32),
                        ],
                        deleted: false,
                    });
                }
            }
        }
        rows
    }
}

impl VirtualTable for SysXmlSchemaCollections {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "xml_schema_collections",
            vec![
                ("xml_collection_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysPeriods {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "periods",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("period_id", DataType::Int, false),
                ("object_id", DataType::Int, false),
                ("start_column_id", DataType::Int, false),
                ("end_column_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysXmlIndexes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "xml_indexes",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("index_id", DataType::Int, false),
                ("type", DataType::TinyInt, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("using_xml_index_id", DataType::Int, true),
                ("xml_index_type", DataType::TinyInt, false),
                ("secondary_type", DataType::VarChar { max_len: 1 }, true),
                ("fill_factor", DataType::TinyInt, false),
                ("is_padded", DataType::Bit, false),
                ("is_disabled", DataType::Bit, false),
                ("is_hypothetical", DataType::Bit, false),
                ("allow_row_locks", DataType::Bit, false),
                ("allow_page_locks", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysInternalTables {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "internal_tables",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("parent_id", DataType::Int, false),
                ("internal_type", DataType::TinyInt, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysEdgeConstraints {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "edge_constraints",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("parent_object_id", DataType::Int, false),
                ("create_date", DataType::DateTime, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysAssemblyModules {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "assembly_modules",
            vec![("object_id", DataType::Int, false)],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysTriggers {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "triggers",
            vec![
                ("name", DataType::NVarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("parent_class", DataType::TinyInt, false),
                (
                    "parent_class_desc",
                    DataType::NVarChar { max_len: 60 },
                    false,
                ),
                ("parent_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::NVarChar { max_len: 60 }, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_ms_shipped", DataType::Bit, false),
                ("is_disabled", DataType::Bit, false),
                ("is_not_for_replication", DataType::Bit, false),
                ("is_instead_of_trigger", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );

        catalog
            .get_triggers()
            .iter()
            .map(|t| {
                let parent_id = catalog
                    .object_id(&t.table_schema, &t.table_name)
                    .unwrap_or(0);
                StoredRow {
                    values: vec![
                        Value::NVarChar(t.name.clone()),
                        Value::Int(t.object_id),
                        Value::TinyInt(1), // OBJECT_OR_COLUMN
                        Value::NVarChar("OBJECT_OR_COLUMN".to_string()),
                        Value::Int(parent_id),
                        Value::Char("TR".to_string()),
                        Value::NVarChar("SQL_TRIGGER".to_string()),
                        created.clone(),
                        created.clone(),
                        Value::Bit(false), // is_ms_shipped
                        Value::Bit(false), // is_disabled
                        Value::Bit(false), // is_not_for_replication
                        Value::Bit(t.is_instead_of),
                    ],
                    deleted: false,
                }
            })
            .collect()
    }
}

impl VirtualTable for SysSqlModules {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "sql_modules",
            vec![
                ("object_id", DataType::Int, false),
                ("definition", DataType::VarChar { max_len: 8000 }, true),
                ("uses_ansi_nulls", DataType::Bit, true),
                ("uses_quoted_identifier", DataType::Bit, true),
                ("is_schema_bound", DataType::Bit, true),
                ("uses_database_collation", DataType::Bit, true),
                ("is_recompiled", DataType::Bit, true),
                ("null_on_null_input", DataType::Bit, true),
                ("execute_as_principal_id", DataType::Int, true),
                ("uses_native_compilation", DataType::Bit, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();

        for r in catalog.get_routines() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(r.object_id),
                    Value::VarChar(r.definition_sql.clone()),
                    Value::Bit(true),
                    Value::Bit(true),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Null,
                    Value::Bit(false),
                ],
                deleted: false,
            });
        }

        for v in catalog.get_views() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(v.object_id),
                    Value::VarChar(v.definition_sql.clone()),
                    Value::Bit(true),
                    Value::Bit(true),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Null,
                    Value::Bit(false),
                ],
                deleted: false,
            });
        }

        for t in catalog.get_triggers() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(t.object_id),
                    Value::VarChar(t.definition_sql.clone()),
                    Value::Bit(true),
                    Value::Bit(true),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Null,
                    Value::Bit(false),
                ],
                deleted: false,
            });
        }

        rows
    }
}

impl VirtualTable for SysSystemSqlModules {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "system_sql_modules",
            vec![
                ("object_id", DataType::Int, false),
                ("definition", DataType::VarChar { max_len: 8000 }, true),
                ("uses_ansi_nulls", DataType::Bit, true),
                ("uses_quoted_identifier", DataType::Bit, true),
                ("is_schema_bound", DataType::Bit, true),
                ("uses_database_collation", DataType::Bit, true),
                ("is_recompiled", DataType::Bit, true),
                ("null_on_null_input", DataType::Bit, true),
                ("execute_as_principal_id", DataType::Int, true),
                ("uses_native_compilation", DataType::Bit, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

pub(crate) struct SysAllSqlModules;

impl VirtualTable for SysAllSqlModules {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "all_sql_modules",
            vec![
                ("object_id", DataType::Int, false),
                ("definition", DataType::VarChar { max_len: 8000 }, true),
                ("uses_ansi_nulls", DataType::Bit, true),
                ("uses_quoted_identifier", DataType::Bit, true),
                ("is_schema_bound", DataType::Bit, true),
                ("uses_database_collation", DataType::Bit, true),
                ("is_recompiled", DataType::Bit, true),
                ("null_on_null_input", DataType::Bit, true),
                ("execute_as_principal_id", DataType::Int, true),
                ("uses_native_compilation", DataType::Bit, true),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = SysSqlModules.rows(catalog, ctx);
        rows.extend(SysSystemSqlModules.rows(catalog, ctx));
        rows
    }
}

impl VirtualTable for SysStats {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "stats",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("stats_id", DataType::Int, false),
                ("auto_created", DataType::Bit, false),
                ("user_created", DataType::Bit, false),
                ("no_recompute", DataType::Bit, false),
                ("has_filter", DataType::Bit, false),
                ("filter_definition", DataType::NVarChar { max_len: 4000 }, true),
                ("is_temporary", DataType::Bit, false),
                ("is_incremental", DataType::Bit, false),
                ("is_ms_shipped", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for idx in catalog.get_indexes() {
            rows.push(StoredRow {
                values: vec![
                    Value::Int(idx.table_id as i32),
                    Value::VarChar(idx.name.clone()),
                    Value::Int(idx.id as i32),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Null,
                    Value::Bit(false),
                    Value::Bit(false),
                    Value::Bit(false),
                ],
                deleted: false,
            });
        }
        rows
    }
}

impl VirtualTable for SysStatsColumns {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "stats_columns",
            vec![
                ("object_id", DataType::Int, false),
                ("stats_id", DataType::Int, false),
                ("stats_column_id", DataType::Int, false),
                ("column_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for idx in catalog.get_indexes() {
            for (i, col_id) in idx.column_ids.iter().enumerate() {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(idx.table_id as i32),
                        Value::Int(idx.id as i32),
                        Value::Int((i + 1) as i32),
                        Value::Int(*col_id as i32),
                    ],
                    deleted: false,
                });
            }
        }
        rows
    }
}

impl VirtualTable for SysServerPrincipals {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "server_principals",
            vec![
                ("principal_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("type", DataType::Char { len: 1 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("is_disabled", DataType::Bit, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                (
                    "default_database_name",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
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
                    Value::VarChar("sa".to_string()),
                    Value::Char("S".to_string()),
                    Value::VarChar("SQL_LOGIN".to_string()),
                    Value::Bit(false),
                    created.clone(),
                    created.clone(),
                    Value::VarChar("master".to_string()),
                ],
                deleted: false,
            },
            StoredRow {
                values: vec![
                    Value::Int(3),
                    Value::VarChar("sysadmin".to_string()),
                    Value::Char("R".to_string()),
                    Value::VarChar("SERVER_ROLE".to_string()),
                    Value::Bit(false),
                    created.clone(),
                    created.clone(),
                    Value::Null,
                ],
                deleted: false,
            },
        ]
    }
}

impl VirtualTable for SysServerRoleMembers {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "server_role_members",
            vec![
                ("role_principal_id", DataType::Int, false),
                ("member_principal_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        vec![StoredRow {
            values: vec![Value::Int(3), Value::Int(1)],
            deleted: false,
        }]
    }
}

/// Stub for sys.sql_expression_dependencies — intentionally empty until
/// cross-object dependency tracking is implemented.
pub(crate) struct SysSqlExpressionDependencies;
pub(crate) struct SysSynonyms;
pub(crate) struct SysSequences;

impl VirtualTable for SysSqlExpressionDependencies {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "sql_expression_dependencies",
            vec![
                ("referencing_id", DataType::Int, false),
                ("referencing_class", DataType::TinyInt, false),
                (
                    "referencing_class_desc",
                    DataType::VarChar { max_len: 60 },
                    false,
                ),
                ("referencing_name", DataType::VarChar { max_len: 128 }, true),
                (
                    "referencing_schema",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                (
                    "referencing_database",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("referenced_id", DataType::Int, true),
                ("referenced_class", DataType::TinyInt, false),
                (
                    "referenced_class_desc",
                    DataType::VarChar { max_len: 60 },
                    false,
                ),
                ("referenced_name", DataType::VarChar { max_len: 128 }, true),
                (
                    "referenced_schema",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                (
                    "referenced_database",
                    DataType::VarChar { max_len: 128 },
                    true,
                ),
                ("is_schema_bound", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysSynonyms {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "synonyms",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_ms_shipped", DataType::Bit, false),
                ("is_published", DataType::Bit, false),
                ("is_schema_published", DataType::Bit, false),
                (
                    "base_object_name",
                    DataType::NVarChar { max_len: 1035 },
                    true,
                ),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );

        catalog
            .get_synonyms()
            .iter()
            .map(|s| {
                let base_name = format!("{}.{}", s.base_object.schema_or_dbo(), s.base_object.name);
                StoredRow {
                    values: vec![
                        Value::VarChar(s.name.clone()),
                        Value::Int(s.object_id),
                        Value::Int(catalog.get_schema_id(&s.schema).unwrap_or(1) as i32),
                        Value::Int(0),
                        Value::Char("SN".to_string()),
                        Value::VarChar("SYNONYM".to_string()),
                        created.clone(),
                        created.clone(),
                        Value::Bit(false),
                        Value::Bit(false),
                        Value::Bit(false),
                        Value::NVarChar(base_name),
                    ],
                    deleted: false,
                }
            })
            .collect()
    }
}

impl VirtualTable for SysSequences {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "sequences",
            vec![
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("object_id", DataType::Int, false),
                ("schema_id", DataType::Int, false),
                ("parent_object_id", DataType::Int, false),
                ("type", DataType::Char { len: 2 }, false),
                ("type_desc", DataType::VarChar { max_len: 60 }, false),
                ("create_date", DataType::DateTime, false),
                ("modify_date", DataType::DateTime, false),
                ("is_ms_shipped", DataType::Bit, false),
                ("is_published", DataType::Bit, false),
                ("is_schema_published", DataType::Bit, false),
                ("start_value", DataType::SqlVariant, false),
                ("increment", DataType::SqlVariant, false),
                ("minimum_value", DataType::SqlVariant, false),
                ("maximum_value", DataType::SqlVariant, false),
                ("is_cycling", DataType::Bit, false),
                ("is_cached", DataType::Bit, false),
                ("cache_size", DataType::Int, true),
                ("system_type_id", DataType::TinyInt, false),
                ("user_type_id", DataType::Int, false),
                ("precision", DataType::TinyInt, false),
                ("scale", DataType::TinyInt, false),
                ("current_value", DataType::SqlVariant, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog, _ctx: &ExecutionContext) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );

        catalog
            .get_sequences()
            .iter()
            .map(|s| {
                StoredRow {
                    values: vec![
                        Value::VarChar(s.name.clone()),
                        Value::Int(s.object_id),
                        Value::Int(catalog.get_schema_id(&s.schema).unwrap_or(1) as i32),
                        Value::Int(0),
                        Value::Char("SO".to_string()),
                        Value::VarChar("SEQUENCE_OBJECT".to_string()),
                        created.clone(),
                        created.clone(),
                        Value::Bit(false),
                        Value::Bit(false),
                        Value::Bit(false),
                        Value::BigInt(s.start_value),
                        Value::BigInt(s.increment),
                        Value::BigInt(s.minimum_value),
                        Value::BigInt(s.maximum_value),
                        Value::Bit(s.is_cycling),
                        Value::Bit(false),
                        Value::Null,
                        Value::TinyInt(127), // BIGINT
                        Value::Int(127),
                        Value::TinyInt(19),
                        Value::TinyInt(0),
                        Value::BigInt(s.current_value),
                    ],
                    deleted: false,
                }
            })
            .collect()
    }
}
