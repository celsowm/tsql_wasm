use super::super::super::virtual_table_def;
use super::super::super::VirtualTable;
use crate::catalog::Catalog;
use crate::storage::StoredRow;
use crate::types::{DataType, Value};

pub(crate) struct SysDataSpaces;
pub(crate) struct SysExtendedProperties;
pub(crate) struct SysIndexColumns;
pub(crate) struct SysForeignKeyColumns;
pub(crate) struct SysXmlSchemaCollections;
pub(crate) struct SysXmlIndexes;
pub(crate) struct SysEdgeConstraints;
pub(crate) struct SysAssemblyModules;
pub(crate) struct SysTriggers;
pub(crate) struct SysSqlModules;
pub(crate) struct SysSystemSqlModules;
pub(crate) struct SysStats;
pub(crate) struct SysServerPrincipals;

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

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
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

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
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
                ("is_included_column", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for idx in catalog.get_indexes() {
            for (ordinal, col_id) in idx.column_ids.iter().enumerate() {
                rows.push(StoredRow {
                    values: vec![
                        Value::Int(idx.table_id as i32),
                        Value::Int(idx.id as i32),
                        Value::Int((ordinal + 1) as i32),
                        Value::Int(*col_id as i32),
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
                ("parent_object_id", DataType::Int, false),
                ("parent_column_id", DataType::Int, false),
                ("referenced_object_id", DataType::Int, false),
                ("referenced_column_id", DataType::Int, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        let mut rows = Vec::new();
        for table in catalog.get_tables() {
            for fk in &table.foreign_keys {
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

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysXmlIndexes {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "xml_indexes",
            vec![
                ("object_id", DataType::Int, false),
                ("index_id", DataType::Int, false),
                ("xml_index_type", DataType::TinyInt, false),
                ("secondary_type", DataType::VarChar { max_len: 1 }, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
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

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
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

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
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

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
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
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysSystemSqlModules {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "system_sql_modules",
            vec![
                ("object_id", DataType::Int, false),
                ("definition", DataType::VarChar { max_len: 8000 }, true),
            ],
        )
    }

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        Vec::new()
    }
}

impl VirtualTable for SysStats {
    fn definition(&self) -> crate::catalog::TableDef {
        virtual_table_def(
            "stats",
            vec![
                ("object_id", DataType::Int, false),
                ("name", DataType::VarChar { max_len: 128 }, false),
                ("auto_created", DataType::Bit, false),
                ("has_filter", DataType::Bit, false),
            ],
        )
    }

    fn rows(&self, catalog: &dyn Catalog) -> Vec<StoredRow> {
        catalog
            .get_indexes()
            .iter()
            .map(|idx| StoredRow {
                values: vec![
                    Value::Int(idx.table_id as i32),
                    Value::VarChar(idx.name.clone()),
                    Value::Bit(false),
                    Value::Bit(false),
                ],
                deleted: false,
            })
            .collect()
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

    fn rows(&self, _catalog: &dyn Catalog) -> Vec<StoredRow> {
        let created = Value::DateTime(
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        );
        vec![StoredRow {
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
        }]
    }
}
