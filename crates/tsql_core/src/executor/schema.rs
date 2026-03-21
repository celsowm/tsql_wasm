use crate::ast::{CreateSchemaStmt, CreateTableStmt, DropSchemaStmt, DropTableStmt};
use crate::catalog::{Catalog, ColumnDef, IdentityDef, TableDef};
use crate::error::DbError;
use crate::storage::InMemoryStorage;

use super::type_mapping::data_type_spec_to_runtime;

pub(crate) struct SchemaExecutor<'a> {
    pub(crate) catalog: &'a mut Catalog,
    pub(crate) storage: &'a mut InMemoryStorage,
}

impl<'a> SchemaExecutor<'a> {
    pub(crate) fn create_table(&mut self, stmt: CreateTableStmt) -> Result<(), DbError> {
        let schema_name = stmt.name.schema_or_dbo().to_string();
        let schema_id = self
            .catalog
            .get_schema_id(&schema_name)
            .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema_name)))?;

        if self
            .catalog
            .find_table(&schema_name, &stmt.name.name)
            .is_some()
        {
            return Err(DbError::Semantic(format!(
                "table '{}.{}' already exists",
                schema_name, stmt.name.name
            )));
        }

        let table_id = self.catalog.alloc_table_id();
        let columns = stmt
            .columns
            .into_iter()
            .map(|spec| self.build_column_def(spec))
            .collect::<Result<Vec<_>, _>>()?;

        let table = TableDef {
            id: table_id,
            schema_id,
            name: stmt.name.name,
            columns,
        };

        self.catalog.tables.push(table);
        self.storage.tables.insert(table_id, Vec::new());
        Ok(())
    }

    pub(crate) fn drop_table(&mut self, stmt: DropTableStmt) -> Result<(), DbError> {
        let schema_name = stmt.name.schema_or_dbo().to_string();
        let table_id = self.catalog.drop_table(&schema_name, &stmt.name.name)?;
        self.storage.tables.remove(&table_id);
        Ok(())
    }

    pub(crate) fn create_schema(&mut self, stmt: CreateSchemaStmt) -> Result<(), DbError> {
        self.catalog.create_schema(&stmt.name)
    }

    pub(crate) fn drop_schema(&mut self, stmt: DropSchemaStmt) -> Result<(), DbError> {
        self.catalog.drop_schema(&stmt.name)
    }

    fn build_column_def(&mut self, spec: crate::ast::ColumnSpec) -> Result<ColumnDef, DbError> {
        let data_type = data_type_spec_to_runtime(&spec.data_type);
        Ok(ColumnDef {
            id: self.catalog.alloc_column_id(),
            name: spec.name,
            data_type,
            nullable: spec.nullable,
            primary_key: spec.primary_key,
            unique: spec.unique || spec.primary_key,
            identity: spec.identity.map(|(seed, inc)| IdentityDef::new(seed, inc)),
            default: spec.default,
        })
    }
}
