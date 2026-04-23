#![allow(dead_code, unused_imports)]

use crate::ast::{
    AlterTableStmt, CreateIndexStmt, CreateSchemaStmt, CreateTableStmt,
    CreateTypeStmt, DropIndexStmt, DropSchemaStmt, DropTableStmt, DropTypeStmt,
};
use crate::error::DbError;

use super::tooling::SessionOptions;
use super::schema_parts;

pub(crate) struct SchemaExecutor<'a> {
    pub(crate) catalog: &'a mut dyn crate::catalog::Catalog,
    pub(crate) storage: &'a mut dyn crate::storage::Storage,
    pub(crate) session_options: &'a SessionOptions,
}

impl<'a> SchemaExecutor<'a> {
    pub(crate) fn create_type(&mut self, stmt: CreateTypeStmt) -> Result<(), DbError> {
        schema_parts::create_type(self, stmt)
    }

    pub(crate) fn drop_type(&mut self, stmt: DropTypeStmt) -> Result<(), DbError> {
        schema_parts::drop_type(self, stmt)
    }

    pub(crate) fn create_table(&mut self, stmt: CreateTableStmt) -> Result<(), DbError> {
        schema_parts::create_table(self, stmt)
    }

    pub(crate) fn drop_table(&mut self, stmt: DropTableStmt) -> Result<(), DbError> {
        schema_parts::drop_table(self, stmt)
    }

    pub(crate) fn create_schema(&mut self, stmt: CreateSchemaStmt) -> Result<(), DbError> {
        schema_parts::create_schema(self, stmt)
    }

    pub(crate) fn drop_schema(&mut self, stmt: DropSchemaStmt) -> Result<(), DbError> {
        schema_parts::drop_schema(self, stmt)
    }

    pub(crate) fn create_view(&mut self, stmt: crate::ast::CreateViewStmt) -> Result<(), DbError> {
        schema_parts::create_view(self, stmt)
    }

    pub(crate) fn drop_view(&mut self, stmt: crate::ast::DropViewStmt) -> Result<(), DbError> {
        schema_parts::drop_view(self, stmt)
    }

    pub(crate) fn create_synonym(
        &mut self,
        stmt: crate::ast::CreateSynonymStmt,
    ) -> Result<(), DbError> {
        schema_parts::create_synonym(self, stmt)
    }

    pub(crate) fn drop_synonym(
        &mut self,
        stmt: crate::ast::DropSynonymStmt,
    ) -> Result<(), DbError> {
        schema_parts::drop_synonym(self, stmt)
    }

    pub(crate) fn create_sequence(
        &mut self,
        stmt: crate::ast::CreateSequenceStmt,
    ) -> Result<(), DbError> {
        schema_parts::create_sequence(self, stmt)
    }

    pub(crate) fn drop_sequence(
        &mut self,
        stmt: crate::ast::DropSequenceStmt,
    ) -> Result<(), DbError> {
        schema_parts::drop_sequence(self, stmt)
    }

    pub(crate) fn create_index(&mut self, stmt: CreateIndexStmt) -> Result<(), DbError> {
        schema_parts::create_index(self, stmt)
    }

    pub(crate) fn drop_index(&mut self, stmt: DropIndexStmt) -> Result<(), DbError> {
        schema_parts::drop_index(self, stmt)
    }

    pub(crate) fn alter_table(&mut self, stmt: AlterTableStmt) -> Result<(), DbError> {
        schema_parts::alter_table(self, stmt)
    }
}
