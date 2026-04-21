#![allow(dead_code, unused_imports)]

use crate::ast::{
    AlterTableAction, AlterTableStmt, CreateIndexStmt, CreateSchemaStmt, CreateTableStmt,
    CreateTypeStmt, DropIndexStmt, DropSchemaStmt, DropTableStmt, DropTypeStmt,
    TableConstraintSpec,
};
use crate::catalog::{
    Catalog, CheckConstraintDef, ColumnDef, ForeignKeyDef, IdentityDef, TableDef, TableTypeDef,
};
use crate::error::DbError;
use crate::storage::Storage;

use super::tooling::format_view_definition;
use super::tooling::SessionOptions;
use super::type_mapping::data_type_spec_to_runtime;
use super::schema_parts;

/// S6: Shared constraint application logic extracted from create_table and alter_table.
/// Eliminates ~65 lines of duplicated constraint handling.
fn apply_table_constraint(table: &mut TableDef, tc: TableConstraintSpec) -> Result<(), DbError> {
    match tc {
        TableConstraintSpec::Default { name, column, expr } => {
            let col = table
                .columns
                .iter_mut()
                .find(|c| c.name.eq_ignore_ascii_case(&column))
                .ok_or_else(|| DbError::column_not_found(&column))?;
            col.default = Some(expr);
            col.default_constraint_name = Some(name);
        }
        TableConstraintSpec::Check { name, expr } => {
            table
                .check_constraints
                .push(CheckConstraintDef { name, expr });
        }
        TableConstraintSpec::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } => {
            table.foreign_keys.push(ForeignKeyDef {
                name,
                columns,
                referenced_table,
                referenced_columns,
                on_delete: on_delete.unwrap_or(crate::ast::ReferentialAction::NoAction),
                on_update: on_update.unwrap_or(crate::ast::ReferentialAction::NoAction),
            });
        }
        TableConstraintSpec::PrimaryKey {
            name: _,
            columns,
            is_clustered,
        } => {
            for col_spec in &columns {
                let col = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&col_spec.name))
                    .ok_or_else(|| DbError::column_not_found(&col_spec.name))?;
                col.primary_key = true;
                col.nullable = false;
                col.is_clustered = is_clustered;
            }
            if columns.len() == 1 {
                if let Some(col) = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&columns[0].name))
                {
                    col.unique = true;
                }
            }
        }
        TableConstraintSpec::Unique {
            name: _,
            columns,
            is_clustered,
        } => {
            for col_spec in &columns {
                let col = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&col_spec.name))
                    .ok_or_else(|| DbError::column_not_found(&col_spec.name))?;
                col.unique = true;
                col.is_clustered = is_clustered;
            }
        }
    }
    Ok(())
}

pub(crate) struct SchemaExecutor<'a> {
    pub(crate) catalog: &'a mut dyn Catalog,
    pub(crate) storage: &'a mut dyn Storage,
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

    fn build_column_def(&mut self, spec: crate::ast::ColumnSpec) -> Result<ColumnDef, DbError> {
        let data_type = data_type_spec_to_runtime(&spec.data_type);
        let nullable = if spec.nullable_explicit {
            spec.nullable
        } else {
            self.session_options.ansi_null_dflt_on
        };
        Ok(ColumnDef {
            id: self.catalog.alloc_column_id(),
            name: spec.name,
            data_type,
            nullable,
            primary_key: spec.primary_key,
            unique: spec.unique || spec.primary_key,
            identity: spec.identity.map(|(seed, inc)| IdentityDef::new(seed, inc)),
            default: spec.default,
            default_constraint_name: spec.default_constraint_name,
            check: spec.check,
            check_constraint_name: spec.check_constraint_name,
            computed_expr: spec.computed_expr,
            collation: spec.collation,
            is_clustered: spec.is_clustered,
            ansi_padding_on: self.session_options.ansi_padding,
        })
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
