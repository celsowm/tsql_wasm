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
#[derive(Debug, Clone)]
struct ConstraintIndexSpec {
    constraint_name: String,
    columns: Vec<String>,
    is_primary_key: bool,
    is_clustered: bool,
    is_unique: bool,
}

fn generated_constraint_name(table_name: &str, columns: &[String], is_primary_key: bool) -> String {
    let prefix = if is_primary_key { "PK" } else { "UQ" };
    let suffix = if columns.is_empty() {
        "col".to_string()
    } else {
        columns.join("_")
    };
    format!("{}_{}_{}", prefix, table_name, suffix)
}

fn generated_index_name(table_name: &str, columns: &[String], is_primary_key: bool) -> String {
    let prefix = if is_primary_key { "PK" } else { "UQ" };
    let suffix = if columns.is_empty() {
        "col".to_string()
    } else {
        columns.join("__")
    };
    format!("{}__{}__{}", prefix, table_name, suffix)
}

/// S6: Shared constraint application logic extracted from create_table and alter_table.
/// Eliminates ~65 lines of duplicated constraint handling.
fn apply_table_constraint(
    table: &mut TableDef,
    tc: TableConstraintSpec,
) -> Result<Option<ConstraintIndexSpec>, DbError> {
    match tc {
        TableConstraintSpec::Default { name, column, expr } => {
            let col = table
                .columns
                .iter_mut()
                .find(|c| c.name.eq_ignore_ascii_case(&column))
                .ok_or_else(|| DbError::column_not_found(&column))?;
            col.default = Some(expr);
            col.default_constraint_name = Some(name);
            Ok(None)
        }
        TableConstraintSpec::Check { name, expr } => {
            table
                .check_constraints
                .push(CheckConstraintDef { name, expr });
            Ok(None)
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
            Ok(None)
        }
        TableConstraintSpec::PrimaryKey {
            name,
            columns,
            clustered,
        } => {
            for col_name in &columns {
                let col = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| DbError::column_not_found(col_name))?;
                col.primary_key = true;
                col.nullable = false;
            }
            if columns.len() == 1 {
                if let Some(col) = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&columns[0]))
                {
                    col.unique = true;
                }
            }
            let constraint_name = if name.is_empty() {
                generated_constraint_name(&table.name, &columns, true)
            } else {
                name
            };
            Ok(Some(ConstraintIndexSpec {
                constraint_name,
                columns,
                is_primary_key: true,
                is_clustered: clustered,
                is_unique: true,
            }))
        }
        TableConstraintSpec::Unique {
            name,
            columns,
            clustered,
        } => {
            for col_name in &columns {
                let col = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| DbError::column_not_found(col_name))?;
                col.unique = true;
            }
            let constraint_name = if name.is_empty() {
                generated_constraint_name(&table.name, &columns, false)
            } else {
                name
            };
            Ok(Some(ConstraintIndexSpec {
                constraint_name,
                columns,
                is_primary_key: false,
                is_clustered: clustered,
                is_unique: true,
            }))
        }
    }
}

pub(crate) struct SchemaExecutor<'a> {
    pub(crate) catalog: &'a mut dyn Catalog,
    pub(crate) storage: &'a mut dyn Storage,
    pub(crate) session_options: &'a SessionOptions,
}

impl<'a> SchemaExecutor<'a> {
    pub(crate) fn create_type(&mut self, stmt: CreateTypeStmt) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        if self.catalog.get_schema_id(&schema).is_none() {
            return Err(DbError::schema_not_found(schema));
        }
        let object_id = self.catalog.alloc_object_id();
        self.catalog.create_table_type(TableTypeDef {
            object_id,
            schema,
            name: stmt.name.name,
            columns: stmt.columns,
            table_constraints: stmt.table_constraints,
        })
    }

    pub(crate) fn drop_type(&mut self, stmt: DropTypeStmt) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        self.catalog.drop_table_type(&schema, &stmt.name.name)
    }

    pub(crate) fn create_table(&mut self, stmt: CreateTableStmt) -> Result<(), DbError> {
        let schema_name = stmt.name.schema_or_dbo().to_string();
        let schema_id = self
            .catalog
            .get_schema_id(&schema_name)
            .ok_or_else(|| DbError::schema_not_found(schema_name.clone()))?;

        if self
            .catalog
            .find_table(&schema_name, &stmt.name.name)
            .is_some()
        {
            return Err(DbError::duplicate_table(&schema_name, &stmt.name.name));
        }

        let table_id = self.catalog.alloc_table_id();
        let has_table_pk = stmt
            .table_constraints
            .iter()
            .any(|c| matches!(c, TableConstraintSpec::PrimaryKey { .. }));
        let has_table_unique = stmt
            .table_constraints
            .iter()
            .any(|c| matches!(c, TableConstraintSpec::Unique { .. }));

        let mut column_fks: Vec<(String, crate::ast::ForeignKeyRef)> = Vec::new();
        for spec in &stmt.columns {
            if let Some(fk) = &spec.foreign_key {
                column_fks.push((spec.name.clone(), fk.clone()));
            }
        }

        let columns = stmt
            .columns
            .into_iter()
            .map(|spec| self.build_column_def(spec))
            .collect::<Result<Vec<_>, _>>()?;

        let mut table = TableDef {
            id: table_id,
            schema_id,
            schema_name: schema_name.clone(),
            name: stmt.name.name,
            columns,
            check_constraints: vec![],
            foreign_keys: vec![],
        };

        for (col_name, fk_ref) in column_fks {
            table.foreign_keys.push(ForeignKeyDef {
                name: format!("FK__{}__{}", table.name, col_name),
                columns: vec![col_name],
                referenced_table: fk_ref.referenced_table,
                referenced_columns: fk_ref.referenced_columns,
                on_delete: fk_ref
                    .on_delete
                    .unwrap_or(crate::ast::ReferentialAction::NoAction),
                on_update: fk_ref
                    .on_update
                    .unwrap_or(crate::ast::ReferentialAction::NoAction),
            });
        }

        let mut constraint_index_specs = Vec::new();
        for tc in stmt.table_constraints {
            if let Some(spec) = apply_table_constraint(&mut table, tc)? {
                constraint_index_specs.push(spec);
            }
        }

        self.catalog.register_table(table.clone());
        self.storage.ensure_table(table_id)?;

        if !has_table_pk {
            for col in &table.columns {
                if col.primary_key {
                    constraint_index_specs.push(ConstraintIndexSpec {
                        constraint_name: generated_constraint_name(
                            &table.name,
                            std::slice::from_ref(&col.name),
                            true,
                        ),
                        columns: vec![col.name.clone()],
                        is_primary_key: true,
                        is_clustered: true,
                        is_unique: true,
                    });
                }
            }
        }

        if !has_table_unique {
            for col in &table.columns {
                if col.unique && !col.primary_key {
                    constraint_index_specs.push(ConstraintIndexSpec {
                        constraint_name: generated_constraint_name(
                            &table.name,
                            std::slice::from_ref(&col.name),
                            false,
                        ),
                        columns: vec![col.name.clone()],
                        is_primary_key: false,
                        is_clustered: false,
                        is_unique: true,
                    });
                }
            }
        }

        for spec in constraint_index_specs {
            let index_name = generated_index_name(&table.name, &spec.columns, spec.is_primary_key);
            self.catalog
                .create_index_with_options(
                    &table.schema_name,
                    &index_name,
                    &table.schema_name,
                    &table.name,
                    &spec.columns,
                    spec.is_clustered,
                    spec.is_unique,
                    spec.is_primary_key,
                    Some(spec.constraint_name),
                )
                .map_err(|e| {
                    DbError::Execution(format!("Failed to create constraint index: {}", e))
                })?;
        }

        Ok(())
    }

    pub(crate) fn drop_table(&mut self, stmt: DropTableStmt) -> Result<(), DbError> {
        let schema_name = stmt.name.schema_or_dbo().to_string();
        let table_id = self.catalog.drop_table(&schema_name, &stmt.name.name)?;
        self.storage.remove_table(table_id)?;
        Ok(())
    }

    pub(crate) fn create_schema(&mut self, stmt: CreateSchemaStmt) -> Result<(), DbError> {
        self.catalog.create_schema(&stmt.name)
    }

    pub(crate) fn drop_schema(&mut self, stmt: DropSchemaStmt) -> Result<(), DbError> {
        self.catalog.drop_schema(&stmt.name)
    }

    pub(crate) fn create_view(&mut self, stmt: crate::ast::CreateViewStmt) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        let view_name = stmt.name.name;
        let query = stmt.query;
        let schema_id = self.catalog.get_schema_id(&schema).unwrap_or(1);
        let mut view = crate::catalog::ViewDef {
            object_id: self.catalog.alloc_object_id(),
            schema,
            name: view_name,
            schema_id,
            query: crate::ast::Statement::Dml(crate::ast::DmlStatement::Select(query)),
            definition_sql: String::new(),
        };
        view.definition_sql = format_view_definition(&view);
        self.catalog.create_view(view)
    }

    pub(crate) fn drop_view(&mut self, stmt: crate::ast::DropViewStmt) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo();
        self.catalog.drop_view(schema, &stmt.name.name)
    }

    pub(crate) fn create_synonym(
        &mut self,
        stmt: crate::ast::CreateSynonymStmt,
    ) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        let object_id = self.catalog.alloc_object_id();
        self.catalog.create_synonym(crate::catalog::SynonymDef {
            object_id,
            schema,
            name: stmt.name.name,
            base_object: stmt.base_object,
        })
    }

    pub(crate) fn drop_synonym(
        &mut self,
        stmt: crate::ast::DropSynonymStmt,
    ) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo();
        self.catalog.drop_synonym(schema, &stmt.name.name)
    }

    pub(crate) fn create_sequence(
        &mut self,
        stmt: crate::ast::CreateSequenceStmt,
    ) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        let data_type = data_type_spec_to_runtime(&stmt.data_type);
        let object_id = self.catalog.alloc_object_id();
        self.catalog.create_sequence(crate::catalog::SequenceDef {
            object_id,
            schema,
            name: stmt.name.name,
            data_type,
            start_value: stmt.start_value,
            increment: stmt.increment,
            current_value: stmt.start_value,
            minimum_value: stmt.minimum_value,
            maximum_value: stmt.maximum_value,
            is_cycling: stmt.is_cycling,
        })
    }

    pub(crate) fn drop_sequence(
        &mut self,
        stmt: crate::ast::DropSequenceStmt,
    ) -> Result<(), DbError> {
        let schema = stmt.name.schema_or_dbo();
        self.catalog.drop_sequence(schema, &stmt.name.name)
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
            ansi_padding_on: self.session_options.ansi_padding,
        })
    }

    pub(crate) fn create_index(&mut self, stmt: CreateIndexStmt) -> Result<(), DbError> {
        let index_schema = stmt.name.schema_or_dbo().to_string();
        let table_schema = stmt.table.schema_or_dbo().to_string();

        self.catalog.create_index(
            &index_schema,
            &stmt.name.name,
            &table_schema,
            &stmt.table.name,
            &stmt.columns,
        )?;

        let index_id = self
            .catalog
            .get_indexes()
            .iter()
            .find(|idx| idx.name.eq_ignore_ascii_case(&stmt.name.name))
            .map(|idx| idx.id);

        if let (Some(index_def_id), Some(table)) = (
            index_id,
            self.catalog.find_table(&table_schema, &stmt.table.name),
        ) {
            let rows = self.storage.get_rows(table.id)?;
            let row_refs: Vec<(usize, &[crate::types::Value])> = rows
                .iter()
                .enumerate()
                .map(|(i, r)| (i, r.values.as_slice()))
                .collect();

            if let Some(index_storage) = self.storage.as_index_storage_mut() {
                index_storage.register_index(
                    index_def_id,
                    self.catalog
                        .get_indexes()
                        .iter()
                        .find(|idx| idx.id == index_def_id)
                        .map(|idx| idx.column_ids.clone())
                        .unwrap_or_default(),
                    self.catalog
                        .get_indexes()
                        .iter()
                        .find(|idx| idx.id == index_def_id)
                        .map(|idx| idx.is_unique)
                        .unwrap_or(false),
                    self.catalog
                        .get_indexes()
                        .iter()
                        .find(|idx| idx.id == index_def_id)
                        .map(|idx| idx.is_clustered)
                        .unwrap_or(false),
                );

                if let Some(idx) = index_storage.get_index_mut(index_def_id) {
                    let _ = idx.rebuild_from_rows(row_refs.as_slice());
                }
            }
        }

        Ok(())
    }

    pub(crate) fn drop_index(&mut self, stmt: DropIndexStmt) -> Result<(), DbError> {
        let index_schema = stmt.name.schema_or_dbo().to_string();
        let table_schema = stmt.table.schema_or_dbo().to_string();
        self.catalog.drop_index(
            &index_schema,
            &stmt.name.name,
            &table_schema,
            &stmt.table.name,
        )
    }

    pub(crate) fn alter_table(&mut self, stmt: AlterTableStmt) -> Result<(), DbError> {
        let schema_name = stmt.table.schema_or_dbo().to_string();
        let table_id = self
            .catalog
            .find_table(&schema_name, &stmt.table.name)
            .map(|t| t.id)
            .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;

        match stmt.action {
            AlterTableAction::AddColumn(col_spec) => {
                let col = self.build_column_def(col_spec)?;
                let table_mut = self
                    .catalog
                    .find_table_mut(&schema_name, &stmt.table.name)
                    .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;
                table_mut.columns.push(col);

                // Add NULL values for the new column in existing rows
                let mut rows_vec = {
                    let rows = match self.storage.scan_rows(table_id) {
                        Ok(rows) => rows,
                        Err(_) => return Ok(()),
                    };
                    match rows.collect::<Result<Vec<_>, DbError>>() {
                        Ok(rows_vec) => rows_vec,
                        Err(_) => return Ok(()),
                    }
                };
                for row in rows_vec.iter_mut() {
                    row.values.push(crate::types::Value::Null);
                }
                self.storage.replace_table(table_id, rows_vec)?;
            }
            AlterTableAction::DropColumn(col_name) => {
                let table_mut = self
                    .catalog
                    .find_table_mut(&schema_name, &stmt.table.name)
                    .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;
                let col_idx = table_mut
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&col_name))
                    .ok_or_else(|| DbError::column_not_found(&col_name))?;
                table_mut.columns.remove(col_idx);

                // Remove the column values from existing rows
                let mut rows_vec = {
                    let rows = match self.storage.scan_rows(table_id) {
                        Ok(rows) => rows,
                        Err(_) => return Ok(()),
                    };
                    match rows.collect::<Result<Vec<_>, DbError>>() {
                        Ok(rows_vec) => rows_vec,
                        Err(_) => return Ok(()),
                    }
                };
                for row in rows_vec.iter_mut() {
                    if col_idx < row.values.len() {
                        row.values.remove(col_idx);
                    }
                }
                self.storage.replace_table(table_id, rows_vec)?;
            }
            AlterTableAction::AlterColumn {
                name,
                data_type,
                nullable,
            } => {
                let table_mut = self
                    .catalog
                    .find_table_mut(&schema_name, &stmt.table.name)
                    .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;
                let col = table_mut
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(&name))
                    .ok_or_else(|| DbError::column_not_found(&name))?;
                col.data_type = data_type_spec_to_runtime(&data_type);
                if let Some(n) = nullable {
                    col.nullable = n;
                }
            }
            AlterTableAction::AddConstraint(constraint) => {
                let table_mut = self
                    .catalog
                    .find_table_mut(&schema_name, &stmt.table.name)
                    .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;
                apply_table_constraint(table_mut, constraint)?;
            }
            AlterTableAction::DropConstraint(constraint_name) => {
                let table_mut = self
                    .catalog
                    .find_table_mut(&schema_name, &stmt.table.name)
                    .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;

                // Try to remove from check constraints
                let removed = table_mut
                    .check_constraints
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&constraint_name));
                if let Some(pos) = removed {
                    table_mut.check_constraints.remove(pos);
                } else {
                    // Try to remove from foreign keys
                    let removed = table_mut
                        .foreign_keys
                        .iter()
                        .position(|fk| fk.name.eq_ignore_ascii_case(&constraint_name));
                    if let Some(pos) = removed {
                        table_mut.foreign_keys.remove(pos);
                    } else {
                        return Err(DbError::constraint_not_found(
                            &stmt.table.name,
                            constraint_name,
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}
