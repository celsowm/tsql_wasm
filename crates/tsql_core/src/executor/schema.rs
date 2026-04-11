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
        TableConstraintSpec::PrimaryKey { name: _, columns } => {
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
        }
        TableConstraintSpec::Unique { name: _, columns } => {
            for col_name in &columns {
                let col = table
                    .columns
                    .iter_mut()
                    .find(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| DbError::column_not_found(col_name))?;
                col.unique = true;
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

        for tc in stmt.table_constraints {
            apply_table_constraint(&mut table, tc)?;
        }

        self.catalog.register_table(table.clone());
        self.storage.ensure_table(table_id)?;

        // Create clustered indexes for PRIMARY KEYs
        for col in &table.columns {
            if col.primary_key {
                let index_name = format!("PK__{}__{}", table.name, col.name);
                self.catalog
                    .create_index_with_options(
                        "dbo",
                        &index_name,
                        &table.schema_name,
                        &table.name,
                        std::slice::from_ref(&col.name),
                        true,       // is_clustered
                        col.unique, // is_unique
                    )
                    .map_err(|e| {
                        DbError::Execution(format!("Failed to create primary key index: {}", e))
                    })?;
            }
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
        let mut view = crate::catalog::ViewDef {
            object_id: self.catalog.alloc_object_id(),
            schema,
            name: view_name,
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
        )
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
