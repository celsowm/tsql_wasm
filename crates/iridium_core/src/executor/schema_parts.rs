use crate::ast::{
    AlterTableAction, AlterTableStmt, CreateIndexStmt, CreateSchemaStmt, CreateTableStmt,
    CreateTypeStmt, DropIndexStmt, DropSchemaStmt, DropTableStmt, DropTypeStmt,
    TableConstraintSpec,
};
use crate::catalog::{
    CheckConstraintDef, ColumnDef, ForeignKeyDef, IdentityDef, TableDef, TableTypeDef,
};
use crate::error::DbError;

use super::tooling::format_view_definition;
use super::schema_physical;
use super::type_mapping::data_type_spec_to_runtime;
use super::schema::SchemaExecutor;

pub(crate) fn apply_table_constraint(table: &mut TableDef, tc: TableConstraintSpec) -> Result<(), DbError> {
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

fn build_column_def(
    executor: &mut SchemaExecutor<'_>,
    spec: crate::ast::ColumnSpec,
) -> Result<ColumnDef, DbError> {
    let data_type = data_type_spec_to_runtime(&spec.data_type);
    let nullable = if spec.nullable_explicit {
        spec.nullable
    } else {
        executor.session_options.ansi_null_dflt_on
    };
    Ok(ColumnDef {
        id: executor.catalog.alloc_column_id(),
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
        ansi_padding_on: executor.session_options.ansi_padding,
    })
}

pub(crate) fn create_type(executor: &mut SchemaExecutor<'_>, stmt: CreateTypeStmt) -> Result<(), DbError> {
    let schema = stmt.name.schema_or_dbo().to_string();
    if executor.catalog.get_schema_id(&schema).is_none() {
        return Err(DbError::schema_not_found(schema));
    }
    let object_id = executor.catalog.alloc_object_id();
    executor.catalog.create_table_type(TableTypeDef {
        object_id,
        schema,
        name: stmt.name.name,
        columns: stmt.columns,
        table_constraints: stmt.table_constraints,
    })
}

pub(crate) fn drop_type(executor: &mut SchemaExecutor<'_>, stmt: DropTypeStmt) -> Result<(), DbError> {
    let schema = stmt.name.schema_or_dbo().to_string();
    executor.catalog.drop_table_type(&schema, &stmt.name.name)
}

pub(crate) fn create_table(executor: &mut SchemaExecutor<'_>, stmt: CreateTableStmt) -> Result<(), DbError> {
    let schema_name = stmt.name.schema_or_dbo().to_string();
    let schema_id = executor
        .catalog
        .get_schema_id(&schema_name)
        .ok_or_else(|| DbError::schema_not_found(schema_name.clone()))?;

    if executor
        .catalog
        .find_table(&schema_name, &stmt.name.name)
        .is_some()
    {
        return Err(DbError::duplicate_table(&schema_name, &stmt.name.name));
    }

    let table_id = executor.catalog.alloc_table_id();

    let mut column_fks: Vec<(String, crate::ast::ForeignKeyRef)> = Vec::new();
    for spec in &stmt.columns {
        if let Some(fk) = &spec.foreign_key {
            column_fks.push((spec.name.clone(), fk.clone()));
        }
    }

    let columns = stmt
        .columns
        .into_iter()
        .map(|spec| build_column_def(executor, spec))
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

    executor.catalog.register_table(table.clone());
    executor.storage.ensure_table(table_id)?;

    for col in &table.columns {
        if col.primary_key || (col.unique && col.is_clustered) {
            let index_name = if col.primary_key {
                format!("PK__{}__{}", table.name, col.name)
            } else {
                format!("UQ__{}__{}", table.name, col.name)
            };
            executor
                .catalog
                .create_index_with_options(
                    "dbo",
                    &index_name,
                    &table.schema_name,
                    &table.name,
                    std::slice::from_ref(&col.name),
                    col.is_clustered,
                    col.unique || col.primary_key,
                )
                .map_err(|e| {
                    DbError::Execution(format!("Failed to create constraint index: {}", e))
                })?;
        }
    }

    Ok(())
}

pub(crate) fn drop_table(executor: &mut SchemaExecutor<'_>, stmt: DropTableStmt) -> Result<(), DbError> {
    let schema_name = stmt.name.schema_or_dbo().to_string();
    let table_id = executor.catalog.drop_table(&schema_name, &stmt.name.name)?;
    executor.storage.remove_table(table_id)?;
    Ok(())
}

pub(crate) fn create_schema(executor: &mut SchemaExecutor<'_>, stmt: CreateSchemaStmt) -> Result<(), DbError> {
    executor.catalog.create_schema(&stmt.name)
}

pub(crate) fn drop_schema(executor: &mut SchemaExecutor<'_>, stmt: DropSchemaStmt) -> Result<(), DbError> {
    executor.catalog.drop_schema(&stmt.name)
}

pub(crate) fn create_view(executor: &mut SchemaExecutor<'_>, stmt: crate::ast::CreateViewStmt) -> Result<(), DbError> {
    let schema = stmt.name.schema_or_dbo().to_string();
    let view_name = stmt.name.name;
    let query = stmt.query;
    let schema_id = executor.catalog.get_schema_id(&schema).unwrap_or(1);
    let mut view = crate::catalog::ViewDef {
        object_id: executor.catalog.alloc_object_id(),
        schema,
        name: view_name,
        schema_id,
        query: crate::ast::Statement::Dml(crate::ast::DmlStatement::Select(query)),
        definition_sql: String::new(),
    };
    view.definition_sql = format_view_definition(&view);
    executor.catalog.create_view(view)
}

pub(crate) fn drop_view(executor: &mut SchemaExecutor<'_>, stmt: crate::ast::DropViewStmt) -> Result<(), DbError> {
    let schema = stmt.name.schema_or_dbo();
    executor.catalog.drop_view(schema, &stmt.name.name)
}

pub(crate) fn create_synonym(
    executor: &mut SchemaExecutor<'_>,
    stmt: crate::ast::CreateSynonymStmt,
) -> Result<(), DbError> {
    let schema = stmt.name.schema_or_dbo().to_string();
    let object_id = executor.catalog.alloc_object_id();
    executor.catalog.create_synonym(crate::catalog::SynonymDef {
        object_id,
        schema,
        name: stmt.name.name,
        base_object: stmt.base_object,
    })
}

pub(crate) fn drop_synonym(
    executor: &mut SchemaExecutor<'_>,
    stmt: crate::ast::DropSynonymStmt,
) -> Result<(), DbError> {
    let schema = stmt.name.schema_or_dbo();
    executor.catalog.drop_synonym(schema, &stmt.name.name)
}

pub(crate) fn create_sequence(
    executor: &mut SchemaExecutor<'_>,
    stmt: crate::ast::CreateSequenceStmt,
) -> Result<(), DbError> {
    let schema = stmt.name.schema_or_dbo().to_string();
    let data_type = data_type_spec_to_runtime(&stmt.data_type);
    let object_id = executor.catalog.alloc_object_id();
    executor.catalog.create_sequence(crate::catalog::SequenceDef {
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
    executor: &mut SchemaExecutor<'_>,
    stmt: crate::ast::DropSequenceStmt,
) -> Result<(), DbError> {
    let schema = stmt.name.schema_or_dbo();
    executor.catalog.drop_sequence(schema, &stmt.name.name)
}

pub(crate) fn create_index(executor: &mut SchemaExecutor<'_>, stmt: CreateIndexStmt) -> Result<(), DbError> {
    let index_schema = stmt.name.schema_or_dbo().to_string();
    let table_schema = stmt.table.schema_or_dbo().to_string();

    let col_names: Vec<String> = stmt.columns.iter().map(|c| c.name.clone()).collect();
    executor.catalog.create_index_with_options(
        &index_schema,
        &stmt.name.name,
        &table_schema,
        &stmt.table.name,
        &col_names,
        stmt.is_clustered,
        stmt.is_unique,
    )?;

    if let (Some(index_def_id), Some(table)) = (
        executor
            .catalog
            .get_indexes()
            .iter()
            .find(|idx| idx.name.eq_ignore_ascii_case(&stmt.name.name))
            .map(|idx| idx.id),
        executor.catalog.find_table(&table_schema, &stmt.table.name),
    ) {
        schema_physical::rebuild_index_for_table(executor, index_def_id, table.id);
    }

    Ok(())
}

pub(crate) fn drop_index(executor: &mut SchemaExecutor<'_>, stmt: DropIndexStmt) -> Result<(), DbError> {
    let index_schema = stmt.name.schema_or_dbo().to_string();
    let table_schema = stmt.table.schema_or_dbo().to_string();
    executor.catalog.drop_index(
        &index_schema,
        &stmt.name.name,
        &table_schema,
        &stmt.table.name,
    )
}

pub(crate) fn alter_table(executor: &mut SchemaExecutor<'_>, stmt: AlterTableStmt) -> Result<(), DbError> {
    let schema_name = stmt.table.schema_or_dbo().to_string();
    let table_id = executor
        .catalog
        .find_table(&schema_name, &stmt.table.name)
        .map(|t| t.id)
        .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;

    match stmt.action {
        AlterTableAction::AddColumn(col_spec) => {
            let col = build_column_def(executor, col_spec)?;
            let table_mut = executor
                .catalog
                .find_table_mut(&schema_name, &stmt.table.name)
                .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;
            table_mut.columns.push(col);

            schema_physical::add_null_column_to_table(executor, table_id)?;
        }
        AlterTableAction::DropColumn(col_name) => {
            let table_mut = executor
                .catalog
                .find_table_mut(&schema_name, &stmt.table.name)
                .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;
            let col_idx = table_mut
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&col_name))
                .ok_or_else(|| DbError::column_not_found(&col_name))?;
            table_mut.columns.remove(col_idx);

            schema_physical::drop_column_from_table(executor, table_id, col_idx)?;
        }
        AlterTableAction::AlterColumn {
            name,
            data_type,
            nullable,
        } => {
            let table_mut = executor
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
            let table_mut = executor
                .catalog
                .find_table_mut(&schema_name, &stmt.table.name)
                .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;
            apply_table_constraint(table_mut, constraint)?;
        }
        AlterTableAction::DropConstraint(constraint_name) => {
            let table_mut = executor
                .catalog
                .find_table_mut(&schema_name, &stmt.table.name)
                .ok_or_else(|| DbError::table_not_found(&schema_name, &stmt.table.name))?;

            let removed = table_mut
                .check_constraints
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(&constraint_name));
            if let Some(pos) = removed {
                table_mut.check_constraints.remove(pos);
            } else {
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
