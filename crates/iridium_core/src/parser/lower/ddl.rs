use super::common::{lower_data_type, lower_expr, lower_object_name};
use super::dml::lower_select;
use super::lower_statement;
use crate::ast as executor_ast;
use crate::error::DbError;
use crate::parser::ast;

pub fn lower_ddl(ddl: ast::DdlStatement) -> Result<executor_ast::Statement, DbError> {
    match ddl {
        ast::DdlStatement::Create(s) => lower_create(*s),
        ast::DdlStatement::AlterTable { table, action } => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::AlterTable(
                executor_ast::statements::ddl::AlterTableStmt {
                    table: lower_object_name(table),
                    action: lower_alter_action(action)?,
                },
            ),
        )),
        ast::DdlStatement::TruncateTable(table) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::TruncateTable(
                executor_ast::statements::ddl::TruncateTableStmt {
                    name: lower_object_name(table),
                },
            ),
        )),
        ast::DdlStatement::DropTable(table) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::DropTable(
                executor_ast::statements::ddl::DropTableStmt {
                    name: lower_object_name(table),
                },
            ),
        )),
        ast::DdlStatement::DropView(name) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::DropView(
                executor_ast::statements::ddl::DropViewStmt {
                    name: lower_object_name(name),
                },
            ),
        )),
        ast::DdlStatement::DropProcedure(name) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::DropProcedure(
                executor_ast::statements::procedural::DropProcedureStmt {
                    name: lower_object_name(name),
                },
            ),
        )),
        ast::DdlStatement::DropFunction(name) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::DropFunction(
                executor_ast::statements::procedural::DropFunctionStmt {
                    name: lower_object_name(name),
                },
            ),
        )),
        ast::DdlStatement::DropTrigger(name) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::DropTrigger(
                executor_ast::statements::procedural::DropTriggerStmt {
                    name: lower_object_name(name),
                },
            ),
        )),
        ast::DdlStatement::DropIndex { name, table } => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::DropIndex(
                executor_ast::statements::ddl::DropIndexStmt {
                    name: lower_object_name(name),
                    table: lower_object_name(table),
                },
            ),
        )),
        ast::DdlStatement::DropType(name) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::DropType(
                executor_ast::statements::ddl::DropTypeStmt {
                    name: lower_object_name(name),
                },
            ),
        )),
        ast::DdlStatement::DropSchema(name) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::DropSchema(
                executor_ast::statements::ddl::DropSchemaStmt { name },
            ),
        )),
        ast::DdlStatement::CreateIndex {
            name,
            table,
            columns,
        } => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::CreateIndex(
                executor_ast::statements::ddl::CreateIndexStmt {
                    name: lower_object_name(name),
                    table: lower_object_name(table),
                    columns,
                },
            ),
        )),
        ast::DdlStatement::CreateType { name, columns } => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::CreateType(
                executor_ast::statements::ddl::CreateTypeStmt {
                    name: lower_object_name(name),
                    columns: columns
                        .into_iter()
                        .map(lower_column_def)
                        .collect::<Result<Vec<_>, _>>()?,
                    table_constraints: Vec::new(),
                },
            ),
        )),
        ast::DdlStatement::CreateSchema(name) => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::CreateSchema(
                executor_ast::statements::ddl::CreateSchemaStmt { name },
            ),
        )),
    }
}

pub fn lower_create(s: ast::CreateStmt) -> Result<executor_ast::Statement, DbError> {
    match s {
        ast::CreateStmt::Table {
            name,
            columns,
            constraints,
        } => Ok(executor_ast::Statement::Ddl(
            executor_ast::statements::DdlStatement::CreateTable(
                executor_ast::statements::ddl::CreateTableStmt {
                    name: lower_object_name(name),
                    columns: columns
                        .into_iter()
                        .map(lower_column_def)
                        .collect::<Result<Vec<_>, _>>()?,
                    table_constraints: constraints
                        .into_iter()
                        .map(lower_table_constraint)
                        .collect::<Result<Vec<_>, _>>()?,
                },
            ),
        )),
        ast::CreateStmt::View { name, query } => Ok(executor_ast::Statement::Procedural(
            executor_ast::statements::ProceduralStatement::CreateView(
                executor_ast::statements::ddl::CreateViewStmt {
                    name: lower_object_name(name),
                    query: lower_select(query)?,
                },
            ),
        )),
        ast::CreateStmt::Procedure { name, params, body } => {
            Ok(executor_ast::Statement::Procedural(
                executor_ast::statements::ProceduralStatement::CreateProcedure(
                    executor_ast::statements::procedural::CreateProcedureStmt {
                        name: lower_object_name(name),
                        params: params
                            .into_iter()
                            .map(lower_routine_param)
                            .collect::<Result<Vec<_>, _>>()?,
                        body: body
                            .into_iter()
                            .map(lower_statement)
                            .collect::<Result<Vec<_>, _>>()?,
                    },
                ),
            ))
        }
        ast::CreateStmt::Function {
            name,
            params,
            returns,
            body,
        } => Ok(executor_ast::Statement::Procedural(
            executor_ast::statements::ProceduralStatement::CreateFunction(
                executor_ast::statements::procedural::CreateFunctionStmt {
                    name: lower_object_name(name),
                    params: params
                        .into_iter()
                        .map(lower_routine_param)
                        .collect::<Result<Vec<_>, _>>()?,
                    returns: returns.map(lower_data_type).transpose()?,
                    body: match body {
                        ast::FunctionBody::ScalarReturn(e) => {
                            executor_ast::statements::procedural::FunctionBody::ScalarReturn(
                                lower_expr(e)?,
                            )
                        }
                        ast::FunctionBody::Block(stmts) => {
                            executor_ast::statements::procedural::FunctionBody::Scalar(
                                stmts
                                    .into_iter()
                                    .map(lower_statement)
                                    .collect::<Result<Vec<_>, _>>()?,
                            )
                        }
                        ast::FunctionBody::Table(sel) => {
                            executor_ast::statements::procedural::FunctionBody::InlineTable(
                                lower_select(sel)?,
                            )
                        }
                    },
                },
            ),
        )),
        ast::CreateStmt::Trigger {
            name,
            table,
            events,
            is_instead_of,
            body,
        } => Ok(executor_ast::Statement::Procedural(
            executor_ast::statements::ProceduralStatement::CreateTrigger(
                executor_ast::statements::procedural::CreateTriggerStmt {
                    name: lower_object_name(name),
                    table: lower_object_name(table),
                    events: events
                        .into_iter()
                        .map(|e| match e {
                            ast::TriggerEvent::Insert => executor_ast::TriggerEvent::Insert,
                            ast::TriggerEvent::Update => executor_ast::TriggerEvent::Update,
                            ast::TriggerEvent::Delete => executor_ast::TriggerEvent::Delete,
                        })
                        .collect(),
                    is_instead_of,
                    body: body
                        .into_iter()
                        .map(lower_statement)
                        .collect::<Result<Vec<_>, _>>()?,
                },
            ),
        )),
    }
}

pub fn lower_column_def(
    c: ast::ColumnDef,
) -> Result<executor_ast::statements::ddl::ColumnSpec, DbError> {
    let nullable_explicit = c.is_nullable.is_some();
    Ok(executor_ast::statements::ddl::ColumnSpec {
        name: c.name,
        data_type: lower_data_type(c.data_type)?,
        nullable: c.is_nullable.unwrap_or(true),
        nullable_explicit,
        identity: c.identity_spec,
        primary_key: c.is_primary_key,
        unique: c.is_unique,
        default: c.default_expr.map(lower_expr).transpose()?,
        default_constraint_name: c.default_constraint_name,
        check: c.check_expr.map(lower_expr).transpose()?,
        check_constraint_name: c.check_constraint_name,
        computed_expr: c.computed_expr.map(lower_expr).transpose()?,
        foreign_key: c
            .foreign_key
            .map(|fk| executor_ast::statements::ddl::ForeignKeyRef {
                referenced_table: lower_object_name(fk.ref_table),
                referenced_columns: fk.ref_columns,
                on_delete: fk.on_delete.map(lower_referential_action),
                on_update: fk.on_update.map(lower_referential_action),
            }),
        ansi_padding_on: true,
    })
}

pub fn lower_routine_param(
    p: ast::RoutineParam,
) -> Result<executor_ast::statements::RoutineParam, DbError> {
    let (param_type, is_readonly) = match p.data_type {
        ast::DataType::Custom(name) => {
            if !p.is_readonly {
                return Err(DbError::Parse(format!(
                    "table-valued parameter '{}' must be READONLY",
                    p.name
                )));
            }
            let name_str = name.as_str();
            let (schema, type_name) = match name_str.rsplit_once('.') {
                Some((schema, ty)) => (Some(schema.to_string()), ty.to_string()),
                None => (None, name_str.to_string()),
            };
            (
                executor_ast::statements::RoutineParamType::TableType(executor_ast::ObjectName {
                    schema,
                    name: type_name,
                }),
                true,
            )
        }
        other => (
            executor_ast::statements::RoutineParamType::Scalar(lower_data_type(other)?),
            p.is_readonly,
        ),
    };
    Ok(executor_ast::statements::RoutineParam {
        name: p.name,
        param_type,
        is_output: p.is_output,
        is_readonly,
        default: p.default.map(lower_expr).transpose()?,
    })
}

pub fn lower_alter_action(
    a: ast::AlterTableAction,
) -> Result<executor_ast::statements::ddl::AlterTableAction, DbError> {
    match a {
        ast::AlterTableAction::AddColumn(c) => Ok(
            executor_ast::statements::ddl::AlterTableAction::AddColumn(lower_column_def(c)?),
        ),
        ast::AlterTableAction::DropColumn(c) => Ok(
            executor_ast::statements::ddl::AlterTableAction::DropColumn(c),
        ),
        ast::AlterTableAction::AlterColumn { name, data_type, nullable } => Ok(
            executor_ast::statements::ddl::AlterTableAction::AlterColumn {
                name,
                data_type: lower_data_type(data_type)?,
                nullable,
            },
        ),
        ast::AlterTableAction::AddConstraint(c) => Ok(
            executor_ast::statements::ddl::AlterTableAction::AddConstraint(lower_table_constraint(
                c,
            )?),
        ),
        ast::AlterTableAction::DropConstraint(c) => {
            Ok(executor_ast::statements::ddl::AlterTableAction::DropConstraint(c))
        }
    }
}

pub fn lower_table_constraint(
    c: ast::TableConstraint,
) -> Result<executor_ast::statements::ddl::TableConstraintSpec, DbError> {
    match c {
        ast::TableConstraint::PrimaryKey { name, columns } => Ok(
            executor_ast::statements::ddl::TableConstraintSpec::PrimaryKey {
                name: name.unwrap_or_default(),
                columns,
            },
        ),
        ast::TableConstraint::Unique { name, columns } => {
            Ok(executor_ast::statements::ddl::TableConstraintSpec::Unique {
                name: name.unwrap_or_default(),
                columns,
            })
        }
        ast::TableConstraint::ForeignKey {
            name,
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        } => Ok(
            executor_ast::statements::ddl::TableConstraintSpec::ForeignKey {
                name: name.unwrap_or_default(),
                columns,
                referenced_table: lower_object_name(ref_table),
                referenced_columns: ref_columns,
                on_delete: on_delete.map(lower_referential_action),
                on_update: on_update.map(lower_referential_action),
            },
        ),
        ast::TableConstraint::Check { name, expr } => {
            Ok(executor_ast::statements::ddl::TableConstraintSpec::Check {
                name: name.unwrap_or_default(),
                expr: lower_expr(expr)?,
            })
        }
        ast::TableConstraint::Default { name, column, expr } => Ok(
            executor_ast::statements::ddl::TableConstraintSpec::Default {
                name: name.unwrap_or_default(),
                column,
                expr: lower_expr(expr)?,
            },
        ),
    }
}

pub fn lower_referential_action(
    a: ast::ReferentialAction,
) -> executor_ast::statements::ddl::ReferentialAction {
    match a {
        ast::ReferentialAction::NoAction => {
            executor_ast::statements::ddl::ReferentialAction::NoAction
        }
        ast::ReferentialAction::Cascade => {
            executor_ast::statements::ddl::ReferentialAction::Cascade
        }
        ast::ReferentialAction::SetNull => {
            executor_ast::statements::ddl::ReferentialAction::SetNull
        }
        ast::ReferentialAction::SetDefault => {
            executor_ast::statements::ddl::ReferentialAction::SetDefault
        }
    }
}
