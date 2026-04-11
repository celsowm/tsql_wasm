use crate::parser::ast;
use crate::ast as executor_ast;
use crate::error::DbError;
use super::common::{lower_expr, lower_object_name, lower_data_type};
use super::dml::lower_select;
use super::lower_statement;

pub fn lower_procedural(proc: ast::ProceduralStatement) -> Result<executor_ast::Statement, DbError> {
    match proc {
        ast::ProceduralStatement::Declare(vars) => {
            if vars.len() == 1 {
                let var = match vars.into_iter().next() {
                    Some(var) => var,
                    None => return Err(DbError::Parse("DECLARE requires at least one variable".into())),
                };
                Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::Declare(executor_ast::statements::procedural::DeclareStmt {
                    name: var.name,
                    data_type: lower_data_type(var.data_type)?,
                    default: var.initial_value.map(lower_expr).transpose()?,
                })))
            } else {
                let mut stmts = Vec::new();
                for var in vars {
                    stmts.push(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::Declare(executor_ast::statements::procedural::DeclareStmt {
                        name: var.name,
                        data_type: lower_data_type(var.data_type)?,
                        default: var.initial_value.map(lower_expr).transpose()?,
                    })));
                }
                Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::BeginEnd(stmts)))
            }
        }
        ast::ProceduralStatement::DeclareTableVar { name, columns, constraints } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::DeclareTableVar(executor_ast::statements::procedural::DeclareTableVarStmt {
                name,
                columns: columns.into_iter().map(super::ddl::lower_column_def).collect::<Result<Vec<_>, _>>()?,
                table_constraints: constraints.into_iter().map(super::ddl::lower_table_constraint).collect::<Result<Vec<_>, _>>()?,
            })))
        }
        ast::ProceduralStatement::DeclareCursor { name, query } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::DeclareCursor(executor_ast::statements::procedural::DeclareCursorStmt {
                name,
                query: lower_select(query)?,
            })))
        }
        ast::ProceduralStatement::Set { variable, expr } => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::Set(executor_ast::statements::procedural::SetStmt {
            name: variable,
            expr: lower_expr(expr)?,
        }))),
        ast::ProceduralStatement::If { condition, then_stmt, else_stmt } => {
            let then_body = match lower_statement(*then_stmt)? {
                executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::BeginEnd(stmts)) => stmts,
                other => vec![other],
            };
            let else_body = match else_stmt {
                Some(s) => Some(match lower_statement(*s)? {
                    executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::BeginEnd(stmts)) => stmts,
                    other => vec![other],
                }),
                None => None,
            };
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::If(executor_ast::statements::procedural::IfStmt {
                condition: lower_expr(condition)?,
                then_body,
                else_body,
            })))
        }
        ast::ProceduralStatement::BeginEnd(stmts) => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::BeginEnd(stmts.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?)))
        }
        ast::ProceduralStatement::While { condition, stmt } => {
            let body = match lower_statement(*stmt)? {
                executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::BeginEnd(stmts)) => stmts,
                other => vec![other],
            };
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::While(executor_ast::statements::procedural::WhileStmt {
                condition: lower_expr(condition)?,
                body,
            })))
        }
        ast::ProceduralStatement::Break => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::Break)),
        ast::ProceduralStatement::Continue => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::Continue)),
        ast::ProceduralStatement::Return(expr) => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::Return(expr.map(lower_expr).transpose()?))),
        ast::ProceduralStatement::Print(expr) => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::Print(lower_expr(expr)?))),
        ast::ProceduralStatement::Raiserror { message, severity, state } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::Raiserror(executor_ast::statements::procedural::RaiserrorStmt {
                message: lower_expr(message)?,
                severity: lower_expr(severity)?,
                state: lower_expr(state)?,
            })))
        }
        ast::ProceduralStatement::TryCatch { try_body, catch_body } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::TryCatch(executor_ast::statements::procedural::TryCatchStmt {
                try_body: try_body.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?,
                catch_body: catch_body.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?,
            })))
        }
        ast::ProceduralStatement::ExecDynamic { sql_expr } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::ExecDynamic(executor_ast::statements::procedural::ExecStmt {
                sql_expr: lower_expr(sql_expr)?,
            })))
        }
        ast::ProceduralStatement::ExecProcedure { return_variable, name, args } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::ExecProcedure(executor_ast::statements::procedural::ExecProcedureStmt {
                return_variable,
                name: lower_object_name(name),
                args: args.into_iter().map(lower_exec_arg).collect::<Result<Vec<_>, _>>()?,
            })))
        }
        ast::ProceduralStatement::SpExecuteSql { sql_expr, params_def, args } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::SpExecuteSql(executor_ast::statements::procedural::SpExecuteSqlStmt {
                sql_expr: lower_expr(sql_expr)?,
                params_def: params_def.map(lower_expr).transpose()?,
                args: args.into_iter().map(lower_exec_arg).collect::<Result<Vec<_>, _>>()?,
            })))
        }
    }
}

pub fn lower_transaction(txn: ast::TransactionStatement) -> Result<executor_ast::Statement, DbError> {
    match txn {
        ast::TransactionStatement::Begin(name) => Ok(executor_ast::Statement::Transaction(executor_ast::statements::TransactionStatement::Begin(name))),
        ast::TransactionStatement::Commit(name) => Ok(executor_ast::Statement::Transaction(executor_ast::statements::TransactionStatement::Commit(name))),
        ast::TransactionStatement::Rollback(name) => Ok(executor_ast::Statement::Transaction(executor_ast::statements::TransactionStatement::Rollback(name))),
        ast::TransactionStatement::Save(name) => Ok(executor_ast::Statement::Transaction(executor_ast::statements::TransactionStatement::Save(name))),
    }
}

pub fn lower_cursor(cursor: ast::CursorStatement) -> Result<executor_ast::Statement, DbError> {
    match cursor {
        ast::CursorStatement::Open(name) => Ok(executor_ast::Statement::Cursor(executor_ast::statements::CursorStatement::OpenCursor(name))),
        ast::CursorStatement::Fetch { name, direction, into_vars } => {
            Ok(executor_ast::Statement::Cursor(executor_ast::statements::CursorStatement::FetchCursor(executor_ast::statements::procedural::FetchCursorStmt {
                name,
                direction: lower_fetch_direction(direction)?,
                into: into_vars,
            })))
        }
        ast::CursorStatement::Close(name) => Ok(executor_ast::Statement::Cursor(executor_ast::statements::CursorStatement::CloseCursor(name))),
        ast::CursorStatement::Deallocate(name) => Ok(executor_ast::Statement::Cursor(executor_ast::statements::CursorStatement::DeallocateCursor(name))),
    }
}

pub fn lower_session(session: ast::SessionStatement) -> Result<executor_ast::Statement, DbError> {
    match session {
        ast::SessionStatement::SetTransactionIsolationLevel(iso) => Ok(executor_ast::Statement::Session(executor_ast::statements::SessionStatement::SetTransactionIsolationLevel(match iso {
            ast::IsolationLevel::ReadUncommitted => executor_ast::IsolationLevel::ReadUncommitted,
            ast::IsolationLevel::ReadCommitted => executor_ast::IsolationLevel::ReadCommitted,
            ast::IsolationLevel::RepeatableRead => executor_ast::IsolationLevel::RepeatableRead,
            ast::IsolationLevel::Serializable => executor_ast::IsolationLevel::Serializable,
            ast::IsolationLevel::Snapshot => executor_ast::IsolationLevel::Snapshot,
        }))),
        ast::SessionStatement::SetOption { option, value } => Ok(executor_ast::Statement::Session(executor_ast::statements::SessionStatement::SetOption(executor_ast::statements::procedural::SetOptionStmt {
            option: match option {
                ast::SessionOption::AnsiNulls => executor_ast::SessionOption::AnsiNulls,
                ast::SessionOption::QuotedIdentifier => executor_ast::SessionOption::QuotedIdentifier,
                ast::SessionOption::NoCount => executor_ast::SessionOption::NoCount,
                ast::SessionOption::XactAbort => executor_ast::SessionOption::XactAbort,
                ast::SessionOption::DateFirst => executor_ast::SessionOption::DateFirst,
                ast::SessionOption::Language => executor_ast::SessionOption::Language,
                ast::SessionOption::DateFormat => executor_ast::SessionOption::DateFormat,
                ast::SessionOption::LockTimeout => executor_ast::SessionOption::LockTimeout,
                ast::SessionOption::RowCount => executor_ast::SessionOption::RowCount,
                ast::SessionOption::TextSize => executor_ast::SessionOption::TextSize,
                ast::SessionOption::ConcatNullYieldsNull => executor_ast::SessionOption::ConcatNullYieldsNull,
                ast::SessionOption::ArithAbort => executor_ast::SessionOption::ArithAbort,
                ast::SessionOption::QueryGovernorCostLimit => executor_ast::SessionOption::QueryGovernorCostLimit,
                ast::SessionOption::DeadlockPriority => executor_ast::SessionOption::DeadlockPriority,
                ast::SessionOption::AnsiNullDfltOn => executor_ast::SessionOption::AnsiNullDfltOn,
                ast::SessionOption::AnsiPadding => executor_ast::SessionOption::AnsiPadding,
                ast::SessionOption::AnsiWarnings => executor_ast::SessionOption::AnsiWarnings,
                ast::SessionOption::CursorCloseOnCommit => executor_ast::SessionOption::CursorCloseOnCommit,
                ast::SessionOption::ImplicitTransactions => executor_ast::SessionOption::ImplicitTransactions,
                ast::SessionOption::Unsupported(v) => executor_ast::SessionOption::Unsupported(v),
            },
            value: match value {
                ast::SessionOptionValue::Bool(v) => executor_ast::SessionOptionValue::Bool(v),
                ast::SessionOptionValue::Int(v) => executor_ast::SessionOptionValue::Int(v),
                ast::SessionOptionValue::Text(v) => executor_ast::SessionOptionValue::Text(v),
            },
        }))),
        ast::SessionStatement::SetIdentityInsert { table, on } => Ok(executor_ast::Statement::Session(executor_ast::statements::SessionStatement::SetIdentityInsert(executor_ast::statements::SetIdentityInsertStmt {
            table: lower_object_name(table),
            on,
        }))),
    }
}

pub fn lower_exec_arg(a: ast::ExecArg) -> Result<executor_ast::statements::procedural::ExecArgument, DbError> {
    Ok(executor_ast::statements::procedural::ExecArgument {
        name: a.name,
        expr: lower_expr(a.expr)?,
        is_output: a.is_output,
    })
}

pub fn lower_fetch_direction(d: ast::FetchDirection) -> Result<executor_ast::statements::procedural::FetchDirection, DbError> {
    match d {
        ast::FetchDirection::Next => Ok(executor_ast::statements::procedural::FetchDirection::Next),
        ast::FetchDirection::Prior => Ok(executor_ast::statements::procedural::FetchDirection::Prior),
        ast::FetchDirection::First => Ok(executor_ast::statements::procedural::FetchDirection::First),
        ast::FetchDirection::Last => Ok(executor_ast::statements::procedural::FetchDirection::Last),
        ast::FetchDirection::Absolute(expr) => Ok(executor_ast::statements::procedural::FetchDirection::Absolute(lower_expr(expr)?)),
        ast::FetchDirection::Relative(expr) => Ok(executor_ast::statements::procedural::FetchDirection::Relative(lower_expr(expr)?)),
    }
}
