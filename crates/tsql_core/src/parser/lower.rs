use crate::parser::ast;
use crate::ast as executor_ast;
use crate::error::DbError;

pub fn lower_batch(parser_stmts: Vec<ast::Statement>) -> Result<Vec<executor_ast::Statement>, DbError> {
    parser_stmts.into_iter().map(lower_statement).collect()
}

pub fn lower_statement(parser_stmt: ast::Statement) -> Result<executor_ast::Statement, DbError> {
    match parser_stmt {
        ast::Statement::Dml(dml) => lower_dml(dml),
        ast::Statement::Ddl(ddl) => lower_ddl(ddl),
        ast::Statement::Procedural(proc) => lower_procedural(proc),
        ast::Statement::Transaction(txn) => lower_transaction(txn),
        ast::Statement::Cursor(cursor) => lower_cursor(cursor),
        ast::Statement::Session(session) => lower_session(session),
        ast::Statement::WithCte { ctes, body } => {
            let ctes: Result<Vec<_>, _> = ctes.into_iter().map(|cte| {
                let query = lower_statement(ast::Statement::Dml(ast::DmlStatement::Select(Box::new(cte.query))))?;
                Ok(executor_ast::statements::procedural::CteDef {
                    name: cte.name,
                    query,
                })
            }).collect();
            let ctes = ctes?;
            let body = Box::new(lower_statement(*body)?);
            Ok(executor_ast::Statement::WithCte(executor_ast::statements::procedural::WithCteStmt {
                recursive: false,
                ctes,
                body,
            }))
        }
    }
}

fn lower_dml(dml: ast::DmlStatement) -> Result<executor_ast::Statement, DbError> {
    match dml {
        ast::DmlStatement::Select(s) => {
            if let Some(ref op) = s.set_op {
                let mut left_parser = (*s).clone();
                left_parser.set_op = None;
                let left = lower_select(left_parser)?;
                let right = lower_select(op.right.clone())?;
                let kind = match op.kind {
                    ast::SetOpKind::Union => executor_ast::statements::query::SetOpKind::Union,
                    ast::SetOpKind::UnionAll => executor_ast::statements::query::SetOpKind::UnionAll,
                    ast::SetOpKind::Intersect => executor_ast::statements::query::SetOpKind::Intersect,
                    ast::SetOpKind::Except => executor_ast::statements::query::SetOpKind::Except,
                };
                let set_op = executor_ast::statements::query::SetOpStmt {
                    left: Box::new(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Select(left))),
                    op: kind,
                    right: Box::new(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Select(right))),
                };
                return Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::SetOp(set_op)));
            }
            Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Select(lower_select(*s)?)))
        }
        ast::DmlStatement::Insert(s) => Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Insert(lower_insert(*s)?))),
        ast::DmlStatement::Update(s) => Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Update(lower_update(*s)?))),
        ast::DmlStatement::Delete(s) => Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Delete(lower_delete(*s)?))),
        ast::DmlStatement::Merge(s) => Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Merge(lower_merge(*s)?))),
        ast::DmlStatement::SelectAssign { assignments, from, selection } => {
            let mut joins = Vec::new();
            let from_tr = if let Some(from_ref) = from {
                let (tr, mut j) = lower_table_ref_recursive(from_ref)?;
                joins.append(&mut j);
                Some(tr)
            } else {
                None
            };
            Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::SelectAssign(executor_ast::statements::procedural::SelectAssignStmt {
                targets: assignments.into_iter().map(|a| Ok(executor_ast::statements::procedural::SelectAssignTarget {
                    variable: a.variable,
                    expr: lower_expr(a.expr)?,
                })).collect::<Result<Vec<_>, _>>()?,
                from: from_tr,
                joins,
                selection: selection.map(lower_expr).transpose()?,
            })))
        }
    }
}

fn lower_ddl(ddl: ast::DdlStatement) -> Result<executor_ast::Statement, DbError> {
    match ddl {
        ast::DdlStatement::Create(s) => lower_create(*s),
        ast::DdlStatement::AlterTable { table, action } => {
            Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::AlterTable(executor_ast::statements::ddl::AlterTableStmt {
                table: lower_object_name(table),
                action: lower_alter_action(action)?,
            })))
        }
        ast::DdlStatement::TruncateTable(table) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::TruncateTable(executor_ast::statements::ddl::TruncateTableStmt {
            name: lower_object_name(table),
        }))),
        ast::DdlStatement::DropTable(table) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::DropTable(executor_ast::statements::ddl::DropTableStmt {
            name: lower_object_name(table),
        }))),
        ast::DdlStatement::DropView(name) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::DropView(executor_ast::statements::ddl::DropViewStmt {
            name: lower_object_name(name),
        }))),
        ast::DdlStatement::DropProcedure(name) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::DropProcedure(executor_ast::statements::procedural::DropProcedureStmt {
            name: lower_object_name(name),
        }))),
        ast::DdlStatement::DropFunction(name) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::DropFunction(executor_ast::statements::procedural::DropFunctionStmt {
            name: lower_object_name(name),
        }))),
        ast::DdlStatement::DropTrigger(name) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::DropTrigger(executor_ast::statements::procedural::DropTriggerStmt {
            name: lower_object_name(name),
        }))),
        ast::DdlStatement::DropIndex { name, table } => {
            Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::DropIndex(executor_ast::statements::ddl::DropIndexStmt {
                name: lower_object_name(name),
                table: lower_object_name(table),
            })))
        }
        ast::DdlStatement::DropType(name) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::DropType(executor_ast::statements::ddl::DropTypeStmt {
            name: lower_object_name(name),
        }))),
        ast::DdlStatement::DropSchema(name) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::DropSchema(executor_ast::statements::ddl::DropSchemaStmt {
            name: name,
        }))),
        ast::DdlStatement::CreateIndex { name, table, columns } => {
            Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::CreateIndex(executor_ast::statements::ddl::CreateIndexStmt {
                name: lower_object_name(name),
                table: lower_object_name(table),
                columns: columns,
            })))
        }
        ast::DdlStatement::CreateType { name, columns } => {
            Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::CreateType(executor_ast::statements::ddl::CreateTypeStmt {
                name: lower_object_name(name),
                columns: columns.into_iter().map(lower_column_def).collect::<Result<Vec<_>, _>>()?,
                table_constraints: Vec::new(),
            })))
        }
        ast::DdlStatement::CreateSchema(name) => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::CreateSchema(executor_ast::statements::ddl::CreateSchemaStmt {
            name: name,
        }))),
    }
}

fn lower_procedural(proc: ast::ProceduralStatement) -> Result<executor_ast::Statement, DbError> {
    match proc {
        ast::ProceduralStatement::Declare(vars) => {
            if vars.len() == 1 {
                let var = vars.into_iter().next().unwrap();
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
                name: name,
                columns: columns.into_iter().map(lower_column_def).collect::<Result<Vec<_>, _>>()?,
                table_constraints: constraints.into_iter().map(lower_table_constraint).collect(),
            })))
        }
        ast::ProceduralStatement::DeclareCursor { name, query } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::DeclareCursor(executor_ast::statements::procedural::DeclareCursorStmt {
                name: name,
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
        ast::ProceduralStatement::ExecProcedure { name, args } => {
            Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::ExecProcedure(executor_ast::statements::procedural::ExecProcedureStmt {
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

fn lower_transaction(txn: ast::TransactionStatement) -> Result<executor_ast::Statement, DbError> {
    match txn {
        ast::TransactionStatement::Begin(name) => Ok(executor_ast::Statement::Transaction(executor_ast::statements::TransactionStatement::Begin(name))),
        ast::TransactionStatement::Commit(name) => Ok(executor_ast::Statement::Transaction(executor_ast::statements::TransactionStatement::Commit(name))),
        ast::TransactionStatement::Rollback(name) => Ok(executor_ast::Statement::Transaction(executor_ast::statements::TransactionStatement::Rollback(name))),
        ast::TransactionStatement::Save(name) => Ok(executor_ast::Statement::Transaction(executor_ast::statements::TransactionStatement::Save(name))),
    }
}

fn lower_cursor(cursor: ast::CursorStatement) -> Result<executor_ast::Statement, DbError> {
    match cursor {
        ast::CursorStatement::Open(name) => Ok(executor_ast::Statement::Cursor(executor_ast::statements::CursorStatement::OpenCursor(name))),
        ast::CursorStatement::Fetch { name, direction, into_vars } => {
            Ok(executor_ast::Statement::Cursor(executor_ast::statements::CursorStatement::FetchCursor(executor_ast::statements::procedural::FetchCursorStmt {
                name: name,
                direction: lower_fetch_direction(direction)?,
                into: into_vars,
            })))
        }
        ast::CursorStatement::Close(name) => Ok(executor_ast::Statement::Cursor(executor_ast::statements::CursorStatement::CloseCursor(name))),
        ast::CursorStatement::Deallocate(name) => Ok(executor_ast::Statement::Cursor(executor_ast::statements::CursorStatement::DeallocateCursor(name))),
    }
}

fn lower_session(session: ast::SessionStatement) -> Result<executor_ast::Statement, DbError> {
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

pub fn lower_expr(parser_expr: ast::Expr) -> Result<executor_ast::expressions::Expr, DbError> {
    match parser_expr {
        ast::Expr::Identifier(id) => Ok(executor_ast::expressions::Expr::Identifier(id)),
        ast::Expr::Variable(id) => Ok(executor_ast::expressions::Expr::Identifier(id)),
        ast::Expr::QualifiedIdentifier(parts) => Ok(executor_ast::expressions::Expr::QualifiedIdentifier(parts)),
        ast::Expr::Wildcard => Ok(executor_ast::expressions::Expr::Wildcard),
        ast::Expr::QualifiedWildcard(parts) => Ok(executor_ast::expressions::Expr::QualifiedWildcard(parts)),
        ast::Expr::Integer(i) => Ok(executor_ast::expressions::Expr::Integer(i)),
        ast::Expr::Float(f) => Ok(executor_ast::expressions::Expr::FloatLiteral(f64::from_bits(f).to_string())),
        ast::Expr::String(s) => Ok(executor_ast::expressions::Expr::String(s)),
        ast::Expr::UnicodeString(s) => Ok(executor_ast::expressions::Expr::UnicodeString(s)),
        ast::Expr::BinaryLiteral(b) => Ok(executor_ast::expressions::Expr::BinaryLiteral(b)),
        ast::Expr::Null => Ok(executor_ast::expressions::Expr::Null),
        ast::Expr::Bool(b) => Ok(executor_ast::expressions::Expr::Integer(if b { 1 } else { 0 })),
        ast::Expr::Binary { left, op, right } => {
             match op {
                 ast::BinaryOp::Like => Ok(executor_ast::expressions::Expr::Like {
                     expr: Box::new(lower_expr(*left)?),
                     pattern: Box::new(lower_expr(*right)?),
                     negated: false,
                 }),
                 _ => Ok(executor_ast::expressions::Expr::Binary {
                     left: Box::new(lower_expr(*left)?),
                     op: lower_binary_op(op)?,
                     right: Box::new(lower_expr(*right)?),
                 })
             }
        }
        ast::Expr::Unary { op, expr } => Ok(executor_ast::expressions::Expr::Unary {
            op: lower_unary_op(op),
            expr: Box::new(lower_expr(*expr)?),
        }),
        ast::Expr::IsNull(expr) => Ok(executor_ast::expressions::Expr::IsNull(Box::new(lower_expr(*expr)?))),
        ast::Expr::IsNotNull(expr) => Ok(executor_ast::expressions::Expr::IsNotNull(Box::new(lower_expr(*expr)?))),
        ast::Expr::Cast { expr, target } => Ok(executor_ast::expressions::Expr::Cast {
            expr: Box::new(lower_expr(*expr)?),
            target: lower_data_type(target)?,
        }),
        ast::Expr::TryCast { expr, target } => Ok(executor_ast::expressions::Expr::TryCast {
            expr: Box::new(lower_expr(*expr)?),
            target: lower_data_type(target)?,
        }),
        ast::Expr::Convert { target, expr, style } => Ok(executor_ast::expressions::Expr::Convert {
            target: lower_data_type(target)?,
            expr: Box::new(lower_expr(*expr)?),
            style,
        }),
        ast::Expr::TryConvert { target, expr, style } => Ok(executor_ast::expressions::Expr::TryConvert {
            target: lower_data_type(target)?,
            expr: Box::new(lower_expr(*expr)?),
            style,
        }),
        ast::Expr::Case { operand, when_clauses, else_result } => Ok(executor_ast::expressions::Expr::Case {
            operand: operand.map(|e| lower_expr(*e)).transpose()?.map(Box::new),
            when_clauses: when_clauses.into_iter().map(|w| Ok(executor_ast::expressions::WhenClause {
                condition: lower_expr(w.condition)?,
                result: lower_expr(w.result)?,
            })).collect::<Result<Vec<_>, _>>()?,
            else_result: else_result.map(|e| lower_expr(*e)).transpose()?.map(Box::new),
        }),
        ast::Expr::InList { expr, list, negated } => Ok(executor_ast::expressions::Expr::InList {
            expr: Box::new(lower_expr(*expr)?),
            list: list.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
            negated,
        }),
        ast::Expr::InSubquery { expr, subquery, negated } => Ok(executor_ast::expressions::Expr::InSubquery {
            expr: Box::new(lower_expr(*expr)?),
            subquery: Box::new(lower_select(*subquery)?),
            negated,
        }),
        ast::Expr::Between { expr, low, high, negated } => Ok(executor_ast::expressions::Expr::Between {
            expr: Box::new(lower_expr(*expr)?),
            low: Box::new(lower_expr(*low)?),
            high: Box::new(lower_expr(*high)?),
            negated,
        }),
        ast::Expr::Like { expr, pattern, negated } => Ok(executor_ast::expressions::Expr::Like {
            expr: Box::new(lower_expr(*expr)?),
            pattern: Box::new(lower_expr(*pattern)?),
            negated,
        }),
        ast::Expr::Exists { subquery, negated } => Ok(executor_ast::expressions::Expr::Exists {
            subquery: Box::new(lower_select(*subquery)?),
            negated,
        }),
        ast::Expr::Subquery(s) => Ok(executor_ast::expressions::Expr::Subquery(Box::new(lower_select(*s)?))),
        ast::Expr::FunctionCall { name, args } => Ok(executor_ast::expressions::Expr::FunctionCall {
            name: name,
            args: args.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
        }),
        ast::Expr::WindowFunction { name, args, partition_by, order_by, frame } => {
            Ok(executor_ast::expressions::Expr::WindowFunction {
                func: lower_window_func(name.as_str()),
                args: args.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
                partition_by: partition_by.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
                order_by: order_by.into_iter().map(lower_order_by_expr).collect::<Result<Vec<_>, _>>()?,
                frame: frame.map(lower_window_frame),
            })
        }
    }
}

pub fn lower_binary_op(op: ast::BinaryOp) -> Result<executor_ast::expressions::BinaryOp, DbError> {
    match op {
        ast::BinaryOp::Eq => Ok(executor_ast::expressions::BinaryOp::Eq),
        ast::BinaryOp::NotEq => Ok(executor_ast::expressions::BinaryOp::NotEq),
        ast::BinaryOp::Gt => Ok(executor_ast::expressions::BinaryOp::Gt),
        ast::BinaryOp::Lt => Ok(executor_ast::expressions::BinaryOp::Lt),
        ast::BinaryOp::Gte => Ok(executor_ast::expressions::BinaryOp::Gte),
        ast::BinaryOp::Lte => Ok(executor_ast::expressions::BinaryOp::Lte),
        ast::BinaryOp::And => Ok(executor_ast::expressions::BinaryOp::And),
        ast::BinaryOp::Or => Ok(executor_ast::expressions::BinaryOp::Or),
        ast::BinaryOp::Add => Ok(executor_ast::expressions::BinaryOp::Add),
        ast::BinaryOp::Subtract => Ok(executor_ast::expressions::BinaryOp::Subtract),
        ast::BinaryOp::Multiply => Ok(executor_ast::expressions::BinaryOp::Multiply),
        ast::BinaryOp::Divide => Ok(executor_ast::expressions::BinaryOp::Divide),
        ast::BinaryOp::Modulo => Ok(executor_ast::expressions::BinaryOp::Modulo),
        ast::BinaryOp::BitwiseAnd => Ok(executor_ast::expressions::BinaryOp::BitwiseAnd),
        ast::BinaryOp::BitwiseOr => Ok(executor_ast::expressions::BinaryOp::BitwiseOr),
        ast::BinaryOp::BitwiseXor => Ok(executor_ast::expressions::BinaryOp::BitwiseXor),
        ast::BinaryOp::Like => Err(DbError::Parse("LIKE should be lowered as a dedicated expression".into())),
    }
}

pub fn lower_unary_op(op: ast::UnaryOp) -> executor_ast::expressions::UnaryOp {
    match op {
        ast::UnaryOp::Negate => executor_ast::expressions::UnaryOp::Negate,
        ast::UnaryOp::Not => executor_ast::expressions::UnaryOp::Not,
        ast::UnaryOp::BitwiseNot => executor_ast::expressions::UnaryOp::BitwiseNot,
    }
}

pub fn lower_window_func(name: &str) -> executor_ast::expressions::WindowFunc {
    match name.to_uppercase().as_str() {
        "ROW_NUMBER" => executor_ast::expressions::WindowFunc::RowNumber,
        "RANK" => executor_ast::expressions::WindowFunc::Rank,
        "DENSE_RANK" => executor_ast::expressions::WindowFunc::DenseRank,
        "NTILE" => executor_ast::expressions::WindowFunc::NTile,
        "LAG" => executor_ast::expressions::WindowFunc::Lag,
        "LEAD" => executor_ast::expressions::WindowFunc::Lead,
        "FIRST_VALUE" => executor_ast::expressions::WindowFunc::FirstValue,
        "LAST_VALUE" => executor_ast::expressions::WindowFunc::LastValue,
        "PERCENTILE_CONT" => executor_ast::expressions::WindowFunc::PercentileCont,
        "PERCENTILE_DISC" => executor_ast::expressions::WindowFunc::PercentileDisc,
        "PERCENT_RANK" => executor_ast::expressions::WindowFunc::PercentileRank,
        _ => executor_ast::expressions::WindowFunc::Aggregate(name.to_string()),
    }
}

pub fn lower_window_frame(frame: ast::WindowFrame) -> executor_ast::expressions::WindowFrame {
    executor_ast::expressions::WindowFrame {
        units: match frame.units {
            ast::WindowFrameUnits::Rows => executor_ast::expressions::WindowFrameUnits::Rows,
            ast::WindowFrameUnits::Range => executor_ast::expressions::WindowFrameUnits::Range,
            ast::WindowFrameUnits::Groups => executor_ast::expressions::WindowFrameUnits::Groups,
        },
        extent: match frame.extent {
            ast::WindowFrameExtent::Bound(b) => executor_ast::expressions::WindowFrameExtent::Bound(lower_window_bound(b)),
            ast::WindowFrameExtent::Between(b1, b2) => executor_ast::expressions::WindowFrameExtent::Between(lower_window_bound(b1), lower_window_bound(b2)),
        }
    }
}

pub fn lower_window_bound(bound: ast::WindowFrameBound) -> executor_ast::expressions::WindowFrameBound {
    match bound {
        ast::WindowFrameBound::UnboundedPreceding => executor_ast::expressions::WindowFrameBound::UnboundedPreceding,
        ast::WindowFrameBound::Preceding(n) => executor_ast::expressions::WindowFrameBound::Preceding(n),
        ast::WindowFrameBound::CurrentRow => executor_ast::expressions::WindowFrameBound::CurrentRow,
        ast::WindowFrameBound::Following(n) => executor_ast::expressions::WindowFrameBound::Following(n),
        ast::WindowFrameBound::UnboundedFollowing => executor_ast::expressions::WindowFrameBound::UnboundedFollowing,
    }
}

pub fn lower_object_name(parts: Vec<String>) -> executor_ast::ObjectName {
    let mut parts_owned = parts;
    if parts_owned.len() == 1 {
        executor_ast::ObjectName { schema: None, name: parts_owned.remove(0) }
    } else if parts_owned.len() == 2 {
        let name = parts_owned.pop().unwrap();
        let schema = Some(parts_owned.pop().unwrap());
        executor_ast::ObjectName { schema, name }
    } else {
        let name = parts_owned.pop().unwrap();
        let schema = Some(parts_owned.pop().unwrap());
        executor_ast::ObjectName { schema, name }
    }
}

pub fn lower_data_type(dt: ast::DataType) -> Result<executor_ast::data_types::DataTypeSpec, DbError> {
    match dt {
        ast::DataType::Int => Ok(executor_ast::data_types::DataTypeSpec::Int),
        ast::DataType::BigInt => Ok(executor_ast::data_types::DataTypeSpec::BigInt),
        ast::DataType::SmallInt => Ok(executor_ast::data_types::DataTypeSpec::SmallInt),
        ast::DataType::TinyInt => Ok(executor_ast::data_types::DataTypeSpec::TinyInt),
        ast::DataType::Bit => Ok(executor_ast::data_types::DataTypeSpec::Bit),
        ast::DataType::Float => Ok(executor_ast::data_types::DataTypeSpec::Float),
        ast::DataType::Real => Ok(executor_ast::data_types::DataTypeSpec::Float),
        ast::DataType::Decimal(p, s) => Ok(executor_ast::data_types::DataTypeSpec::Decimal(p, s)),
        ast::DataType::Numeric(p, s) => Ok(executor_ast::data_types::DataTypeSpec::Numeric(p, s)),
        ast::DataType::VarChar(n) => Ok(executor_ast::data_types::DataTypeSpec::VarChar(n.unwrap_or(u16::MAX as u32) as u16)),
        ast::DataType::NVarChar(n) => Ok(executor_ast::data_types::DataTypeSpec::NVarChar(n.unwrap_or(u16::MAX as u32) as u16)),
        ast::DataType::Char(n) => Ok(executor_ast::data_types::DataTypeSpec::Char(n.unwrap_or(1) as u16)),
        ast::DataType::NChar(n) => Ok(executor_ast::data_types::DataTypeSpec::NChar(n.unwrap_or(1) as u16)),
        ast::DataType::Binary(n) => Ok(executor_ast::data_types::DataTypeSpec::Binary(n.unwrap_or(1) as u16)),
        ast::DataType::VarBinary(n) => Ok(executor_ast::data_types::DataTypeSpec::VarBinary(n.unwrap_or(u16::MAX as u32) as u16)),
        ast::DataType::Date => Ok(executor_ast::data_types::DataTypeSpec::Date),
        ast::DataType::Time => Ok(executor_ast::data_types::DataTypeSpec::Time),
        ast::DataType::DateTime => Ok(executor_ast::data_types::DataTypeSpec::DateTime),
        ast::DataType::DateTime2 => Ok(executor_ast::data_types::DataTypeSpec::DateTime2),
        ast::DataType::Money => Ok(executor_ast::data_types::DataTypeSpec::Money),
        ast::DataType::SmallMoney => Ok(executor_ast::data_types::DataTypeSpec::SmallMoney),
        ast::DataType::UniqueIdentifier => Ok(executor_ast::data_types::DataTypeSpec::UniqueIdentifier),
        ast::DataType::SqlVariant => Ok(executor_ast::data_types::DataTypeSpec::SqlVariant),
        ast::DataType::Xml => Ok(executor_ast::data_types::DataTypeSpec::Xml),
        ast::DataType::DateTimeOffset => Ok(executor_ast::data_types::DataTypeSpec::VarChar(255)),
        ast::DataType::SmallDateTime => Ok(executor_ast::data_types::DataTypeSpec::DateTime),
        ast::DataType::Image => Ok(executor_ast::data_types::DataTypeSpec::VarBinary(8000)),
        ast::DataType::Text => Ok(executor_ast::data_types::DataTypeSpec::VarChar(8000)),
        ast::DataType::NText => Ok(executor_ast::data_types::DataTypeSpec::NVarChar(4000)),
        ast::DataType::Table => Ok(executor_ast::data_types::DataTypeSpec::VarChar(255)),
        ast::DataType::Custom(_) => Ok(executor_ast::data_types::DataTypeSpec::VarChar(255)),
    }
}

pub fn lower_select(s: ast::SelectStmt) -> Result<executor_ast::statements::query::SelectStmt, DbError> {
    if s.set_op.is_some() {
        return Err(DbError::Parse("Subqueries with UNION/INTERSECT/EXCEPT not yet supported in this version".into()));
    }

    let mut from = None;
    let mut joins = Vec::new();

    if let Some(from_ref) = s.from {
        let (tr, mut j) = lower_table_ref_recursive(from_ref)?;
        from = Some(tr);
        joins.append(&mut j);
    }

    for join in s.joins {
        joins.push(lower_join_clause(join)?);
    }

    Ok(executor_ast::statements::query::SelectStmt {
        distinct: s.distinct,
        top: s.top.map(|top| Ok(executor_ast::statements::query::TopSpec { value: lower_expr(top.value)? })).transpose()?,
        projection: s.projection.into_iter().map(|i| Ok(executor_ast::statements::query::SelectItem {
            expr: lower_expr(i.expr)?,
            alias: i.alias,
        })).collect::<Result<Vec<_>, DbError>>()?,
        into_table: s.into_table.map(lower_object_name_owned),
        from,
        joins,
        applies: s.applies.into_iter().map(|a| {
            let (tr, extra_joins) = lower_table_ref_recursive(a.table)?;
            if !extra_joins.is_empty() {
                return Err(DbError::Parse("Joins inside APPLY are not yet supported in this version".into()));
            }
            let subquery = match tr.factor {
                executor_ast::common::TableFactor::Derived(s) => *s,
                executor_ast::common::TableFactor::Values { rows, columns } => {
                    executor_ast::statements::query::SelectStmt {
                        from: Some(executor_ast::common::TableRef {
                            factor: executor_ast::common::TableFactor::Values { rows, columns },
                            alias: tr.alias.clone(),
                            pivot: None,
                            unpivot: None,
                            hints: Vec::new(),
                        }),
                        joins: Vec::new(),
                        applies: Vec::new(),
                        projection: vec![executor_ast::statements::query::SelectItem {
                            expr: executor_ast::expressions::Expr::Wildcard,
                            alias: None,
                        }],
                        into_table: None,
                        distinct: false,
                        top: None,
                        selection: None,
                        group_by: Vec::new(),
                        having: None,
                        order_by: Vec::new(),
                        offset: None,
                        fetch: None,
                    }
                }
                _ => return Err(DbError::Parse("Only subqueries and VALUES are supported in APPLY in this version".into())),
            };
            Ok(executor_ast::statements::query::ApplyClause {
                apply_type: match a.apply_type {
                    ast::ApplyType::Cross => executor_ast::statements::query::ApplyType::Cross,
                    ast::ApplyType::Outer => executor_ast::statements::query::ApplyType::Outer,
                },
                subquery,
                alias: tr.alias.unwrap_or_default(),
            })
        }).collect::<Result<Vec<_>, DbError>>()?,
        selection: s.selection.map(lower_expr).transpose()?,
        group_by: s.group_by.into_iter().map(lower_expr).collect::<Result<Vec<_>, DbError>>()?,
        having: s.having.map(lower_expr).transpose()?,
        order_by: s.order_by.into_iter().map(lower_order_by_expr).collect::<Result<Vec<_>, DbError>>()?,
        offset: s.offset.map(lower_expr).transpose()?,
        fetch: s.fetch.map(lower_expr).transpose()?,
    })
}

fn lower_join_clause(join: ast::JoinClause) -> Result<executor_ast::statements::query::JoinClause, DbError> {
    Ok(executor_ast::statements::query::JoinClause {
        join_type: match join.join_type {
            ast::JoinType::Inner => executor_ast::statements::query::JoinType::Inner,
            ast::JoinType::Left => executor_ast::statements::query::JoinType::Left,
            ast::JoinType::Right => executor_ast::statements::query::JoinType::Right,
            ast::JoinType::Full => executor_ast::statements::query::JoinType::Full,
            ast::JoinType::Cross => executor_ast::statements::query::JoinType::Cross,
        },
        table: lower_table_ref_recursive(join.table)?.0,
        on: join.on.map(lower_expr).transpose()?,
    })
}

fn lower_from_clause_internal(tables: Vec<ast::TableRef>) -> Result<(executor_ast::common::TableRef, Vec<executor_ast::statements::query::JoinClause>), DbError> {
    if tables.is_empty() {
        return Err(DbError::Parse("FROM clause must have at least one table".into()));
    }
    let mut iter = tables.into_iter();
    let first = iter.next().unwrap();
    let (tr, mut joins) = lower_table_ref_recursive(first)?;
    for t in iter {
        let (next_tr, mut next_j) = lower_table_ref_recursive(t)?;
        joins.push(executor_ast::statements::query::JoinClause {
            join_type: executor_ast::statements::query::JoinType::Cross,
            table: next_tr,
            on: None,
        });
        joins.append(&mut next_j);
    }
    Ok((tr, joins))
}

fn lower_table_ref_recursive(tr: ast::TableRef) -> Result<(executor_ast::common::TableRef, Vec<executor_ast::statements::query::JoinClause>), DbError> {
    let alias = tr.alias;
    let hints = tr.hints;
    let pivot = tr.pivot.map(|p| Box::new(executor_ast::common::PivotSpec {
        aggregate_func: p.aggregate_func,
        aggregate_col: p.aggregate_col,
        pivot_col: p.pivot_col,
        pivot_values: p.pivot_values,
    }));
    let unpivot = tr.unpivot.map(|u| Box::new(executor_ast::common::UnpivotSpec {
        value_col: u.value_col,
        pivot_col: u.pivot_col,
        column_list: u.column_list,
    }));

    match tr.factor {
        ast::TableFactor::Named(name) => Ok((executor_ast::common::TableRef {
            factor: executor_ast::common::TableFactor::Named(lower_object_name_owned(name)),
            alias,
            pivot,
            unpivot,
            hints,
        }, Vec::new())),
        ast::TableFactor::Values { rows, columns } => Ok((executor_ast::common::TableRef {
            factor: executor_ast::common::TableFactor::Values {
                rows: rows.into_iter().map(|r| r.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()).collect::<Result<Vec<_>, _>>()?,
                columns: columns,
            },
            alias,
            pivot,
            unpivot,
            hints,
        }, Vec::new())),
        ast::TableFactor::Derived(subquery) => Ok((executor_ast::common::TableRef {
            factor: executor_ast::common::TableFactor::Derived(Box::new(lower_select(*subquery)?)),
            alias,
            pivot,
            unpivot,
            hints,
        }, Vec::new())),
        ast::TableFactor::TableValuedFunction { name, args, alias: tvf_alias } => {
            let func_name = name.last().unwrap().to_string();
            let arg_strs: Vec<String> = args
                .into_iter()
                .map(|a| lower_expr(a).map(|expr| crate::executor::tooling::formatting::format_expr(&expr)))
                .collect::<Result<Vec<_>, _>>()?;
            let full_name = format!("{}({})", func_name, arg_strs.join(", "));
            Ok((executor_ast::common::TableRef {
                factor: executor_ast::common::TableFactor::Named(executor_ast::common::ObjectName {
                    schema: if name.len() > 1 { Some(name[0].to_string()) } else { None },
                    name: full_name,
                }),
                alias: tvf_alias.or(alias),
                pivot,
                unpivot,
                hints: Vec::new(),
            }, Vec::new()))
        }
    }
}

fn lower_object_name_owned(name: ast::ObjectName) -> executor_ast::ObjectName {
    executor_ast::ObjectName {
        schema: name.schema,
        name: name.name,
    }
}

pub fn lower_insert(s: ast::InsertStmt) -> Result<executor_ast::statements::dml::InsertStmt, DbError> {
    Ok(executor_ast::statements::dml::InsertStmt {
        table: lower_object_name(s.table),
        columns: if s.columns.is_empty() { None } else { Some(s.columns) },
        source: match s.source {
            ast::InsertSource::Values(rows) => executor_ast::statements::dml::InsertSource::Values(
                rows.into_iter().map(|r| r.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()).collect::<Result<Vec<_>, _>>()?
            ),
            ast::InsertSource::Select(sel) => executor_ast::statements::dml::InsertSource::Select(Box::new(lower_select(*sel)?)),
            ast::InsertSource::Exec { procedure, args } => executor_ast::statements::dml::InsertSource::Exec(Box::new(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::ExecProcedure(executor_ast::statements::procedural::ExecProcedureStmt {
                name: lower_object_name(procedure),
                args: args.into_iter().map(|e| Ok(executor_ast::statements::procedural::ExecArgument {
                    name: None, 
                    expr: lower_expr(e)?,
                    is_output: false,
                })).collect::<Result<Vec<_>, DbError>>()?,
            })))),
            ast::InsertSource::DefaultValues => executor_ast::statements::dml::InsertSource::DefaultValues,
        },
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_update(s: ast::UpdateStmt) -> Result<executor_ast::statements::dml::UpdateStmt, DbError> {
    let (table_tr, mut extra_joins) = lower_table_ref_recursive(s.table)?;
    let table = match table_tr.factor {
        executor_ast::common::TableFactor::Named(ref o) => o.clone(),
        _ => return Err(DbError::Parse("UPDATE target must be an object".into())),
    };
    
    for join in s.joins {
        extra_joins.push(lower_join_clause(join)?);
    }
    
    let mut from_clause = None;
    if let Some(from_refs) = s.from {
        let (tr, mut j) = lower_from_clause_internal(from_refs)?;
        extra_joins.append(&mut j);
        from_clause = Some(executor_ast::statements::dml::FromClause {
            tables: vec![tr],
            joins: extra_joins,
            applies: Vec::new(),
        });
    } else if !extra_joins.is_empty() {
        from_clause = Some(executor_ast::statements::dml::FromClause {
            tables: vec![table_tr],
            joins: extra_joins,
            applies: Vec::new(),
        });
    }

    Ok(executor_ast::statements::dml::UpdateStmt {
        table,
        assignments: s.assignments.into_iter().map(|a| Ok(executor_ast::statements::dml::Assignment {
            column: a.column,
            expr: lower_expr(a.expr)?,
        })).collect::<Result<Vec<_>, _>>()?,
        top: s.top.map(|e| Ok(executor_ast::statements::query::TopSpec { value: lower_expr(e)? })).transpose()?,
        from: from_clause,
        selection: s.selection.map(lower_expr).transpose()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_delete(s: ast::DeleteStmt) -> Result<executor_ast::statements::dml::DeleteStmt, DbError> {
    let table = lower_object_name(s.table);
    let (tr, mut joins) = lower_from_clause_internal(s.from)?;
    
    for join in s.joins {
        joins.push(lower_join_clause(join)?);
    }

    Ok(executor_ast::statements::dml::DeleteStmt {
        table,
        top: s.top.map(|e| Ok(executor_ast::statements::query::TopSpec { value: lower_expr(e)? })).transpose()?,
        from: Some(executor_ast::statements::dml::FromClause {
            tables: vec![tr],
            joins,
            applies: Vec::new(),
        }),
        selection: s.selection.map(lower_expr).transpose()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_merge(s: ast::MergeStmt) -> Result<executor_ast::statements::dml::MergeStmt, DbError> {
    let (target, _) = lower_table_ref_recursive(s.target)?;
    let (source_tr, _) = lower_table_ref_recursive(s.source)?;
    Ok(executor_ast::statements::dml::MergeStmt {
        target,
        source: executor_ast::statements::dml::MergeSource::Table(source_tr),
        on_condition: lower_expr(s.on_condition)?,
        when_clauses: s.when_clauses.into_iter().map(|w| Ok(executor_ast::statements::dml::MergeWhenClause {
            when: match w.when {
                ast::MergeWhen::Matched => executor_ast::statements::dml::MergeWhen::Matched,
                ast::MergeWhen::NotMatched => executor_ast::statements::dml::MergeWhen::NotMatched,
                ast::MergeWhen::NotMatchedBySource => executor_ast::statements::dml::MergeWhen::NotMatchedBySource,
            },
            condition: w.condition.map(lower_expr).transpose()?,
            action: match w.action {
                ast::MergeAction::Update { assignments } => executor_ast::statements::dml::MergeAction::Update {
                    assignments: assignments.into_iter().map(|a| Ok(executor_ast::statements::dml::Assignment {
                        column: a.column,
                        expr: lower_expr(a.expr)?,
                    })).collect::<Result<Vec<_>, _>>()?,
                },
                ast::MergeAction::Insert { columns, values } => executor_ast::statements::dml::MergeAction::Insert {
                    columns: columns,
                    values: values.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
                },
                ast::MergeAction::Delete => executor_ast::statements::dml::MergeAction::Delete,
            },
        })).collect::<Result<Vec<_>, _>>()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_create(s: ast::CreateStmt) -> Result<executor_ast::Statement, DbError> {
    match s {
        ast::CreateStmt::Table { name, columns, constraints } => Ok(executor_ast::Statement::Ddl(executor_ast::statements::DdlStatement::CreateTable(executor_ast::statements::ddl::CreateTableStmt {
            name: lower_object_name(name),
            columns: columns.into_iter().map(lower_column_def).collect::<Result<Vec<_>, _>>()?,
            table_constraints: constraints.into_iter().map(lower_table_constraint).collect(),
        }))),
        ast::CreateStmt::View { name, query } => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::CreateView(executor_ast::statements::ddl::CreateViewStmt {
            name: lower_object_name(name),
            query: lower_select(query)?,
        }))),
        ast::CreateStmt::Procedure { name, params, body } => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::CreateProcedure(executor_ast::statements::procedural::CreateProcedureStmt {
            name: lower_object_name(name),
            params: params.into_iter().map(lower_routine_param).collect::<Result<Vec<_>, _>>()?,
            body: body.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?,
        }))),
        ast::CreateStmt::Function { name, params, returns, body } => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::CreateFunction(executor_ast::statements::procedural::CreateFunctionStmt {
            name: lower_object_name(name),
            params: params.into_iter().map(lower_routine_param).collect::<Result<Vec<_>, _>>()?,
            returns: returns.map(lower_data_type).transpose()?,
            body: match body {
                ast::FunctionBody::ScalarReturn(e) => executor_ast::statements::procedural::FunctionBody::ScalarReturn(lower_expr(e)?),
                ast::FunctionBody::Block(stmts) => executor_ast::statements::procedural::FunctionBody::Scalar(stmts.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?),
                ast::FunctionBody::Table(sel) => executor_ast::statements::procedural::FunctionBody::InlineTable(lower_select(sel)?),
            },
        }))),
        ast::CreateStmt::Trigger { name, table, events, is_instead_of, body } => Ok(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::CreateTrigger(executor_ast::statements::procedural::CreateTriggerStmt {
            name: lower_object_name(name),
            table: lower_object_name(table),
            events: events.into_iter().map(|e| match e {
                ast::TriggerEvent::Insert => executor_ast::TriggerEvent::Insert,
                ast::TriggerEvent::Update => executor_ast::TriggerEvent::Update,
                ast::TriggerEvent::Delete => executor_ast::TriggerEvent::Delete,
            }).collect(),
            is_instead_of,
            body: body.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?,
        }))),
    }
}

pub fn lower_column_def(c: ast::ColumnDef) -> Result<executor_ast::statements::ddl::ColumnSpec, DbError> {
    Ok(executor_ast::statements::ddl::ColumnSpec {
        name: c.name,
        data_type: lower_data_type(c.data_type)?,
        nullable: c.is_nullable.unwrap_or(true),
        identity: c.identity_spec,
        primary_key: c.is_primary_key,
        unique: c.is_unique,
        default: c.default_expr.map(lower_expr).transpose()?,
        default_constraint_name: c.default_constraint_name,
        check: c.check_expr.map(lower_expr).transpose()?,
        check_constraint_name: c.check_constraint_name,
        computed_expr: c.computed_expr.map(lower_expr).transpose()?,
        foreign_key: c.foreign_key.map(|fk| executor_ast::statements::ddl::ForeignKeyRef {
            referenced_table: lower_object_name(fk.ref_table),
            referenced_columns: fk.ref_columns,
            on_delete: fk.on_delete.map(lower_referential_action),
            on_update: fk.on_update.map(lower_referential_action),
        }),
    })
}

pub fn lower_routine_param(p: ast::RoutineParam) -> Result<executor_ast::statements::RoutineParam, DbError> {
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

pub fn lower_output_column(c: ast::OutputColumn) -> executor_ast::statements::dml::OutputColumn {
    executor_ast::statements::dml::OutputColumn {
        source: match c.source {
            ast::OutputSource::Inserted => executor_ast::statements::dml::OutputSource::Inserted,
            ast::OutputSource::Deleted => executor_ast::statements::dml::OutputSource::Deleted,
        },
        column: c.column,
        alias: c.alias,
        is_wildcard: c.is_wildcard,
    }
}

pub fn lower_order_by_expr(o: ast::OrderByExpr) -> Result<executor_ast::statements::query::OrderByExpr, DbError> {
    Ok(executor_ast::statements::query::OrderByExpr {
        expr: lower_expr(o.expr)?,
        asc: o.asc,
    })
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

pub fn lower_alter_action(a: ast::AlterTableAction) -> Result<executor_ast::statements::ddl::AlterTableAction, DbError> {
    match a {
        ast::AlterTableAction::AddColumn(c) => Ok(executor_ast::statements::ddl::AlterTableAction::AddColumn(lower_column_def(c)?)),
        ast::AlterTableAction::DropColumn(c) => Ok(executor_ast::statements::ddl::AlterTableAction::DropColumn(c)),
        ast::AlterTableAction::AddConstraint(c) => Ok(executor_ast::statements::ddl::AlterTableAction::AddConstraint(lower_table_constraint(c))),
        ast::AlterTableAction::DropConstraint(c) => Ok(executor_ast::statements::ddl::AlterTableAction::DropConstraint(c)),
    }
}

pub fn lower_table_constraint(c: ast::TableConstraint) -> executor_ast::statements::ddl::TableConstraintSpec {
    match c {
        ast::TableConstraint::PrimaryKey { name, columns } => executor_ast::statements::ddl::TableConstraintSpec::PrimaryKey {
            name: name.unwrap_or_default(),
            columns: columns,
        },
        ast::TableConstraint::Unique { name, columns } => executor_ast::statements::ddl::TableConstraintSpec::Unique {
            name: name.unwrap_or_default(),
            columns: columns,
        },
        ast::TableConstraint::ForeignKey { name, columns, ref_table, ref_columns, on_delete, on_update } => executor_ast::statements::ddl::TableConstraintSpec::ForeignKey {
            name: name.unwrap_or_default(),
            columns: columns,
            referenced_table: lower_object_name(ref_table),
            referenced_columns: ref_columns,
            on_delete: on_delete.map(lower_referential_action),
            on_update: on_update.map(lower_referential_action),
        },
        ast::TableConstraint::Check { name, expr } => executor_ast::statements::ddl::TableConstraintSpec::Check {
            name: name.unwrap_or_default(),
            expr: lower_expr(expr).unwrap(),
        },
        ast::TableConstraint::Default { name, column, expr } => executor_ast::statements::ddl::TableConstraintSpec::Default {
            name: name.unwrap_or_default(),
            column: column,
            expr: lower_expr(expr).unwrap(),
        },
    }
}

pub fn lower_referential_action(a: ast::ReferentialAction) -> executor_ast::statements::ddl::ReferentialAction {
    match a {
        ast::ReferentialAction::NoAction => executor_ast::statements::ddl::ReferentialAction::NoAction,
        ast::ReferentialAction::Cascade => executor_ast::statements::ddl::ReferentialAction::Cascade,
        ast::ReferentialAction::SetNull => executor_ast::statements::ddl::ReferentialAction::SetNull,
        ast::ReferentialAction::SetDefault => executor_ast::statements::ddl::ReferentialAction::SetDefault,
    }
}
