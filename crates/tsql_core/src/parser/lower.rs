use crate::parser::ast as v2;
use crate::ast as old;
use crate::error::DbError;
use std::borrow::Cow;

pub fn lower_batch<'a>(v2_stmts: Vec<v2::Statement<'a>>) -> Result<Vec<old::Statement>, DbError> {
    v2_stmts.into_iter().map(lower_statement).collect()
}

pub fn lower_statement<'a>(v2_stmt: v2::Statement<'a>) -> Result<old::Statement, DbError> {
    match v2_stmt {
        v2::Statement::Select(s) => {
            if let Some(ref op) = s.set_op {
                let mut left_v2 = (*s).clone();
                left_v2.set_op = None;
                let left = lower_select(left_v2)?;
                let right = lower_select(op.right.clone())?;
                return Ok(old::Statement::SetOp(old::statements::query::SetOpStmt {
                    left: Box::new(old::Statement::Select(left)),
                    op: match op.kind {
                        v2::SetOpKind::Union => old::statements::query::SetOpKind::Union,
                        v2::SetOpKind::UnionAll => old::statements::query::SetOpKind::UnionAll,
                        v2::SetOpKind::Intersect => old::statements::query::SetOpKind::Intersect,
                        v2::SetOpKind::Except => old::statements::query::SetOpKind::Except,
                    },
                    right: Box::new(old::Statement::Select(right)),
                }));
            }
            Ok(old::Statement::Select(lower_select(*s)?))
        }
        v2::Statement::Insert(s) => Ok(old::Statement::Insert(lower_insert(*s)?)),
        v2::Statement::Update(s) => Ok(old::Statement::Update(lower_update(*s)?)),
        v2::Statement::Delete(s) => Ok(old::Statement::Delete(lower_delete(*s)?)),
        v2::Statement::Merge(s) => Ok(old::Statement::Merge(lower_merge(*s)?)),
        v2::Statement::Declare(vars) => {
            if vars.len() == 1 {
                let var = vars.into_iter().next().unwrap();
                Ok(old::Statement::Declare(old::statements::procedural::DeclareStmt {
                    name: var.name.into_owned(),
                    data_type: lower_data_type(var.data_type)?,
                    default: var.initial_value.map(lower_expr).transpose()?,
                }))
            } else {
                let mut stmts = Vec::new();
                for var in vars {
                    stmts.push(old::Statement::Declare(old::statements::procedural::DeclareStmt {
                        name: var.name.into_owned(),
                        data_type: lower_data_type(var.data_type)?,
                        default: var.initial_value.map(lower_expr).transpose()?,
                    }));
                }
                Ok(old::Statement::BeginEnd(stmts))
            }
        }
        v2::Statement::Set { variable, expr } => Ok(old::Statement::Set(old::statements::procedural::SetStmt {
            name: variable.into_owned(),
            expr: lower_expr(expr)?,
        })),
        v2::Statement::If { condition, then_stmt, else_stmt } => {
            let then_body = match lower_statement(*then_stmt)? {
                old::Statement::BeginEnd(stmts) => stmts,
                other => vec![other],
            };
            let else_body = match else_stmt {
                Some(s) => Some(match lower_statement(*s)? {
                    old::Statement::BeginEnd(stmts) => stmts,
                    other => vec![other],
                }),
                None => None,
            };
            Ok(old::Statement::If(old::statements::procedural::IfStmt {
                condition: lower_expr(condition)?,
                then_body,
                else_body,
            }))
        }
        v2::Statement::BeginEnd(stmts) => {
            Ok(old::Statement::BeginEnd(stmts.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?))
        }
        v2::Statement::While { condition, stmt } => {
            let body = match lower_statement(*stmt)? {
                old::Statement::BeginEnd(stmts) => stmts,
                other => vec![other],
            };
            Ok(old::Statement::While(old::statements::procedural::WhileStmt {
                condition: lower_expr(condition)?,
                body,
            }))
        }
        v2::Statement::Print(expr) => Ok(old::Statement::Print(lower_expr(expr)?)),
        v2::Statement::Break => Ok(old::Statement::Break),
        v2::Statement::Continue => Ok(old::Statement::Continue),
        v2::Statement::Return(expr) => Ok(old::Statement::Return(expr.map(lower_expr).transpose()?)),
        v2::Statement::Create(s) => lower_create(*s),
        v2::Statement::BeginTransaction(name) => Ok(old::Statement::BeginTransaction(name.map(|n| n.into_owned()))),
        v2::Statement::CommitTransaction(name) => Ok(old::Statement::CommitTransaction(name.map(|n| n.into_owned()))),
        v2::Statement::RollbackTransaction(name) => Ok(old::Statement::RollbackTransaction(name.map(|n| n.into_owned()))),
        v2::Statement::SaveTransaction(name) => Ok(old::Statement::SaveTransaction(name.into_owned())),
        v2::Statement::SetTransactionIsolationLevel(iso) => Ok(old::Statement::SetTransactionIsolationLevel(iso)),
        v2::Statement::SetOption { option, value } => Ok(old::Statement::SetOption(old::statements::procedural::SetOptionStmt { 
            option, 
            value
        })),
        v2::Statement::SetIdentityInsert { table, on } => Ok(old::Statement::SetIdentityInsert(old::statements::SetIdentityInsertStmt {
            table: lower_object_name(table),
            on,
        })),
        v2::Statement::DropTable(table) => Ok(old::Statement::DropTable(old::statements::ddl::DropTableStmt {
            name: lower_object_name(table),
        })),
        v2::Statement::DropView(name) => Ok(old::Statement::DropView(old::statements::ddl::DropViewStmt {
            name: lower_object_name(name),
        })),
        v2::Statement::DropProcedure(name) => Ok(old::Statement::DropProcedure(old::statements::procedural::DropProcedureStmt {
            name: lower_object_name(name),
        })),
        v2::Statement::TruncateTable(table) => Ok(old::Statement::TruncateTable(old::statements::ddl::TruncateTableStmt {
            name: lower_object_name(table),
        })),
        v2::Statement::WithCte { ctes, body } => {
            let mut old_ctes = Vec::new();
            for cte in ctes {
                old_ctes.push(old::statements::procedural::CteDef {
                    name: cte.name.into_owned(),
                    query: lower_statement(v2::Statement::Select(Box::new(cte.query)))?,
                });
            }
            Ok(old::Statement::WithCte(old::statements::procedural::WithCteStmt {
                recursive: false,
                ctes: old_ctes,
                body: Box::new(lower_statement(*body)?),
            }))
        }
        v2::Statement::AlterTable { table, action } => {
            Ok(old::Statement::AlterTable(old::statements::ddl::AlterTableStmt {
                table: lower_object_name(table),
                action: lower_alter_action(action)?,
            }))
        }
        v2::Statement::CreateIndex { name, table, columns } => {
            Ok(old::Statement::CreateIndex(old::statements::ddl::CreateIndexStmt {
                name: lower_object_name(name),
                table: lower_object_name(table),
                columns: columns.into_iter().map(|c| c.into_owned()).collect(),
            }))
        }
        v2::Statement::DropIndex { name, table } => {
            Ok(old::Statement::DropIndex(old::statements::ddl::DropIndexStmt {
                name: lower_object_name(name),
                table: lower_object_name(table),
            }))
        }
        v2::Statement::CreateType { name, columns } => {
            Ok(old::Statement::CreateType(old::statements::ddl::CreateTypeStmt {
                name: lower_object_name(name),
                columns: columns.into_iter().map(lower_column_def).collect::<Result<Vec<_>, _>>()?,
                table_constraints: Vec::new(),
            }))
        }
        v2::Statement::DropType(name) => Ok(old::Statement::DropType(old::statements::ddl::DropTypeStmt {
            name: lower_object_name(name),
        })),
        v2::Statement::CreateSchema(name) => Ok(old::Statement::CreateSchema(old::statements::ddl::CreateSchemaStmt {
            name: name.into_owned(),
        })),
        v2::Statement::DropSchema(name) => Ok(old::Statement::DropSchema(old::statements::ddl::DropSchemaStmt {
            name: name.into_owned(),
        })),
        v2::Statement::DropFunction(name) => Ok(old::Statement::DropFunction(old::statements::procedural::DropFunctionStmt {
            name: lower_object_name(name),
        })),
        v2::Statement::DropTrigger(name) => Ok(old::Statement::DropTrigger(old::statements::procedural::DropTriggerStmt {
            name: lower_object_name(name),
        })),
        v2::Statement::TryCatch { try_body, catch_body } => {
            Ok(old::Statement::TryCatch(old::statements::procedural::TryCatchStmt {
                try_body: try_body.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?,
                catch_body: catch_body.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?,
            }))
        }
        v2::Statement::Raiserror { message, severity, state } => {
            Ok(old::Statement::Raiserror(old::statements::procedural::RaiserrorStmt {
                message: lower_expr(message)?,
                severity: lower_expr(severity)?,
                state: lower_expr(state)?,
            }))
        }
        v2::Statement::DeclareTableVar { name, columns, constraints } => {
            Ok(old::Statement::DeclareTableVar(old::statements::procedural::DeclareTableVarStmt {
                name: name.into_owned(),
                columns: columns.into_iter().map(lower_column_def).collect::<Result<Vec<_>, _>>()?,
                table_constraints: constraints.into_iter().map(lower_table_constraint).collect(),
            }))
        }
        v2::Statement::DeclareCursor { name, query } => {
            Ok(old::Statement::DeclareCursor(old::statements::procedural::DeclareCursorStmt {
                name: name.into_owned(),
                query: lower_select(query)?,
            }))
        }
        v2::Statement::OpenCursor(name) => Ok(old::Statement::OpenCursor(name.into_owned())),
        v2::Statement::FetchCursor { name, direction, into_vars } => {
            Ok(old::Statement::FetchCursor(old::statements::procedural::FetchCursorStmt {
                name: name.into_owned(),
                direction: lower_fetch_direction(direction)?,
                into: into_vars.map(|v| v.into_iter().map(|i| i.into_owned()).collect()),
            }))
        }
        v2::Statement::CloseCursor(name) => Ok(old::Statement::CloseCursor(name.into_owned())),
        v2::Statement::DeallocateCursor(name) => Ok(old::Statement::DeallocateCursor(name.into_owned())),
        v2::Statement::SelectAssign { assignments, from, selection } => {
            let mut joins = Vec::new();
            let from_tr = if let Some(from_refs) = from {
                let (tr, mut j) = lower_from_clause_internal(from_refs)?;
                joins.append(&mut j);
                Some(tr)
            } else {
                None
            };
            Ok(old::Statement::SelectAssign(old::statements::procedural::SelectAssignStmt {
                targets: assignments.into_iter().map(|a| Ok(old::statements::procedural::SelectAssignTarget {
                    variable: a.variable.into_owned(),
                    expr: lower_expr(a.expr)?,
                })).collect::<Result<Vec<_>, _>>()?,
                from: from_tr,
                joins,
                selection: selection.map(lower_expr).transpose()?,
            }))
        }
        v2::Statement::ExecDynamic { sql_expr } => {
            Ok(old::Statement::ExecDynamic(old::statements::procedural::ExecStmt {
                sql_expr: lower_expr(sql_expr)?,
            }))
        }
        v2::Statement::ExecProcedure { name, args } => {
            Ok(old::Statement::ExecProcedure(old::statements::procedural::ExecProcedureStmt {
                name: lower_object_name(name),
                args: args.into_iter().map(lower_exec_arg).collect::<Result<Vec<_>, _>>()?,
            }))
        }
        v2::Statement::SpExecuteSql { sql_expr, params_def, args } => {
            Ok(old::Statement::SpExecuteSql(old::statements::procedural::SpExecuteSqlStmt {
                sql_expr: lower_expr(sql_expr)?,
                params_def: params_def.map(lower_expr).transpose()?,
                args: args.into_iter().map(lower_exec_arg).collect::<Result<Vec<_>, _>>()?,
            }))
        }
    }
}

pub fn lower_expr(v2_expr: v2::Expr) -> Result<old::expressions::Expr, DbError> {
    match v2_expr {
        v2::Expr::Identifier(id) => Ok(old::expressions::Expr::Identifier(id.into_owned())),
        v2::Expr::Variable(id) => Ok(old::expressions::Expr::Identifier(id.into_owned())),
        v2::Expr::QualifiedIdentifier(parts) => Ok(old::expressions::Expr::QualifiedIdentifier(parts.into_iter().map(|p| p.into_owned()).collect())),
        v2::Expr::Wildcard => Ok(old::expressions::Expr::Wildcard),
        v2::Expr::Integer(i) => Ok(old::expressions::Expr::Integer(i)),
        v2::Expr::Float(f) => Ok(old::expressions::Expr::FloatLiteral(f64::from_bits(f).to_string())),
        v2::Expr::String(s) => Ok(old::expressions::Expr::String(s.into_owned())),
        v2::Expr::UnicodeString(s) => Ok(old::expressions::Expr::UnicodeString(s.into_owned())),
        v2::Expr::BinaryLiteral(b) => Ok(old::expressions::Expr::BinaryLiteral(b)),
        v2::Expr::Null => Ok(old::expressions::Expr::Null),
        v2::Expr::Bool(b) => Ok(old::expressions::Expr::Integer(if b { 1 } else { 0 })),
        v2::Expr::Binary { left, op, right } => {
             match op {
                 v2::BinaryOp::Like => Ok(old::expressions::Expr::Like {
                     expr: Box::new(lower_expr(*left)?),
                     pattern: Box::new(lower_expr(*right)?),
                     negated: false,
                 }),
                 _ => Ok(old::expressions::Expr::Binary {
                     left: Box::new(lower_expr(*left)?),
                     op: lower_binary_op(op)?,
                     right: Box::new(lower_expr(*right)?),
                 })
             }
        }
        v2::Expr::Unary { op, expr } => Ok(old::expressions::Expr::Unary {
            op: lower_unary_op(op),
            expr: Box::new(lower_expr(*expr)?),
        }),
        v2::Expr::IsNull(expr) => Ok(old::expressions::Expr::IsNull(Box::new(lower_expr(*expr)?))),
        v2::Expr::IsNotNull(expr) => Ok(old::expressions::Expr::IsNotNull(Box::new(lower_expr(*expr)?))),
        v2::Expr::Cast { expr, target } => Ok(old::expressions::Expr::Cast {
            expr: Box::new(lower_expr(*expr)?),
            target: lower_data_type(target)?,
        }),
        v2::Expr::TryCast { expr, target } => Ok(old::expressions::Expr::TryCast {
            expr: Box::new(lower_expr(*expr)?),
            target: lower_data_type(target)?,
        }),
        v2::Expr::Convert { target, expr, style } => Ok(old::expressions::Expr::Convert {
            target: lower_data_type(target)?,
            expr: Box::new(lower_expr(*expr)?),
            style,
        }),
        v2::Expr::TryConvert { target, expr, style } => Ok(old::expressions::Expr::TryConvert {
            target: lower_data_type(target)?,
            expr: Box::new(lower_expr(*expr)?),
            style,
        }),
        v2::Expr::Case { operand, when_clauses, else_result } => Ok(old::expressions::Expr::Case {
            operand: operand.map(|e| lower_expr(*e)).transpose()?.map(Box::new),
            when_clauses: when_clauses.into_iter().map(|w| Ok(old::expressions::WhenClause {
                condition: lower_expr(w.condition)?,
                result: lower_expr(w.result)?,
            })).collect::<Result<Vec<_>, _>>()?,
            else_result: else_result.map(|e| lower_expr(*e)).transpose()?.map(Box::new),
        }),
        v2::Expr::InList { expr, list, negated } => Ok(old::expressions::Expr::InList {
            expr: Box::new(lower_expr(*expr)?),
            list: list.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
            negated,
        }),
        v2::Expr::InSubquery { expr, subquery, negated } => Ok(old::expressions::Expr::InSubquery {
            expr: Box::new(lower_expr(*expr)?),
            subquery: Box::new(lower_select(*subquery)?),
            negated,
        }),
        v2::Expr::Between { expr, low, high, negated } => Ok(old::expressions::Expr::Between {
            expr: Box::new(lower_expr(*expr)?),
            low: Box::new(lower_expr(*low)?),
            high: Box::new(lower_expr(*high)?),
            negated,
        }),
        v2::Expr::Like { expr, pattern, negated } => Ok(old::expressions::Expr::Like {
            expr: Box::new(lower_expr(*expr)?),
            pattern: Box::new(lower_expr(*pattern)?),
            negated,
        }),
        v2::Expr::Exists { subquery, negated } => Ok(old::expressions::Expr::Exists {
            subquery: Box::new(lower_select(*subquery)?),
            negated,
        }),
        v2::Expr::Subquery(s) => Ok(old::expressions::Expr::Subquery(Box::new(lower_select(*s)?))),
        v2::Expr::FunctionCall { name, args } => Ok(old::expressions::Expr::FunctionCall {
            name: name.into_owned(),
            args: args.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
        }),
        v2::Expr::WindowFunction { name, args, partition_by, order_by, frame } => {
            Ok(old::expressions::Expr::WindowFunction {
                func: lower_window_func(name.as_ref()),
                args: args.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
                partition_by: partition_by.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
                order_by: order_by.into_iter().map(lower_order_by_expr).collect::<Result<Vec<_>, _>>()?,
                frame: frame.map(lower_window_frame),
            })
        }
    }
}

pub fn lower_binary_op(op: v2::BinaryOp) -> Result<old::expressions::BinaryOp, DbError> {
    match op {
        v2::BinaryOp::Eq => Ok(old::expressions::BinaryOp::Eq),
        v2::BinaryOp::NotEq => Ok(old::expressions::BinaryOp::NotEq),
        v2::BinaryOp::Gt => Ok(old::expressions::BinaryOp::Gt),
        v2::BinaryOp::Lt => Ok(old::expressions::BinaryOp::Lt),
        v2::BinaryOp::Gte => Ok(old::expressions::BinaryOp::Gte),
        v2::BinaryOp::Lte => Ok(old::expressions::BinaryOp::Lte),
        v2::BinaryOp::And => Ok(old::expressions::BinaryOp::And),
        v2::BinaryOp::Or => Ok(old::expressions::BinaryOp::Or),
        v2::BinaryOp::Add => Ok(old::expressions::BinaryOp::Add),
        v2::BinaryOp::Subtract => Ok(old::expressions::BinaryOp::Subtract),
        v2::BinaryOp::Multiply => Ok(old::expressions::BinaryOp::Multiply),
        v2::BinaryOp::Divide => Ok(old::expressions::BinaryOp::Divide),
        v2::BinaryOp::Modulo => Ok(old::expressions::BinaryOp::Modulo),
        _ => Err(DbError::Parse(format!("Binary operator {:?} not supported in old AST", op))),
    }
}

pub fn lower_unary_op(op: v2::UnaryOp) -> old::expressions::UnaryOp {
    match op {
        v2::UnaryOp::Negate => old::expressions::UnaryOp::Negate,
        v2::UnaryOp::Not => old::expressions::UnaryOp::Not,
        v2::UnaryOp::BitwiseNot => old::expressions::UnaryOp::Not, // fallback
    }
}

pub fn lower_window_func(name: &str) -> old::expressions::WindowFunc {
    match name.to_uppercase().as_str() {
        "ROW_NUMBER" => old::expressions::WindowFunc::RowNumber,
        "RANK" => old::expressions::WindowFunc::Rank,
        "DENSE_RANK" => old::expressions::WindowFunc::DenseRank,
        "NTILE" => old::expressions::WindowFunc::NTile,
        "LAG" => old::expressions::WindowFunc::Lag,
        "LEAD" => old::expressions::WindowFunc::Lead,
        "FIRST_VALUE" => old::expressions::WindowFunc::FirstValue,
        "LAST_VALUE" => old::expressions::WindowFunc::LastValue,
        "PERCENTILE_CONT" => old::expressions::WindowFunc::PercentileCont,
        "PERCENTILE_DISC" => old::expressions::WindowFunc::PercentileDisc,
        "PERCENT_RANK" => old::expressions::WindowFunc::PercentileRank,
        _ => old::expressions::WindowFunc::Aggregate(name.to_string()),
    }
}

pub fn lower_window_frame(frame: v2::WindowFrame) -> old::expressions::WindowFrame {
    old::expressions::WindowFrame {
        units: match frame.units {
            v2::WindowFrameUnits::Rows => old::expressions::WindowFrameUnits::Rows,
            v2::WindowFrameUnits::Range => old::expressions::WindowFrameUnits::Range,
            v2::WindowFrameUnits::Groups => old::expressions::WindowFrameUnits::Groups,
        },
        extent: match frame.extent {
            v2::WindowFrameExtent::Bound(b) => old::expressions::WindowFrameExtent::Bound(lower_window_bound(b)),
            v2::WindowFrameExtent::Between(b1, b2) => old::expressions::WindowFrameExtent::Between(lower_window_bound(b1), lower_window_bound(b2)),
        }
    }
}

pub fn lower_window_bound(bound: v2::WindowFrameBound) -> old::expressions::WindowFrameBound {
    match bound {
        v2::WindowFrameBound::UnboundedPreceding => old::expressions::WindowFrameBound::UnboundedPreceding,
        v2::WindowFrameBound::Preceding(n) => old::expressions::WindowFrameBound::Preceding(n),
        v2::WindowFrameBound::CurrentRow => old::expressions::WindowFrameBound::CurrentRow,
        v2::WindowFrameBound::Following(n) => old::expressions::WindowFrameBound::Following(n),
        v2::WindowFrameBound::UnboundedFollowing => old::expressions::WindowFrameBound::UnboundedFollowing,
    }
}

pub fn lower_object_name<'a>(parts: Vec<Cow<'a, str>>) -> old::ObjectName {
    let mut parts_owned: Vec<String> = parts.into_iter().map(|p| p.into_owned()).collect();
    if parts_owned.len() == 1 {
        old::ObjectName { schema: None, name: parts_owned.remove(0) }
    } else if parts_owned.len() == 2 {
        let name = parts_owned.pop().unwrap();
        let schema = Some(parts_owned.pop().unwrap());
        old::ObjectName { schema, name }
    } else {
        let name = parts_owned.pop().unwrap();
        let schema = Some(parts_owned.pop().unwrap());
        old::ObjectName { schema, name }
    }
}

pub fn lower_data_type<'a>(dt: v2::DataType<'a>) -> Result<old::data_types::DataTypeSpec, DbError> {
    match dt {
        v2::DataType::Int => Ok(old::data_types::DataTypeSpec::Int),
        v2::DataType::BigInt => Ok(old::data_types::DataTypeSpec::BigInt),
        v2::DataType::SmallInt => Ok(old::data_types::DataTypeSpec::SmallInt),
        v2::DataType::TinyInt => Ok(old::data_types::DataTypeSpec::TinyInt),
        v2::DataType::Bit => Ok(old::data_types::DataTypeSpec::Bit),
        v2::DataType::Float => Ok(old::data_types::DataTypeSpec::Float),
        v2::DataType::Decimal(p, s) => Ok(old::data_types::DataTypeSpec::Decimal(p, s)),
        v2::DataType::Numeric(p, s) => Ok(old::data_types::DataTypeSpec::Numeric(p, s)),
        v2::DataType::VarChar(n) => Ok(old::data_types::DataTypeSpec::VarChar(n.unwrap_or(u16::MAX as u32) as u16)),
        v2::DataType::NVarChar(n) => Ok(old::data_types::DataTypeSpec::NVarChar(n.unwrap_or(u16::MAX as u32) as u16)),
        v2::DataType::Char(n) => Ok(old::data_types::DataTypeSpec::Char(n.unwrap_or(1) as u16)),
        v2::DataType::NChar(n) => Ok(old::data_types::DataTypeSpec::NChar(n.unwrap_or(1) as u16)),
        v2::DataType::Binary(n) => Ok(old::data_types::DataTypeSpec::Binary(n.unwrap_or(1) as u16)),
        v2::DataType::VarBinary(n) => Ok(old::data_types::DataTypeSpec::VarBinary(n.unwrap_or(u16::MAX as u32) as u16)),
        v2::DataType::Date => Ok(old::data_types::DataTypeSpec::Date),
        v2::DataType::Time => Ok(old::data_types::DataTypeSpec::Time),
        v2::DataType::DateTime => Ok(old::data_types::DataTypeSpec::DateTime),
        v2::DataType::DateTime2 => Ok(old::data_types::DataTypeSpec::DateTime2),
        v2::DataType::Money => Ok(old::data_types::DataTypeSpec::Money),
        v2::DataType::SmallMoney => Ok(old::data_types::DataTypeSpec::SmallMoney),
        v2::DataType::UniqueIdentifier => Ok(old::data_types::DataTypeSpec::UniqueIdentifier),
        v2::DataType::SqlVariant => Ok(old::data_types::DataTypeSpec::SqlVariant),
        v2::DataType::Xml => Ok(old::data_types::DataTypeSpec::Xml),
        v2::DataType::Custom(_) => Ok(old::data_types::DataTypeSpec::VarChar(255)), // Fallback
        _ => Err(DbError::Parse(format!("Data type {:?} not supported in old AST", dt))),
    }
}

pub fn lower_select<'a>(s: v2::SelectStmt<'a>) -> Result<old::statements::query::SelectStmt, DbError> {
    if s.set_op.is_some() {
        return Err(DbError::Parse("Subqueries with UNION/INTERSECT/EXCEPT not yet supported in this version".into()));
    }

    let mut joins = Vec::new();
    let mut from = None;
    
    if let Some(from_refs) = s.from {
        let (tr, mut j) = lower_from_clause_internal(from_refs)?;
        from = Some(tr);
        joins.append(&mut j);
    }

    Ok(old::statements::query::SelectStmt {
        distinct: s.distinct,
        top: s.top.map(|e| Ok(old::statements::query::TopSpec { value: lower_expr(e)? })).transpose()?,
        projection: s.projection.into_iter().map(|i| Ok(old::statements::query::SelectItem {
            expr: lower_expr(i.expr)?,
            alias: i.alias.map(|a| a.into_owned()),
        })).collect::<Result<Vec<_>, DbError>>()?,
        into_table: s.into_table.map(lower_object_name),
        from,
        joins,
        applies: s.applies.into_iter().map(|a| Ok(old::statements::query::ApplyClause {
            apply_type: match a.apply_type {
                v2::ApplyType::Cross => old::statements::query::ApplyType::Cross,
                v2::ApplyType::Outer => old::statements::query::ApplyType::Outer,
            },
            subquery: lower_select(*a.subquery)?,
            alias: a.alias.into_owned(),
        })).collect::<Result<Vec<_>, DbError>>()?,
        selection: s.selection.map(lower_expr).transpose()?,
        group_by: s.group_by.into_iter().map(lower_expr).collect::<Result<Vec<_>, DbError>>()?,
        having: s.having.map(lower_expr).transpose()?,
        order_by: s.order_by.into_iter().map(lower_order_by_expr).collect::<Result<Vec<_>, DbError>>()?,
        offset: s.offset.map(lower_expr).transpose()?,
        fetch: s.fetch.map(lower_expr).transpose()?,
    })
}

fn lower_from_clause_internal<'a>(tables: Vec<v2::TableRef<'a>>) -> Result<(old::common::TableRef, Vec<old::statements::query::JoinClause>), DbError> {
    if tables.is_empty() {
        return Err(DbError::Parse("FROM clause must have at least one table".into()));
    }
    let mut iter = tables.into_iter();
    let first = iter.next().unwrap();
    let (tr, mut joins) = lower_table_ref_recursive(first)?;
    for t in iter {
        let (next_tr, mut next_j) = lower_table_ref_recursive(t)?;
        joins.push(old::statements::query::JoinClause {
            join_type: old::statements::query::JoinType::Cross,
            table: next_tr,
            on: None,
        });
        joins.append(&mut next_j);
    }
    Ok((tr, joins))
}

fn lower_table_ref_recursive<'a>(tr: v2::TableRef<'a>) -> Result<(old::common::TableRef, Vec<old::statements::query::JoinClause>), DbError> {
    match tr {
        v2::TableRef::Table { name, alias, hints } => {
            Ok((old::common::TableRef {
                name: old::common::TableName::Object(lower_object_name(name)),
                alias: alias.map(|a| a.into_owned()),
                pivot: None,
                unpivot: None,
                hints: hints.into_iter().map(|h| h.into_owned()).collect(),
            }, Vec::new()))
        }
        v2::TableRef::Subquery { subquery, alias } => {
            Ok((old::common::TableRef {
                name: old::common::TableName::Subquery(Box::new(lower_select(*subquery)?)),
                alias: Some(alias.into_owned()),
                pivot: None,
                unpivot: None,
                hints: Vec::new(),
            }, Vec::new()))
        }
        v2::TableRef::Join { left, join_type, right, on } => {
            let (l_tr, mut l_joins) = lower_table_ref_recursive(*left)?;
            let (r_tr, mut r_joins) = lower_table_ref_recursive(*right)?;
            l_joins.push(old::statements::query::JoinClause {
                join_type: match join_type {
                    v2::JoinType::Inner => old::statements::query::JoinType::Inner,
                    v2::JoinType::Left => old::statements::query::JoinType::Left,
                    v2::JoinType::Right => old::statements::query::JoinType::Right,
                    v2::JoinType::Full => old::statements::query::JoinType::Full,
                    v2::JoinType::Cross => old::statements::query::JoinType::Cross,
                },
                table: r_tr,
                on: on.map(lower_expr).transpose()?,
            });
            l_joins.append(&mut r_joins);
            Ok((l_tr, l_joins))
        }
        v2::TableRef::Pivot { source, spec, alias } => {
            let (mut tr, joins) = lower_table_ref_recursive(*source)?;
            tr.pivot = Some(Box::new(old::common::PivotSpec {
                aggregate_func: spec.aggregate_func.into_owned(),
                aggregate_col: spec.aggregate_col.into_owned(),
                pivot_col: spec.pivot_col.into_owned(),
                pivot_values: spec.pivot_values.into_iter().map(|v| v.into_owned()).collect(),
            }));
            tr.alias = Some(alias.into_owned());
            Ok((tr, joins))
        }
        v2::TableRef::Unpivot { source, spec, alias } => {
            let (mut tr, joins) = lower_table_ref_recursive(*source)?;
            tr.unpivot = Some(Box::new(old::common::UnpivotSpec {
                value_col: spec.value_col.into_owned(),
                pivot_col: spec.pivot_col.into_owned(),
                column_list: spec.column_list.into_iter().map(|c| c.into_owned()).collect(),
            }));
            tr.alias = Some(alias.into_owned());
            Ok((tr, joins))
        }
        v2::TableRef::TableValuedFunction { name, args, alias } => {
            // Convert to a table reference with the function call as part of the name
            // The old AST doesn't have a separate TVF variant, so we encode it as a table name
            let func_name = name.last().unwrap().to_string();
            let arg_strs: Vec<String> = args.into_iter().map(|a| format!("{:?}", a)).collect();
            let full_name = format!("{}({})", func_name, arg_strs.join(", "));
            Ok((old::common::TableRef {
                name: old::common::TableName::Object(old::common::ObjectName {
                    schema: if name.len() > 1 { Some(name[0].to_string()) } else { None },
                    name: full_name,
                }),
                alias: alias.map(|a| a.into_owned()),
                pivot: None,
                unpivot: None,
                hints: Vec::new(),
            }, Vec::new()))
        }
    }
}

pub fn lower_insert<'a>(s: v2::InsertStmt<'a>) -> Result<old::statements::dml::InsertStmt, DbError> {
    Ok(old::statements::dml::InsertStmt {
        table: lower_object_name(s.table),
        columns: if s.columns.is_empty() { None } else { Some(s.columns.into_iter().map(|c| c.into_owned()).collect()) },
        source: match s.source {
            v2::InsertSource::Values(rows) => old::statements::dml::InsertSource::Values(
                rows.into_iter().map(|r| r.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()).collect::<Result<Vec<_>, _>>()?
            ),
            v2::InsertSource::Select(sel) => old::statements::dml::InsertSource::Select(Box::new(lower_select(*sel)?)),
            v2::InsertSource::Exec { procedure, args } => old::statements::dml::InsertSource::Exec(Box::new(old::Statement::ExecProcedure(old::statements::procedural::ExecProcedureStmt {
                name: lower_object_name(procedure),
                args: args.into_iter().map(|e| Ok(old::statements::procedural::ExecArgument {
                    name: None, 
                    expr: lower_expr(e)?,
                    is_output: false,
                })).collect::<Result<Vec<_>, DbError>>()?,
            }))),
            v2::InsertSource::DefaultValues => old::statements::dml::InsertSource::DefaultValues,
        },
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_update<'a>(s: v2::UpdateStmt<'a>) -> Result<old::statements::dml::UpdateStmt, DbError> {
    let (table_tr, mut extra_joins) = lower_table_ref_recursive(s.table)?;
    let table = match table_tr.name {
        old::common::TableName::Object(ref o) => o.clone(),
        _ => return Err(DbError::Parse("UPDATE target must be an object".into())),
    };
    
    let mut from_clause = None;
    if let Some(from_refs) = s.from {
        let (tr, mut j) = lower_from_clause_internal(from_refs)?;
        extra_joins.append(&mut j);
        from_clause = Some(old::statements::dml::FromClause {
            tables: vec![tr],
            joins: extra_joins,
            applies: Vec::new(),
        });
    } else if !extra_joins.is_empty() {
        from_clause = Some(old::statements::dml::FromClause {
            tables: vec![table_tr],
            joins: extra_joins,
            applies: Vec::new(),
        });
    }

    Ok(old::statements::dml::UpdateStmt {
        table,
        assignments: s.assignments.into_iter().map(|a| Ok(old::statements::dml::Assignment {
            column: a.column.into_owned(),
            expr: lower_expr(a.expr)?,
        })).collect::<Result<Vec<_>, _>>()?,
        top: s.top.map(|e| Ok(old::statements::query::TopSpec { value: lower_expr(e)? })).transpose()?,
        from: from_clause,
        selection: s.selection.map(lower_expr).transpose()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_delete<'a>(s: v2::DeleteStmt<'a>) -> Result<old::statements::dml::DeleteStmt, DbError> {
    let (tr, joins) = lower_from_clause_internal(s.from)?;
    let table = match tr.name {
        old::common::TableName::Object(ref o) => o.clone(),
        _ => return Err(DbError::Parse("DELETE target must be an object".into())),
    };

    Ok(old::statements::dml::DeleteStmt {
        table,
        top: s.top.map(|e| Ok(old::statements::query::TopSpec { value: lower_expr(e)? })).transpose()?,
        from: Some(old::statements::dml::FromClause {
            tables: vec![tr],
            joins,
            applies: Vec::new(),
        }),
        selection: s.selection.map(lower_expr).transpose()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_merge<'a>(s: v2::MergeStmt<'a>) -> Result<old::statements::dml::MergeStmt, DbError> {
    let (target, _) = lower_table_ref_recursive(s.target)?;
    let (source_tr, _) = lower_table_ref_recursive(s.source)?;
    Ok(old::statements::dml::MergeStmt {
        target,
        source: old::statements::dml::MergeSource::Table(source_tr),
        on_condition: lower_expr(s.on_condition)?,
        when_clauses: s.when_clauses.into_iter().map(|w| Ok(old::statements::dml::MergeWhenClause {
            when: match w.when {
                v2::MergeWhen::Matched => old::statements::dml::MergeWhen::Matched,
                v2::MergeWhen::NotMatched => old::statements::dml::MergeWhen::NotMatched,
                v2::MergeWhen::NotMatchedBySource => old::statements::dml::MergeWhen::NotMatchedBySource,
            },
            condition: w.condition.map(lower_expr).transpose()?,
            action: match w.action {
                v2::MergeAction::Update { assignments } => old::statements::dml::MergeAction::Update {
                    assignments: assignments.into_iter().map(|a| Ok(old::statements::dml::Assignment {
                        column: a.column.into_owned(),
                        expr: lower_expr(a.expr)?,
                    })).collect::<Result<Vec<_>, _>>()?,
                },
                v2::MergeAction::Insert { columns, values } => old::statements::dml::MergeAction::Insert {
                    columns: columns.into_iter().map(|c| c.into_owned()).collect(),
                    values: values.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
                },
                v2::MergeAction::Delete => old::statements::dml::MergeAction::Delete,
            },
        })).collect::<Result<Vec<_>, _>>()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_create<'a>(s: v2::CreateStmt<'a>) -> Result<old::Statement, DbError> {
    match s {
        v2::CreateStmt::Table { name, columns, constraints } => Ok(old::Statement::CreateTable(old::statements::ddl::CreateTableStmt {
            name: lower_object_name(name),
            columns: columns.into_iter().map(lower_column_def).collect::<Result<Vec<_>, _>>()?,
            table_constraints: constraints.into_iter().map(lower_table_constraint).collect(),
        })),
        v2::CreateStmt::View { name, query } => Ok(old::Statement::CreateView(old::statements::ddl::CreateViewStmt {
            name: lower_object_name(name),
            query: lower_select(query)?,
        })),
        v2::CreateStmt::Procedure { name, params, body } => Ok(old::Statement::CreateProcedure(old::statements::procedural::CreateProcedureStmt {
            name: lower_object_name(name),
            params: params.into_iter().map(lower_routine_param).collect::<Result<Vec<_>, _>>()?,
            body: body.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?,
        })),
        v2::CreateStmt::Function { name, params, returns, body } => Ok(old::Statement::CreateFunction(old::statements::procedural::CreateFunctionStmt {
            name: lower_object_name(name),
            params: params.into_iter().map(lower_routine_param).collect::<Result<Vec<_>, _>>()?,
            returns: returns.map(lower_data_type).transpose()?,
            body: match body {
                v2::FunctionBody::ScalarReturn(e) => old::statements::procedural::FunctionBody::ScalarReturn(lower_expr(e)?),
                v2::FunctionBody::Block(stmts) => old::statements::procedural::FunctionBody::Scalar(stmts.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?),
                v2::FunctionBody::Table(sel) => old::statements::procedural::FunctionBody::InlineTable(lower_select(sel)?),
            },
        })),
        v2::CreateStmt::Trigger { name, table, events, is_instead_of, body } => Ok(old::Statement::CreateTrigger(old::statements::procedural::CreateTriggerStmt {
            name: lower_object_name(name),
            table: lower_object_name(table),
            events,
            is_instead_of,
            body: body.into_iter().map(lower_statement).collect::<Result<Vec<_>, _>>()?,
        })),
    }
}

pub fn lower_column_def<'a>(c: v2::ColumnDef<'a>) -> Result<old::statements::ddl::ColumnSpec, DbError> {
    Ok(old::statements::ddl::ColumnSpec {
        name: c.name.into_owned(),
        data_type: lower_data_type(c.data_type)?,
        nullable: c.is_nullable.unwrap_or(true),
        identity: c.identity_spec,
        primary_key: c.is_primary_key,
        unique: c.is_unique,
        default: c.default_expr.map(lower_expr).transpose()?,
        default_constraint_name: c.default_constraint_name.map(|n| n.into_owned()),
        check: c.check_expr.map(lower_expr).transpose()?,
        check_constraint_name: c.check_constraint_name.map(|n| n.into_owned()),
        computed_expr: c.computed_expr.map(lower_expr).transpose()?,
        foreign_key: c.foreign_key.map(|fk| old::statements::ddl::ForeignKeyRef {
            referenced_table: lower_object_name(fk.ref_table),
            referenced_columns: fk.ref_columns.into_iter().map(|c| c.into_owned()).collect(),
            on_delete: fk.on_delete.map(lower_referential_action),
            on_update: fk.on_update.map(lower_referential_action),
        }),
    })
}

pub fn lower_routine_param<'a>(p: v2::RoutineParam<'a>) -> Result<old::statements::RoutineParam, DbError> {
    Ok(old::statements::RoutineParam {
        name: p.name.into_owned(),
        param_type: old::statements::RoutineParamType::Scalar(lower_data_type(p.data_type)?),
        is_output: p.is_output,
        is_readonly: false,
        default: p.default.map(lower_expr).transpose()?,
    })
}

pub fn lower_output_column<'a>(c: v2::OutputColumn<'a>) -> old::statements::dml::OutputColumn {
    old::statements::dml::OutputColumn {
        source: match c.source {
            v2::OutputSource::Inserted => old::statements::dml::OutputSource::Inserted,
            v2::OutputSource::Deleted => old::statements::dml::OutputSource::Deleted,
        },
        column: c.column.into_owned(),
        alias: c.alias.map(|a| a.into_owned()),
        is_wildcard: c.is_wildcard,
    }
}

pub fn lower_order_by_expr<'a>(o: v2::OrderByExpr<'a>) -> Result<old::statements::query::OrderByExpr, DbError> {
    Ok(old::statements::query::OrderByExpr {
        expr: lower_expr(o.expr)?,
        asc: o.asc,
    })
}

pub fn lower_exec_arg<'a>(a: v2::ExecArg<'a>) -> Result<old::statements::procedural::ExecArgument, DbError> {
    Ok(old::statements::procedural::ExecArgument {
        name: a.name.map(|n| n.into_owned()),
        expr: lower_expr(a.expr)?,
        is_output: a.is_output,
    })
}

pub fn lower_fetch_direction<'a>(d: v2::FetchDirection<'a>) -> Result<old::statements::procedural::FetchDirection, DbError> {
    match d {
        v2::FetchDirection::Next => Ok(old::statements::procedural::FetchDirection::Next),
        v2::FetchDirection::Prior => Ok(old::statements::procedural::FetchDirection::Prior),
        v2::FetchDirection::First => Ok(old::statements::procedural::FetchDirection::First),
        v2::FetchDirection::Last => Ok(old::statements::procedural::FetchDirection::Last),
        v2::FetchDirection::Absolute(expr) => Ok(old::statements::procedural::FetchDirection::Absolute(lower_expr(expr)?)),
        v2::FetchDirection::Relative(expr) => Ok(old::statements::procedural::FetchDirection::Relative(lower_expr(expr)?)),
    }
}

pub fn lower_alter_action<'a>(a: v2::AlterTableAction<'a>) -> Result<old::statements::ddl::AlterTableAction, DbError> {
    match a {
        v2::AlterTableAction::AddColumn(c) => Ok(old::statements::ddl::AlterTableAction::AddColumn(lower_column_def(c)?)),
        v2::AlterTableAction::DropColumn(c) => Ok(old::statements::ddl::AlterTableAction::DropColumn(c.into_owned())),
        v2::AlterTableAction::AddConstraint(c) => Ok(old::statements::ddl::AlterTableAction::AddConstraint(lower_table_constraint(c))),
        v2::AlterTableAction::DropConstraint(c) => Ok(old::statements::ddl::AlterTableAction::DropConstraint(c.into_owned())),
    }
}

pub fn lower_table_constraint<'a>(c: v2::TableConstraint<'a>) -> old::statements::ddl::TableConstraintSpec {
    match c {
        v2::TableConstraint::PrimaryKey { name, columns } => old::statements::ddl::TableConstraintSpec::PrimaryKey {
            name: name.map(|n| n.into_owned()).unwrap_or_default(),
            columns: columns.into_iter().map(|c| c.into_owned()).collect(),
        },
        v2::TableConstraint::Unique { name, columns } => old::statements::ddl::TableConstraintSpec::Unique {
            name: name.map(|n| n.into_owned()).unwrap_or_default(),
            columns: columns.into_iter().map(|c| c.into_owned()).collect(),
        },
        v2::TableConstraint::ForeignKey { name, columns, ref_table, ref_columns, on_delete, on_update } => old::statements::ddl::TableConstraintSpec::ForeignKey {
            name: name.map(|n| n.into_owned()).unwrap_or_default(),
            columns: columns.into_iter().map(|c| c.into_owned()).collect(),
            referenced_table: lower_object_name(ref_table),
            referenced_columns: ref_columns.into_iter().map(|c| c.into_owned()).collect(),
            on_delete: on_delete.map(lower_referential_action),
            on_update: on_update.map(lower_referential_action),
        },
        v2::TableConstraint::Check { name, expr } => old::statements::ddl::TableConstraintSpec::Check {
            name: name.map(|n| n.into_owned()).unwrap_or_default(),
            expr: lower_expr(expr).unwrap(),
        },
        v2::TableConstraint::Default { name, column, expr } => old::statements::ddl::TableConstraintSpec::Default {
            name: name.map(|n| n.into_owned()).unwrap_or_default(),
            column: column.into_owned(),
            expr: lower_expr(expr).unwrap(),
        },
    }
}

pub fn lower_referential_action(a: v2::ReferentialAction) -> old::statements::ddl::ReferentialAction {
    match a {
        v2::ReferentialAction::NoAction => old::statements::ddl::ReferentialAction::NoAction,
        v2::ReferentialAction::Cascade => old::statements::ddl::ReferentialAction::Cascade,
        v2::ReferentialAction::SetNull => old::statements::ddl::ReferentialAction::SetNull,
        v2::ReferentialAction::SetDefault => old::statements::ddl::ReferentialAction::SetDefault,
    }
}
