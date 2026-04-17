use super::dml::lower_select;
use crate::ast as executor_ast;
use crate::error::DbError;
use crate::parser::ast;

pub fn lower_expr(parser_expr: ast::Expr) -> Result<executor_ast::expressions::Expr, DbError> {
    match parser_expr {
        ast::Expr::Identifier(id) => Ok(executor_ast::expressions::Expr::Identifier(id)),
        ast::Expr::Variable(id) => Ok(executor_ast::expressions::Expr::Identifier(id)),
        ast::Expr::QualifiedIdentifier(parts) => {
            Ok(executor_ast::expressions::Expr::QualifiedIdentifier(parts))
        }
        ast::Expr::Wildcard => Ok(executor_ast::expressions::Expr::Wildcard),
        ast::Expr::QualifiedWildcard(parts) => {
            Ok(executor_ast::expressions::Expr::QualifiedWildcard(parts))
        }
        ast::Expr::Integer(i) => Ok(executor_ast::expressions::Expr::Integer(i)),
        ast::Expr::Float(f) => Ok(executor_ast::expressions::Expr::FloatLiteral(f)),
        ast::Expr::String(s) => Ok(executor_ast::expressions::Expr::String(s)),
        ast::Expr::UnicodeString(s) => Ok(executor_ast::expressions::Expr::UnicodeString(s)),
        ast::Expr::BinaryLiteral(b) => Ok(executor_ast::expressions::Expr::BinaryLiteral(b)),
        ast::Expr::Null => Ok(executor_ast::expressions::Expr::Null),
        ast::Expr::Bool(b) => Ok(executor_ast::expressions::Expr::Integer(if b {
            1
        } else {
            0
        })),
        ast::Expr::Binary { left, op, right } => match op {
            ast::BinaryOp::Like => Ok(executor_ast::expressions::Expr::Like {
                expr: Box::new(lower_expr(*left)?),
                pattern: Box::new(lower_expr(*right)?),
                negated: false,
            }),
            _ => Ok(executor_ast::expressions::Expr::Binary {
                left: Box::new(lower_expr(*left)?),
                op: lower_binary_op(op)?,
                right: Box::new(lower_expr(*right)?),
            }),
        },
        ast::Expr::Unary { op, expr } => Ok(executor_ast::expressions::Expr::Unary {
            op: lower_unary_op(op),
            expr: Box::new(lower_expr(*expr)?),
        }),
        ast::Expr::IsNull(expr) => Ok(executor_ast::expressions::Expr::IsNull(Box::new(
            lower_expr(*expr)?,
        ))),
        ast::Expr::IsNotNull(expr) => Ok(executor_ast::expressions::Expr::IsNotNull(Box::new(
            lower_expr(*expr)?,
        ))),
        ast::Expr::Cast { expr, target } => Ok(executor_ast::expressions::Expr::Cast {
            expr: Box::new(lower_expr(*expr)?),
            target: lower_data_type(target)?,
        }),
        ast::Expr::TryCast { expr, target } => Ok(executor_ast::expressions::Expr::TryCast {
            expr: Box::new(lower_expr(*expr)?),
            target: lower_data_type(target)?,
        }),
        ast::Expr::Convert {
            target,
            expr,
            style,
        } => Ok(executor_ast::expressions::Expr::Convert {
            target: lower_data_type(target)?,
            expr: Box::new(lower_expr(*expr)?),
            style,
        }),
        ast::Expr::TryConvert {
            target,
            expr,
            style,
        } => Ok(executor_ast::expressions::Expr::TryConvert {
            target: lower_data_type(target)?,
            expr: Box::new(lower_expr(*expr)?),
            style,
        }),
        ast::Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => Ok(executor_ast::expressions::Expr::Case {
            operand: operand.map(|e| lower_expr(*e)).transpose()?.map(Box::new),
            when_clauses: when_clauses
                .into_iter()
                .map(|w| {
                    Ok(executor_ast::expressions::WhenClause {
                        condition: lower_expr(w.condition)?,
                        result: lower_expr(w.result)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            else_result: else_result
                .map(|e| lower_expr(*e))
                .transpose()?
                .map(Box::new),
        }),
        ast::Expr::InList {
            expr,
            list,
            negated,
        } => Ok(executor_ast::expressions::Expr::InList {
            expr: Box::new(lower_expr(*expr)?),
            list: list
                .into_iter()
                .map(lower_expr)
                .collect::<Result<Vec<_>, _>>()?,
            negated,
        }),
        ast::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(executor_ast::expressions::Expr::InSubquery {
            expr: Box::new(lower_expr(*expr)?),
            subquery: Box::new(lower_select(*subquery)?),
            negated,
        }),
        ast::Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(executor_ast::expressions::Expr::Between {
            expr: Box::new(lower_expr(*expr)?),
            low: Box::new(lower_expr(*low)?),
            high: Box::new(lower_expr(*high)?),
            negated,
        }),
        ast::Expr::Like {
            expr,
            pattern,
            negated,
        } => Ok(executor_ast::expressions::Expr::Like {
            expr: Box::new(lower_expr(*expr)?),
            pattern: Box::new(lower_expr(*pattern)?),
            negated,
        }),
        ast::Expr::Exists { subquery, negated } => Ok(executor_ast::expressions::Expr::Exists {
            subquery: Box::new(lower_select(*subquery)?),
            negated,
        }),
        ast::Expr::Subquery(s) => Ok(executor_ast::expressions::Expr::Subquery(Box::new(
            lower_select(*s)?,
        ))),
        ast::Expr::FunctionCall {
            name,
            args,
            within_group,
        } => {
            Ok(executor_ast::expressions::Expr::FunctionCall {
                name,
                args: args
                    .into_iter()
                    .map(lower_expr)
                    .collect::<Result<Vec<_>, _>>()?,
                within_group: within_group
                    .into_iter()
                    .map(lower_order_by_expr)
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        ast::Expr::WindowFunction {
            name,
            args,
            partition_by,
            order_by,
            frame,
        } => Ok(executor_ast::expressions::Expr::WindowFunction {
            func: lower_window_func(name.as_str()),
            args: args
                .into_iter()
                .map(lower_expr)
                .collect::<Result<Vec<_>, _>>()?,
            partition_by: partition_by
                .into_iter()
                .map(lower_expr)
                .collect::<Result<Vec<_>, _>>()?,
            order_by: order_by
                .into_iter()
                .map(lower_order_by_expr)
                .collect::<Result<Vec<_>, _>>()?,
            frame: frame.map(lower_window_frame),
        }),
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
        ast::BinaryOp::Like => Err(DbError::Parse(
            "LIKE should be lowered as a dedicated expression".into(),
        )),
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
            ast::WindowFrameExtent::Bound(b) => {
                executor_ast::expressions::WindowFrameExtent::Bound(lower_window_bound(b))
            }
            ast::WindowFrameExtent::Between(b1, b2) => {
                executor_ast::expressions::WindowFrameExtent::Between(
                    lower_window_bound(b1),
                    lower_window_bound(b2),
                )
            }
        },
    }
}

pub fn lower_window_bound(
    bound: ast::WindowFrameBound,
) -> executor_ast::expressions::WindowFrameBound {
    match bound {
        ast::WindowFrameBound::UnboundedPreceding => {
            executor_ast::expressions::WindowFrameBound::UnboundedPreceding
        }
        ast::WindowFrameBound::Preceding(n) => {
            executor_ast::expressions::WindowFrameBound::Preceding(n)
        }
        ast::WindowFrameBound::CurrentRow => {
            executor_ast::expressions::WindowFrameBound::CurrentRow
        }
        ast::WindowFrameBound::Following(n) => {
            executor_ast::expressions::WindowFrameBound::Following(n)
        }
        ast::WindowFrameBound::UnboundedFollowing => {
            executor_ast::expressions::WindowFrameBound::UnboundedFollowing
        }
    }
}

pub fn lower_object_name(mut parts: Vec<String>) -> executor_ast::ObjectName {
    match parts.len() {
        0 => executor_ast::ObjectName {
            schema: None,
            name: "".to_string(),
        },
        1 => executor_ast::ObjectName {
            schema: None,
            name: parts.remove(0),
        },
        _ => {
            let name = parts.pop().unwrap_or_default();
            let schema = Some(parts.pop().unwrap_or_default());
            executor_ast::ObjectName { schema, name }
        }
    }
}

pub fn lower_object_name_owned(name: ast::ObjectName) -> executor_ast::ObjectName {
    executor_ast::ObjectName {
        schema: name.schema,
        name: name.name,
    }
}

pub fn lower_data_type(
    dt: ast::DataType,
) -> Result<executor_ast::data_types::DataTypeSpec, DbError> {
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
        ast::DataType::VarChar(n) => Ok(executor_ast::data_types::DataTypeSpec::VarChar(
            n.unwrap_or(u16::MAX as u32) as u16,
        )),
        ast::DataType::NVarChar(n) => Ok(executor_ast::data_types::DataTypeSpec::NVarChar(
            n.unwrap_or(u16::MAX as u32) as u16,
        )),
        ast::DataType::Char(n) => Ok(executor_ast::data_types::DataTypeSpec::Char(
            n.unwrap_or(1) as u16
        )),
        ast::DataType::NChar(n) => Ok(executor_ast::data_types::DataTypeSpec::NChar(
            n.unwrap_or(1) as u16,
        )),
        ast::DataType::Binary(n) => Ok(executor_ast::data_types::DataTypeSpec::Binary(
            n.unwrap_or(1) as u16,
        )),
        ast::DataType::VarBinary(n) => Ok(executor_ast::data_types::DataTypeSpec::VarBinary(
            n.unwrap_or(u16::MAX as u32) as u16,
        )),
        ast::DataType::Date => Ok(executor_ast::data_types::DataTypeSpec::Date),
        ast::DataType::Time => Ok(executor_ast::data_types::DataTypeSpec::Time),
        ast::DataType::DateTime => Ok(executor_ast::data_types::DataTypeSpec::DateTime),
        ast::DataType::DateTime2 => Ok(executor_ast::data_types::DataTypeSpec::DateTime2),
        ast::DataType::Money => Ok(executor_ast::data_types::DataTypeSpec::Money),
        ast::DataType::SmallMoney => Ok(executor_ast::data_types::DataTypeSpec::SmallMoney),
        ast::DataType::UniqueIdentifier => {
            Ok(executor_ast::data_types::DataTypeSpec::UniqueIdentifier)
        }
        ast::DataType::SqlVariant => Ok(executor_ast::data_types::DataTypeSpec::SqlVariant),
        ast::DataType::Xml => Ok(executor_ast::data_types::DataTypeSpec::Xml),
        ast::DataType::DateTimeOffset => Ok(executor_ast::data_types::DataTypeSpec::DateTimeOffset),
        ast::DataType::SmallDateTime => Ok(executor_ast::data_types::DataTypeSpec::SmallDateTime),
        ast::DataType::Image => Ok(executor_ast::data_types::DataTypeSpec::VarBinary(8000)),
        ast::DataType::Text => Ok(executor_ast::data_types::DataTypeSpec::VarChar(8000)),
        ast::DataType::NText => Ok(executor_ast::data_types::DataTypeSpec::NVarChar(4000)),
        ast::DataType::Table => Ok(executor_ast::data_types::DataTypeSpec::VarChar(255)),
        ast::DataType::Custom(_) => Ok(executor_ast::data_types::DataTypeSpec::VarChar(255)),
    }
}

pub fn lower_order_by_expr(
    o: ast::OrderByExpr,
) -> Result<executor_ast::statements::query::OrderByExpr, DbError> {
    Ok(executor_ast::statements::query::OrderByExpr {
        expr: lower_expr(o.expr)?,
        asc: o.asc,
    })
}
