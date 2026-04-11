use crate::ast::{Expr, SelectItem};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::{DataType, Value};

use super::super::clock::Clock;
use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr;
use super::super::model::JoinedRow;
use super::super::projection::{expand_projection_columns, expand_wildcard_values};
use super::super::result::QueryResult;

pub(crate) fn execute_flat_select(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    projection: Vec<SelectItem>,
    rows: Vec<JoinedRow>,
    ctx: &mut ExecutionContext,
) -> Result<QueryResult, DbError> {
    let columns = expand_projection_columns(&projection, rows.first());
    let projected_rows = project_flat_rows(catalog, storage, clock, &projection, &rows, ctx);

    let expr_types = expand_projection_types(&projection, rows.first());
    let row_types = derive_types_from_rows(&projected_rows, columns.len());

    let mut column_types = Vec::with_capacity(columns.len());
    for i in 0..columns.len() {
        column_types.push(
            expr_types
                .get(i)
                .and_then(|t| t.clone())
                .or_else(|| row_types.get(i).and_then(|t| t.clone()))
                .unwrap_or(DataType::VarChar { max_len: 4000 }),
        );
    }

    Ok(QueryResult {
        columns,
        column_types,
        rows: projected_rows,
        ..Default::default()
    })
}

pub(crate) fn project_flat_rows(
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
    projection: &[SelectItem],
    rows: &[JoinedRow],
    ctx: &mut ExecutionContext,
) -> Vec<Vec<Value>> {
    rows.iter()
        .map(|row| {
            let mut out = Vec::new();
            for item in projection {
                match &item.expr {
                    Expr::Wildcard => out.extend(expand_wildcard_values(row)),
                    Expr::QualifiedWildcard(parts) => {
                        let table_name = match parts.last() {
                            Some(name) => name,
                            None => continue,
                        };
                        out.extend(
                            crate::executor::projection::expand_qualified_wildcard_values(
                                row, table_name,
                            ),
                        );
                    }
                    expr => {
                        let val = eval_expr(expr, row, ctx, catalog, storage, clock);
                        match val {
                            Ok(v) => out.push(v),
                            Err(_) => {
                                // If qualified fails, try unqualified if it's a simple identifier
                                if let Expr::QualifiedIdentifier(parts) = expr {
                                    if parts.len() == 2 {
                                        let fallback = Expr::Identifier(parts[1].clone());
                                        out.push(
                                            eval_expr(&fallback, row, ctx, catalog, storage, clock)
                                                .unwrap_or(Value::Null),
                                        );
                                    } else {
                                        out.push(Value::Null);
                                    }
                                } else {
                                    out.push(Value::Null);
                                }
                            }
                        }
                    }
                }
            }
            out
        })
        .collect()
}

fn derive_types_from_rows(rows: &[Vec<Value>], col_count: usize) -> Vec<Option<DataType>> {
    let mut out = vec![None; col_count];
    for row in rows {
        for (i, value) in row.iter().enumerate().take(col_count) {
            if out[i].is_none() {
                out[i] = value.data_type();
            }
        }
    }
    out
}

fn expand_projection_types(
    projection: &[SelectItem],
    sample: Option<&JoinedRow>,
) -> Vec<Option<DataType>> {
    let mut types = Vec::new();
    for item in projection {
        types.extend(expand_projection_item_types(item, sample));
    }
    types
}

fn expand_projection_item_types(
    item: &SelectItem,
    sample: Option<&JoinedRow>,
) -> Vec<Option<DataType>> {
    match &item.expr {
        Expr::Wildcard => {
            if let Some(row) = sample {
                row.iter()
                    .flat_map(|binding| {
                        binding
                            .table
                            .columns
                            .iter()
                            .map(|c| Some(c.data_type.clone()))
                    })
                    .collect()
            } else {
                vec![None]
            }
        }
        Expr::QualifiedWildcard(parts) => {
            if let Some(row) = sample {
                let table_name = match parts.last() {
                    Some(name) => name,
                    None => return vec![None],
                };
                row.iter()
                    .filter(|binding| {
                        binding.alias.eq_ignore_ascii_case(table_name)
                            || binding.table.name.eq_ignore_ascii_case(table_name)
                    })
                    .flat_map(|binding| {
                        binding
                            .table
                            .columns
                            .iter()
                            .map(|c| Some(c.data_type.clone()))
                    })
                    .collect()
            } else {
                vec![None]
            }
        }
        expr => vec![infer_expr_type(expr, sample)],
    }
}

fn infer_expr_type(expr: &Expr, sample: Option<&JoinedRow>) -> Option<DataType> {
    match expr {
        Expr::Identifier(name) => lookup_identifier_type(name, sample),
        Expr::QualifiedIdentifier(parts) => lookup_qualified_identifier_type(parts, sample),
        Expr::Integer(v) => Some(if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
            DataType::Int
        } else {
            DataType::BigInt
        }),
        Expr::FloatLiteral(_) => Some(DataType::Float),
        Expr::BinaryLiteral(bytes) => Some(DataType::VarBinary {
            max_len: bytes.len().max(1) as u16,
        }),
        Expr::String(v) => Some(DataType::VarChar {
            max_len: v.len().max(1) as u16,
        }),
        Expr::UnicodeString(v) => Some(DataType::NVarChar {
            max_len: v.encode_utf16().count().max(1) as u16,
        }),
        Expr::Null => None,
        Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::Like { .. } => Some(DataType::Bit),
        Expr::Exists { .. } | Expr::InSubquery { .. } => Some(DataType::Bit),
        Expr::Cast { target, .. }
        | Expr::TryCast { target, .. }
        | Expr::Convert { target, .. }
        | Expr::TryConvert { target, .. } => Some(
            super::super::type_mapping::data_type_spec_to_runtime(target),
        ),
        Expr::Unary { op, expr: inner } => match op {
            crate::ast::UnaryOp::Not => Some(DataType::Bit),
            crate::ast::UnaryOp::Negate | crate::ast::UnaryOp::BitwiseNot => {
                infer_expr_type(inner, sample)
            }
        },
        Expr::Binary { left, op, right } => match op {
            crate::ast::BinaryOp::Eq
            | crate::ast::BinaryOp::NotEq
            | crate::ast::BinaryOp::Gt
            | crate::ast::BinaryOp::Lt
            | crate::ast::BinaryOp::Gte
            | crate::ast::BinaryOp::Lte
            | crate::ast::BinaryOp::And
            | crate::ast::BinaryOp::Or => Some(DataType::Bit),
            crate::ast::BinaryOp::Add => {
                let lt = infer_expr_type(left, sample);
                let rt = infer_expr_type(right, sample);
                if is_text_type(lt.as_ref()) || is_text_type(rt.as_ref()) {
                    Some(DataType::VarChar { max_len: 4000 })
                } else if matches!(lt, Some(DataType::Float)) || matches!(rt, Some(DataType::Float))
                {
                    Some(DataType::Float)
                } else {
                    Some(DataType::Int)
                }
            }
            crate::ast::BinaryOp::Subtract
            | crate::ast::BinaryOp::Multiply
            | crate::ast::BinaryOp::Divide
            | crate::ast::BinaryOp::Modulo
            | crate::ast::BinaryOp::BitwiseAnd
            | crate::ast::BinaryOp::BitwiseOr
            | crate::ast::BinaryOp::BitwiseXor => Some(DataType::Int),
        },
        Expr::Case {
            when_clauses,
            else_result,
            ..
        } => {
            for clause in when_clauses {
                if let Some(t) = infer_expr_type(&clause.result, sample) {
                    return Some(t);
                }
            }
            else_result
                .as_ref()
                .and_then(|expr| infer_expr_type(expr, sample))
        }
        _ => None,
    }
}

fn is_text_type(t: Option<&DataType>) -> bool {
    matches!(
        t,
        Some(DataType::Char { .. })
            | Some(DataType::VarChar { .. })
            | Some(DataType::NChar { .. })
            | Some(DataType::NVarChar { .. })
            | Some(DataType::Xml)
    )
}

fn lookup_identifier_type(name: &str, sample: Option<&JoinedRow>) -> Option<DataType> {
    let row = sample?;
    for binding in row {
        if let Some(col) = binding
            .table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        {
            return Some(col.data_type.clone());
        }
    }
    None
}

fn lookup_qualified_identifier_type(
    parts: &[String],
    sample: Option<&JoinedRow>,
) -> Option<DataType> {
    let row = sample?;
    if parts.is_empty() {
        return None;
    }
    let col_name = parts.last()?;
    let prefix = if parts.len() > 1 {
        Some(parts[parts.len() - 2].as_str())
    } else {
        None
    };

    for binding in row {
        if let Some(prefix_name) = prefix {
            let matches_prefix = binding.alias.eq_ignore_ascii_case(prefix_name)
                || binding.table.name.eq_ignore_ascii_case(prefix_name);
            if !matches_prefix {
                continue;
            }
        }
        if let Some(col) = binding
            .table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(col_name))
        {
            return Some(col.data_type.clone());
        }
    }
    None
}
