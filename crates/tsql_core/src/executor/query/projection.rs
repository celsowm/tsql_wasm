use crate::ast::{Expr, SelectItem};
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

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
    
    let mut column_types = Vec::new();
    if !projected_rows.is_empty() {
        for val in &projected_rows[0] {
            column_types.push(val.data_type().unwrap_or(crate::types::DataType::VarChar { max_len: 4000 }));
        }
    } else {
        column_types = vec![crate::types::DataType::VarChar { max_len: 4000 }; columns.len()];
    }

    Ok(QueryResult {
        columns,
        column_types,
        rows: projected_rows,
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
                        let table_name = parts.last().unwrap();
                        out.extend(crate::executor::projection::expand_qualified_wildcard_values(row, table_name));
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
                                        out.push(eval_expr(&fallback, row, ctx, catalog, storage, clock).unwrap_or(Value::Null));
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
