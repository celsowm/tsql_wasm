use crate::ast::{BinaryOp, Expr, OrderByExpr};
use crate::catalog::Catalog;

use crate::executor::model::BoundTable;
use crate::executor::physical::ScanStrategy;

pub(crate) fn choose_scan_strategy(
    bound: &BoundTable,
    predicate: Option<&Expr>,
    order_by: &[OrderByExpr],
    catalog: &dyn Catalog,
) -> ScanStrategy {
    let indexes: Vec<&crate::catalog::IndexDef> = catalog
        .get_indexes()
        .iter()
        .filter(|idx| idx.table_id == bound.table.id)
        .collect();
    if indexes.is_empty() {
        return ScanStrategy::TableScan;
    }
    let Some(idx) = indexes.first() else {
        return ScanStrategy::TableScan;
    };
    let Some(first_col_id) = idx.column_ids.first() else {
        return ScanStrategy::TableScan;
    };
    let Some(first_col) = bound.table.columns.iter().find(|c| c.id == *first_col_id) else {
        return ScanStrategy::TableScan;
    };

    if let Some(pred) = predicate {
        if let Some((op, _)) =
            extract_index_predicate_rhs(Some(pred), &bound.alias, &first_col.name)
        {
            if matches!(op, BinaryOp::Eq) {
                return ScanStrategy::IndexSeek { index_id: idx.id };
            }
            return ScanStrategy::IndexScan { index_id: idx.id };
        }
    }
    if order_by.len() == 1 {
        if let Expr::QualifiedIdentifier(parts) = &order_by[0].expr {
            if parts.len() >= 2
                && parts[0].eq_ignore_ascii_case(&bound.alias)
                && parts[1].eq_ignore_ascii_case(&first_col.name)
                && order_by[0].asc
            {
                return ScanStrategy::IndexScan { index_id: idx.id };
            }
        }
    }
    ScanStrategy::TableScan
}

pub(crate) fn extract_index_predicate_rhs(
    predicate: Option<&Expr>,
    alias: &str,
    column: &str,
) -> Option<(BinaryOp, Expr)> {
    let pred = predicate?;
    match pred {
        Expr::Binary { left, op, right } => {
            if let Expr::QualifiedIdentifier(parts) = left.as_ref() {
                if parts.len() >= 2
                    && parts[0].eq_ignore_ascii_case(alias)
                    && parts[1].eq_ignore_ascii_case(column)
                    && is_supported_index_op(*op)
                {
                    return Some((*op, (*right.clone())));
                }
            }
            if let Expr::QualifiedIdentifier(parts) = right.as_ref() {
                if parts.len() >= 2
                    && parts[0].eq_ignore_ascii_case(alias)
                    && parts[1].eq_ignore_ascii_case(column)
                    && is_supported_index_op(*op)
                {
                    return Some((*op, (*left.clone())));
                }
            }
            if *op == BinaryOp::And {
                extract_index_predicate_rhs(Some(left), alias, column)
                    .or_else(|| extract_index_predicate_rhs(Some(right), alias, column))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn is_supported_index_op(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq | BinaryOp::Gt | BinaryOp::Gte | BinaryOp::Lt | BinaryOp::Lte
    )
}
