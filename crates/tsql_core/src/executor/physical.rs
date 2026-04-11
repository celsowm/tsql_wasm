use crate::ast::Expr;

use super::model::BoundTable;

#[derive(Debug, Clone)]
pub(crate) enum ScanStrategy {
    TableScan,
    IndexSeek { index_id: u32 },
    IndexScan { index_id: u32 },
}

#[derive(Debug, Clone)]
pub(crate) struct PhysicalScan {
    pub(crate) bound: BoundTable,
    pub(crate) strategy: ScanStrategy,
    pub(crate) pushed_predicate: Option<Expr>,
}

#[derive(Debug, Clone)]
pub(crate) struct PhysicalPivot {
    pub(crate) spec: crate::ast::PivotSpec,
    pub(crate) alias: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PhysicalUnpivot {
    pub(crate) spec: crate::ast::UnpivotSpec,
    pub(crate) alias: String,
}
