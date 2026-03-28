use crate::ast::{ApplyClause, Expr, JoinClause, OrderByExpr, SelectItem, TopSpec};

use super::model::BoundTable;

#[derive(Debug, Clone)]
pub(crate) enum LogicalPlan {
    Scan {
        table: crate::ast::TableRef,
    },
    Pivot {
        input: Box<LogicalPlan>,
        spec: crate::ast::PivotSpec,
        alias: String,
    },
    Unpivot {
        input: Box<LogicalPlan>,
        spec: crate::ast::UnpivotSpec,
        alias: String,
    },
    Join {
        left: Box<LogicalPlan>,
        join: JoinClause,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
    },
    Project {
        input: Box<LogicalPlan>,
        projection: Vec<SelectItem>,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByExpr>,
    },
    Top {
        input: Box<LogicalPlan>,
        top: TopSpec,
    },
}

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
pub(crate) struct PhysicalJoin {
    pub(crate) right: PhysicalScan,
    pub(crate) join: JoinClause,
}

#[derive(Debug, Clone)]
pub(crate) struct PhysicalPlan {
    pub(crate) base: PhysicalScan,
    pub(crate) joins: Vec<PhysicalJoin>,
    pub(crate) applies: Vec<ApplyClause>,
    pub(crate) pivots: Vec<PhysicalPivot>,
    pub(crate) unpivots: Vec<PhysicalUnpivot>,
    pub(crate) residual_filter: Option<Expr>,
    pub(crate) projection: Vec<SelectItem>,
    pub(crate) group_by: Vec<Expr>,
    pub(crate) having: Option<Expr>,
    pub(crate) distinct: bool,
    pub(crate) order_by: Vec<OrderByExpr>,
    pub(crate) top: Option<TopSpec>,
    pub(crate) required_columns: Vec<String>,
    pub(crate) order_satisfied_by_scan: bool,
    pub(crate) offset: Option<Expr>,
    pub(crate) fetch: Option<Expr>,
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
