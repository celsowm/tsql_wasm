use super::super::expressions::Expr;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectStmt<'a> {
    pub distinct: bool,
    pub top: Option<Expr<'a>>,
    pub projection: Vec<SelectItem<'a>>,
    pub into_table: Option<Vec<Cow<'a, str>>>,
    pub from: Option<Vec<TableRef<'a>>>,
    pub applies: Vec<ApplyClause<'a>>,
    pub selection: Option<Expr<'a>>,
    pub group_by: Vec<Expr<'a>>,
    pub having: Option<Expr<'a>>,
    pub order_by: Vec<OrderByExpr<'a>>,
    pub offset: Option<Expr<'a>>,
    pub fetch: Option<Expr<'a>>,
    pub set_op: Option<Box<SetOp<'a>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApplyClause<'a> {
    pub apply_type: ApplyType,
    pub subquery: Box<SelectStmt<'a>>,
    pub alias: Cow<'a, str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplyType {
    Cross,
    Outer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SetOp<'a> {
    pub kind: SetOpKind,
    pub right: SelectStmt<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SetOpKind {
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectItem<'a> {
    pub expr: Expr<'a>,
    pub alias: Option<Cow<'a, str>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableRef<'a> {
    Table {
        name: Vec<Cow<'a, str>>,
        alias: Option<Cow<'a, str>>,
        hints: Vec<Cow<'a, str>>,
    },
    Subquery {
        subquery: Box<SelectStmt<'a>>,
        alias: Cow<'a, str>,
    },
    Join {
        left: Box<TableRef<'a>>,
        join_type: JoinType,
        right: Box<TableRef<'a>>,
        on: Option<Expr<'a>>,
    },
    Pivot {
        source: Box<TableRef<'a>>,
        spec: PivotSpec<'a>,
        alias: Cow<'a, str>,
    },
    Unpivot {
        source: Box<TableRef<'a>>,
        spec: UnpivotSpec<'a>,
        alias: Cow<'a, str>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PivotSpec<'a> {
    pub aggregate_func: Cow<'a, str>,
    pub aggregate_col: Cow<'a, str>,
    pub pivot_col: Cow<'a, str>,
    pub pivot_values: Vec<Cow<'a, str>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnpivotSpec<'a> {
    pub value_col: Cow<'a, str>,
    pub pivot_col: Cow<'a, str>,
    pub column_list: Vec<Cow<'a, str>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderByExpr<'a> {
    pub expr: Expr<'a>,
    pub asc: bool,
}
