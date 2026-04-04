use super::super::common::{ObjectName, TableRef};
use super::super::expressions::Expr;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectStmt<'a> {
    pub from: Option<TableRef<'a>>,
    pub joins: Vec<JoinClause<'a>>,
    pub applies: Vec<ApplyClause<'a>>,
    pub projection: Vec<SelectItem<'a>>,
    pub into_table: Option<ObjectName<'a>>,
    pub distinct: bool,
    pub top: Option<TopSpec<'a>>,
    pub selection: Option<Expr<'a>>,
    pub group_by: Vec<Expr<'a>>,
    pub having: Option<Expr<'a>>,
    pub order_by: Vec<OrderByExpr<'a>>,
    pub offset: Option<Expr<'a>>,
    pub fetch: Option<Expr<'a>>,
    pub set_op: Option<Box<SetOp<'a>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinClause<'a> {
    pub join_type: JoinType,
    pub table: TableRef<'a>,
    pub on: Option<Expr<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApplyClause<'a> {
    pub apply_type: ApplyType,
    pub table: TableRef<'a>,
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
pub struct TopSpec<'a> {
    pub value: Expr<'a>,
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
