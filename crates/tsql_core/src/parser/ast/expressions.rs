use super::statements::query::{SelectStmt, OrderByExpr};
use super::common::DataType;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    Like,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Negate,
    Not,
    BitwiseNot,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Expr<'a> {
    Identifier(Cow<'a, str>),
    QualifiedIdentifier(Vec<Cow<'a, str>>),
    Variable(Cow<'a, str>),
    Wildcard,
    QualifiedWildcard(Vec<Cow<'a, str>>),
    Integer(i64),
    Float(u64), // f64::to_bits()
    String(Cow<'a, str>),
    UnicodeString(Cow<'a, str>),
    BinaryLiteral(Vec<u8>),
    Null,
    Bool(bool),
    FunctionCall {
        name: Cow<'a, str>,
        args: Vec<Expr<'a>>,
    },
    Binary {
        left: Box<Expr<'a>>,
        op: BinaryOp,
        right: Box<Expr<'a>>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr<'a>>,
    },
    Cast {
        expr: Box<Expr<'a>>,
        target: DataType<'a>,
    },
    Convert {
        target: DataType<'a>,
        expr: Box<Expr<'a>>,
        style: Option<i32>,
    },
    Case {
        operand: Option<Box<Expr<'a>>>,
        when_clauses: Vec<WhenClause<'a>>,
        else_result: Option<Box<Expr<'a>>>,
    },
    Subquery(Box<SelectStmt<'a>>),
    InList {
        expr: Box<Expr<'a>>,
        list: Vec<Expr<'a>>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr<'a>>,
        subquery: Box<SelectStmt<'a>>,
        negated: bool,
    },
    Between {
        expr: Box<Expr<'a>>,
        low: Box<Expr<'a>>,
        high: Box<Expr<'a>>,
        negated: bool,
    },
    Like {
        expr: Box<Expr<'a>>,
        pattern: Box<Expr<'a>>,
        negated: bool,
    },
    IsNull(Box<Expr<'a>>),
    IsNotNull(Box<Expr<'a>>),
    Exists {
        subquery: Box<SelectStmt<'a>>,
        negated: bool,
    },
    TryCast {
        expr: Box<Expr<'a>>,
        target: DataType<'a>,
    },
    TryConvert {
        target: DataType<'a>,
        expr: Box<Expr<'a>>,
        style: Option<i32>,
    },
    WindowFunction {
        name: Cow<'a, str>,
        args: Vec<Expr<'a>>,
        partition_by: Vec<Expr<'a>>,
        order_by: Vec<OrderByExpr<'a>>,
        frame: Option<WindowFrame<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowFrame<'a> {
    pub units: WindowFrameUnits,
    pub extent: WindowFrameExtent,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a> WindowFrame<'a> {
    pub fn new(units: WindowFrameUnits, extent: WindowFrameExtent) -> Self {
        Self { units, extent, _phantom: std::marker::PhantomData }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameExtent {
    Bound(WindowFrameBound),
    Between(WindowFrameBound, WindowFrameBound),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(Option<i64>),
    CurrentRow,
    Following(Option<i64>),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WhenClause<'a> {
    pub condition: Expr<'a>,
    pub result: Expr<'a>,
}
