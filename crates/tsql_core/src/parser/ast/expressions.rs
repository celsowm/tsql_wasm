use super::statements::query::{SelectStmt, OrderByExpr};
use super::common::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    Negate,
    Not,
    BitwiseNot,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Identifier(String),
    QualifiedIdentifier(Vec<String>),
    Variable(String),
    Wildcard,
    QualifiedWildcard(Vec<String>),
    Integer(i64),
    Float(String),
    String(String),
    UnicodeString(String),
    BinaryLiteral(Vec<u8>),
    Null,
    Bool(bool),
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        target: DataType,
    },
    Convert {
        target: DataType,
        expr: Box<Expr>,
        style: Option<i32>,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_result: Option<Box<Expr>>,
    },
    Subquery(Box<SelectStmt>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStmt>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Exists {
        subquery: Box<SelectStmt>,
        negated: bool,
    },
    TryCast {
        expr: Box<Expr>,
        target: DataType,
    },
    TryConvert {
        target: DataType,
        expr: Box<Expr>,
        style: Option<i32>,
    },
    WindowFunction {
        name: String,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
        frame: Option<WindowFrame>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub extent: WindowFrameExtent,
}

impl WindowFrame {
    pub fn new(units: WindowFrameUnits, extent: WindowFrameExtent) -> Self {
        Self { units, extent }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowFrameExtent {
    Bound(WindowFrameBound),
    Between(WindowFrameBound, WindowFrameBound),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(Option<i64>),
    CurrentRow,
    Following(Option<i64>),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}
