use crate::ast::data_types::DataTypeSpec;
use crate::ast::statements::query::OrderByExpr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Identifier(String),
    QualifiedIdentifier(Vec<String>),
    Wildcard,
    QualifiedWildcard(Vec<String>),
    Integer(i64),
    FloatLiteral(String),
    BinaryLiteral(Vec<u8>),
    String(String),
    UnicodeString(String),
    Null,
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        within_group: Vec<OrderByExpr>,
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
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Cast {
        expr: Box<Expr>,
        target: DataTypeSpec,
    },
    TryCast {
        expr: Box<Expr>,
        target: DataTypeSpec,
    },
    Convert {
        target: DataTypeSpec,
        expr: Box<Expr>,
        style: Option<i32>,
    },
    TryConvert {
        target: DataTypeSpec,
        expr: Box<Expr>,
        style: Option<i32>,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_result: Option<Box<Expr>>,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
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
    WindowFunction {
        func: WindowFunc,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
        frame: Option<WindowFrame>,
    },
    Subquery(Box<crate::ast::statements::query::SelectStmt>),
    Exists {
        subquery: Box<crate::ast::statements::query::SelectStmt>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<crate::ast::statements::query::SelectStmt>,
        negated: bool,
    },
    NextValueFor {
        sequence_name: crate::ast::ObjectName,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    NTile,
    Lag,
    Lead,
    FirstValue,
    LastValue,
    PercentileCont,
    PercentileDisc,
    PercentileRank,
    Aggregate(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub extent: WindowFrameExtent,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    Negate,
    Not,
    BitwiseNot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
}
