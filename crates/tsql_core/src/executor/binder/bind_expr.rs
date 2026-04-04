use crate::ast::{BinaryOp, UnaryOp};
use crate::types::DataType;
use crate::types::Value;

/// A pre-resolved expression where all column identifiers have been bound to
/// concrete (table_idx, col_idx) positions. This eliminates per-row string
/// comparisons during expression evaluation.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum BoundExpr {
    /// A column resolved to a specific table and column index in the row.
    Column {
        table_idx: usize,
        col_idx: usize,
        data_type: DataType,
    },
    /// A literal value (no resolution needed).
    Literal(Value),
    /// A binary operation on two bound sub-expressions.
    Binary {
        left: Box<BoundExpr>,
        op: BinaryOp,
        right: Box<BoundExpr>,
    },
    /// A unary operation on a bound sub-expression.
    Unary {
        op: UnaryOp,
        expr: Box<BoundExpr>,
    },
    /// IS NULL check on a bound sub-expression.
    IsNull(Box<BoundExpr>),
    /// IS NOT NULL check on a bound sub-expression.
    IsNotNull(Box<BoundExpr>),
    /// CAST expression.
    Cast {
        expr: Box<BoundExpr>,
        target: crate::ast::DataTypeSpec,
    },
    /// TRY_CAST expression.
    TryCast {
        expr: Box<BoundExpr>,
        target: crate::ast::DataTypeSpec,
    },
    /// CONVERT expression.
    Convert {
        target: crate::ast::DataTypeSpec,
        expr: Box<BoundExpr>,
        style: Option<i32>,
    },
    /// TRY_CONVERT expression.
    TryConvert {
        target: crate::ast::DataTypeSpec,
        expr: Box<BoundExpr>,
        style: Option<i32>,
    },
    /// Function call with bound arguments.
    FunctionCall {
        name: String,
        args: Vec<BoundExpr>,
    },
    /// CASE expression.
    Case {
        operand: Option<Box<BoundExpr>>,
        when_clauses: Vec<(BoundExpr, BoundExpr)>,
        else_result: Option<Box<BoundExpr>>,
    },
    /// IN list expression.
    InList {
        expr: Box<BoundExpr>,
        list: Vec<BoundExpr>,
        negated: bool,
    },
    /// BETWEEN expression.
    Between {
        expr: Box<BoundExpr>,
        low: Box<BoundExpr>,
        high: Box<BoundExpr>,
        negated: bool,
    },
    /// LIKE expression.
    Like {
        expr: Box<BoundExpr>,
        pattern: Box<BoundExpr>,
        negated: bool,
    },
    /// Subquery expression — cannot be bound at plan time, falls back to dynamic eval.
    Subquery(Box<crate::ast::SelectStmt>),
    /// EXISTS subquery — cannot be bound at plan time.
    Exists {
        subquery: Box<crate::ast::SelectStmt>,
        negated: bool,
    },
    /// IN subquery — cannot be bound at plan time.
    InSubquery {
        expr: Box<BoundExpr>,
        subquery: Box<crate::ast::SelectStmt>,
        negated: bool,
    },
    /// Window function — value is precomputed and stored in context.
    WindowFunction {
        key: String,
    },
    /// An expression that could not be statically bound (e.g., references outer row
    /// in a correlated subquery). Falls back to dynamic evaluation at runtime.
    Dynamic(crate::ast::Expr),
}
