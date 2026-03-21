#[derive(Debug, Clone)]
pub struct ObjectName {
    pub schema: Option<String>,
    pub name: String,
}

impl ObjectName {
    pub fn schema_or_dbo(&self) -> &str {
        self.schema.as_deref().unwrap_or("dbo")
    }
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: ObjectName,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub name: ObjectName,
    pub columns: Vec<ColumnSpec>,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataTypeSpec,
    pub nullable: bool,
    pub primary_key: bool,
    pub identity: Option<(i64, i64)>,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: ObjectName,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expr>>,
    pub default_values: bool,
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub from: TableRef,
    pub joins: Vec<JoinClause>,
    pub projection: Vec<SelectItem>,
    pub top: Option<TopSpec>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub on: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TopSpec {
    pub value: Expr,
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub desc: bool,
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: ObjectName,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table: ObjectName,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Identifier(String),
    QualifiedIdentifier(Vec<String>),
    Wildcard,
    Integer(i64),
    String(String),
    UnicodeString(String),
    Null,
    FunctionCall { name: String, args: Vec<Expr> },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Cast {
        expr: Box<Expr>,
        target: DataTypeSpec,
    },
    Convert {
        target: DataTypeSpec,
        expr: Box<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataTypeSpec {
    Bit,
    Int,
    BigInt,
    VarChar(u16),
    NVarChar(u16),
    DateTime,
}
