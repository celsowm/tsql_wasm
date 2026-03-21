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
    DropTable(DropTableStmt),
    CreateSchema(CreateSchemaStmt),
    DropSchema(DropSchemaStmt),
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
pub struct DropTableStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone)]
pub struct CreateSchemaStmt {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct DropSchemaStmt {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: DataTypeSpec,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
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
    pub from: Option<TableRef>,
    pub joins: Vec<JoinClause>,
    pub projection: Vec<SelectItem>,
    pub distinct: bool,
    pub top: Option<TopSpec>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
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
    Right,
    Full,
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

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Identifier(String),
    QualifiedIdentifier(Vec<String>),
    Wildcard,
    Integer(i64),
    FloatLiteral(String),
    String(String),
    UnicodeString(String),
    Null,
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Negate,
    Not,
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
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataTypeSpec {
    Bit,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal(u8, u8),
    Char(u16),
    VarChar(u16),
    NChar(u16),
    NVarChar(u16),
    Date,
    Time,
    DateTime,
    DateTime2,
    UniqueIdentifier,
}
