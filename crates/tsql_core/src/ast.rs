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
    BeginTransaction(Option<String>),
    CommitTransaction,
    RollbackTransaction(Option<String>),
    SaveTransaction(String),
    SetTransactionIsolationLevel(IsolationLevel),
    CreateTable(CreateTableStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    DropTable(DropTableStmt),
    CreateSchema(CreateSchemaStmt),
    DropSchema(DropSchemaStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    SetOp(SetOpStmt),
    TruncateTable(TruncateTableStmt),
    AlterTable(AlterTableStmt),
    WithCte(WithCteStmt),
    Declare(DeclareStmt),
    Set(SetStmt),
    If(IfStmt),
    BeginEnd(Vec<Statement>),
    While(WhileStmt),
    Break,
    Continue,
    Return(Option<Expr>),
    ExecDynamic(ExecStmt),
    ExecProcedure(ExecProcedureStmt),
    SpExecuteSql(SpExecuteSqlStmt),
    SelectAssign(SelectAssignStmt),
    DeclareTableVar(DeclareTableVarStmt),
    CreateProcedure(CreateProcedureStmt),
    DropProcedure(DropProcedureStmt),
    CreateFunction(CreateFunctionStmt),
    DropFunction(DropFunctionStmt),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

#[derive(Debug, Clone)]
pub struct DeclareStmt {
    pub name: String,
    pub data_type: DataTypeSpec,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct SetStmt {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct IfStmt {
    pub condition: Expr,
    pub then_body: Vec<Statement>,
    pub else_body: Option<Vec<Statement>>,
}

#[derive(Debug, Clone)]
pub struct WhileStmt {
    pub condition: Expr,
    pub body: Vec<Statement>,
}

#[derive(Debug, Clone)]
pub struct ExecStmt {
    pub sql_expr: Expr,
}

#[derive(Debug, Clone)]
pub struct ExecArgument {
    pub name: Option<String>,
    pub expr: Expr,
    pub is_output: bool,
}

#[derive(Debug, Clone)]
pub struct ExecProcedureStmt {
    pub name: ObjectName,
    pub args: Vec<ExecArgument>,
}

#[derive(Debug, Clone)]
pub struct SpExecuteSqlStmt {
    pub sql_expr: Expr,
    pub params_def: Option<Expr>,
    pub args: Vec<ExecArgument>,
}

#[derive(Debug, Clone)]
pub struct SelectAssignTarget {
    pub variable: String,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub struct SelectAssignStmt {
    pub targets: Vec<SelectAssignTarget>,
    pub from: Option<TableRef>,
    pub joins: Vec<JoinClause>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct DeclareTableVarStmt {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub table_constraints: Vec<TableConstraintSpec>,
}

#[derive(Debug, Clone)]
pub struct RoutineParam {
    pub name: String,
    pub data_type: DataTypeSpec,
    pub is_output: bool,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct CreateProcedureStmt {
    pub name: ObjectName,
    pub params: Vec<RoutineParam>,
    pub body: Vec<Statement>,
}

#[derive(Debug, Clone)]
pub struct DropProcedureStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone)]
pub enum FunctionBody {
    ScalarReturn(Expr),
    InlineTable(SelectStmt),
}

#[derive(Debug, Clone)]
pub struct CreateFunctionStmt {
    pub name: ObjectName,
    pub params: Vec<RoutineParam>,
    pub returns: Option<DataTypeSpec>,
    pub body: FunctionBody,
}

#[derive(Debug, Clone)]
pub struct DropFunctionStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone)]
pub struct WithCteStmt {
    pub ctes: Vec<CteDef>,
    pub body: Box<Statement>,
}

#[derive(Debug, Clone)]
pub struct CteDef {
    pub name: String,
    pub query: SelectStmt,
}

#[derive(Debug, Clone)]
pub struct TruncateTableStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone)]
pub struct AlterTableStmt {
    pub table: ObjectName,
    pub action: AlterTableAction,
}

#[derive(Debug, Clone)]
pub enum AlterTableAction {
    AddColumn(ColumnSpec),
    DropColumn(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Debug, Clone)]
pub struct SetOpStmt {
    pub left: Box<Statement>,
    pub op: SetOpKind,
    pub right: Box<Statement>,
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub name: ObjectName,
    pub columns: Vec<ColumnSpec>,
    pub table_constraints: Vec<TableConstraintSpec>,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub name: ObjectName,
    pub table: ObjectName,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DropIndexStmt {
    pub name: ObjectName,
    pub table: ObjectName,
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
    pub default_constraint_name: Option<String>,
    pub check: Option<Expr>,
    pub check_constraint_name: Option<String>,
    pub computed_expr: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum TableConstraintSpec {
    Default {
        name: String,
        column: String,
        expr: Expr,
    },
    Check {
        name: String,
        expr: Expr,
    },
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

#[derive(Debug, Clone)]
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
    Subquery(Box<SelectStmt>),
    Exists {
        subquery: Box<SelectStmt>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStmt>,
        negated: bool,
    },
}

#[derive(Debug, Clone)]
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
    SqlVariant,
}
