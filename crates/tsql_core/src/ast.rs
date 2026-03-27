use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectName {
    pub schema: Option<String>,
    pub name: String,
}

impl ObjectName {
    pub fn schema_or_dbo(&self) -> &str {
        self.schema.as_deref().unwrap_or("dbo")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub name: ObjectName,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    SetOption(SetOptionStmt),
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
    CreateView(CreateViewStmt),
    DropView(DropViewStmt),
    Merge(MergeStmt),
    Print(Expr),
    DeclareCursor(DeclareCursorStmt),
    OpenCursor(String),
    FetchCursor(FetchCursorStmt),
    CloseCursor(String),
    DeallocateCursor(String),
    CreateTrigger(CreateTriggerStmt),
    DropTrigger(DropTriggerStmt),
    Raiserror(RaiserrorStmt),
    TryCatch(TryCatchStmt),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaiserrorStmt {
    pub message: Expr,
    pub severity: Expr,
    pub state: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TryCatchStmt {
    pub try_body: Vec<Statement>,
    pub catch_body: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareCursorStmt {
    pub name: String,
    pub query: SelectStmt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchCursorStmt {
    pub name: String,
    pub direction: FetchDirection,
    pub into: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FetchDirection {
    Next,
    Prior,
    First,
    Last,
    Absolute(Expr),
    Relative(Expr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTriggerStmt {
    pub name: ObjectName,
    pub table: ObjectName,
    pub events: Vec<TriggerEvent>,
    pub is_instead_of: bool,
    pub body: Vec<Statement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTriggerStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeStmt {
    pub target: TableRef,
    pub source: MergeSource,
    pub on_condition: Expr,
    pub when_clauses: Vec<MergeWhenClause>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<ObjectName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MergeSource {
    Table(TableRef),
    Subquery(SelectStmt, Option<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MergeWhen {
    Matched,
    NotMatched,
    NotMatchedBySource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeWhenClause {
    pub when: MergeWhen,
    pub condition: Option<Expr>,
    pub action: MergeAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MergeAction {
    Update { assignments: Vec<Assignment> },
    Insert { columns: Vec<String>, values: Vec<Expr> },
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareStmt {
    pub name: String,
    pub data_type: DataTypeSpec,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetStmt {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetOptionStmt {
    pub option: SessionOption,
    pub value: SessionOptionValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionOption {
    AnsiNulls,
    QuotedIdentifier,
    NoCount,
    XactAbort,
    DateFirst,
    Language,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionOptionValue {
    Bool(bool),
    Int(i32),
    Text(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IfStmt {
    pub condition: Expr,
    pub then_body: Vec<Statement>,
    pub else_body: Option<Vec<Statement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhileStmt {
    pub condition: Expr,
    pub body: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecStmt {
    pub sql_expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecArgument {
    pub name: Option<String>,
    pub expr: Expr,
    pub is_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecProcedureStmt {
    pub name: ObjectName,
    pub args: Vec<ExecArgument>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpExecuteSqlStmt {
    pub sql_expr: Expr,
    pub params_def: Option<Expr>,
    pub args: Vec<ExecArgument>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectAssignTarget {
    pub variable: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectAssignStmt {
    pub targets: Vec<SelectAssignTarget>,
    pub from: Option<TableRef>,
    pub joins: Vec<JoinClause>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareTableVarStmt {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub table_constraints: Vec<TableConstraintSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineParam {
    pub name: String,
    pub data_type: DataTypeSpec,
    pub is_output: bool,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProcedureStmt {
    pub name: ObjectName,
    pub params: Vec<RoutineParam>,
    pub body: Vec<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropProcedureStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionBody {
    ScalarReturn(Expr),
    Scalar(Vec<Statement>),
    InlineTable(SelectStmt),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFunctionStmt {
    pub name: ObjectName,
    pub params: Vec<RoutineParam>,
    pub returns: Option<DataTypeSpec>,
    pub body: FunctionBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropFunctionStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateViewStmt {
    pub name: ObjectName,
    pub query: SelectStmt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropViewStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithCteStmt {
    pub ctes: Vec<CteDef>,
    pub body: Box<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CteDef {
    pub name: String,
    pub query: SelectStmt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruncateTableStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterTableStmt {
    pub table: ObjectName,
    pub action: AlterTableAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTableAction {
    AddColumn(ColumnSpec),
    DropColumn(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOpKind {
    Union,
    UnionAll,
    Intersect,
    Except,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetOpStmt {
    pub left: Box<Statement>,
    pub op: SetOpKind,
    pub right: Box<Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableStmt {
    pub name: ObjectName,
    pub columns: Vec<ColumnSpec>,
    pub table_constraints: Vec<TableConstraintSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTableStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateIndexStmt {
    pub name: ObjectName,
    pub table: ObjectName,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropIndexStmt {
    pub name: ObjectName,
    pub table: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSchemaStmt {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropSchemaStmt {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    ForeignKey {
        name: String,
        columns: Vec<String>,
        referenced_table: ObjectName,
        referenced_columns: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertStmt {
    pub table: ObjectName,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expr>>,
    pub default_values: bool,
    pub select_source: Option<Box<SelectStmt>>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<ObjectName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputColumn {
    pub source: OutputSource,
    pub column: String,
    pub alias: Option<String>,
    #[serde(default)]
    pub is_wildcard: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputSource {
    Inserted,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectStmt {
    pub from: Option<TableRef>,
    pub joins: Vec<JoinClause>,
    pub applies: Vec<ApplyClause>,
    pub projection: Vec<SelectItem>,
    pub into_table: Option<ObjectName>,
    pub distinct: bool,
    pub top: Option<TopSpec>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub offset: Option<Expr>,
    pub fetch: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: TableRef,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ApplyClause {
    pub apply_type: ApplyType,
    pub subquery: SelectStmt,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ApplyType {
    Cross,
    Outer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopSpec {
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStmt {
    pub table: ObjectName,
    pub assignments: Vec<Assignment>,
    pub selection: Option<Expr>,
    pub from: Option<FromClause>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<ObjectName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FromClause {
    pub tables: Vec<TableRef>,
    pub joins: Vec<JoinClause>,
    #[serde(default)]
    pub applies: Vec<ApplyClause>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteStmt {
    pub table: ObjectName,
    pub selection: Option<Expr>,
    pub from: Option<FromClause>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<ObjectName>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expr {
    Identifier(String),
    QualifiedIdentifier(Vec<String>),
    Wildcard,
    Integer(i64),
    FloatLiteral(String),
    BinaryLiteral(Vec<u8>),
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
    WindowFunction {
        func: WindowFunc,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
        frame: Option<WindowFrame>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataTypeSpec {
    Bit,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Decimal(u8, u8),
    Money,
    SmallMoney,
    Char(u16),
    VarChar(u16),
    NChar(u16),
    NVarChar(u16),
    Binary(u16),
    VarBinary(u16),
    Date,
    Time,
    DateTime,
    DateTime2,
    UniqueIdentifier,
    SqlVariant,
}
