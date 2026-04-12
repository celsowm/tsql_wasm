use super::super::common::DataType;
use super::super::common::TableRef;
use super::super::expressions::Expr;
use super::query::{JoinClause, SelectStmt};
use serde::{Deserialize, Serialize};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Statement {
    Dml(DmlStatement),
    Ddl(DdlStatement),
    Procedural(ProceduralStatement),
    Transaction(TransactionStatement),
    Cursor(CursorStatement),
    Session(SessionStatement),
    WithCte {
        ctes: Vec<CteDef>,
        body: Box<Statement>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DmlStatement {
    Select(Box<SelectStmt>),
    Insert(Box<InsertStmt>),
    Update(Box<UpdateStmt>),
    Delete(Box<DeleteStmt>),
    Merge(Box<MergeStmt>),
    SelectAssign {
        assignments: Vec<SelectAssignTarget>,
        from: Option<TableRef>,
        selection: Option<Expr>,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DdlStatement {
    Create(Box<CreateStmt>),
    AlterTable {
        table: Vec<String>,
        action: AlterTableAction,
    },
    TruncateTable(Vec<String>),
    DropTable(Vec<String>),
    DropView(Vec<String>),
    DropProcedure(Vec<String>),
    DropFunction(Vec<String>),
    DropTrigger(Vec<String>),
    DropIndex {
        name: Vec<String>,
        table: Vec<String>,
    },
    DropType(Vec<String>),
    DropSchema(String),
    CreateIndex {
        name: Vec<String>,
        table: Vec<String>,
        columns: Vec<String>,
    },
    CreateType {
        name: Vec<String>,
        columns: Vec<ColumnDef>,
    },
    CreateSchema(String),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProceduralStatement {
    Declare(Vec<DeclareVar>),
    DeclareTableVar {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    DeclareCursor {
        name: String,
        query: SelectStmt,
    },
    Set {
        variable: String,
        expr: Expr,
    },
    If {
        condition: Expr,
        then_stmt: Box<Statement>,
        else_stmt: Option<Box<Statement>>,
    },
    BeginEnd(Vec<Statement>),
    While {
        condition: Expr,
        stmt: Box<Statement>,
    },
    Break,
    Continue,
    Return(Option<Expr>),
    Print(Expr),
    Raiserror {
        message: Expr,
        severity: Expr,
        state: Expr,
    },
    TryCatch {
        try_body: Vec<Statement>,
        catch_body: Vec<Statement>,
    },
    ExecDynamic {
        sql_expr: Expr,
    },
    ExecProcedure {
        return_variable: Option<String>,
        name: Vec<String>,
        args: Vec<ExecArg>,
    },
    SpExecuteSql {
        sql_expr: Expr,
        params_def: Option<Expr>,
        args: Vec<ExecArg>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionStatement {
    Begin(Option<String>),
    Commit(Option<String>),
    Rollback(Option<String>),
    Save(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CursorStatement {
    Open(String),
    Fetch {
        name: String,
        direction: FetchDirection,
        into_vars: Option<Vec<String>>,
    },
    Close(String),
    Deallocate(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SessionStatement {
    SetTransactionIsolationLevel(IsolationLevel),
    SetOption {
        option: SessionOption,
        value: SessionOptionValue,
    },
    SetIdentityInsert {
        table: Vec<String>,
        on: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MergeStmt {
    pub target: TableRef,
    pub source: TableRef,
    pub on_condition: Expr,
    pub when_clauses: Vec<MergeWhenClause>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MergeWhenClause {
    pub when: MergeWhen,
    pub condition: Option<Expr>,
    pub action: MergeAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MergeWhen {
    Matched,
    NotMatched,
    NotMatchedBySource,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MergeAction {
    Update {
        assignments: Vec<UpdateAssignment>,
    },
    Insert {
        columns: Vec<String>,
        values: Vec<Expr>,
    },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CteDef {
    pub name: String,
    pub columns: Vec<String>,
    pub query: SelectStmt,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InsertStmt {
    pub table: Vec<String>,
    pub columns: Vec<String>,
    pub source: InsertSource,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(Box<SelectStmt>),
    Exec {
        procedure: Vec<String>,
        args: Vec<Expr>,
    },
    DefaultValues,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateStmt {
    pub table: TableRef,
    pub assignments: Vec<UpdateAssignment>,
    pub top: Option<Expr>,
    pub from: Option<Vec<TableRef>>,
    pub joins: Vec<JoinClause>,
    pub selection: Option<Expr>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UpdateAssignment {
    pub column: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeleteStmt {
    pub table: Vec<String>,
    pub top: Option<Expr>,
    pub from: Vec<TableRef>,
    pub joins: Vec<JoinClause>,
    pub selection: Option<Expr>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeclareVar {
    pub name: String,
    pub data_type: DataType,
    pub initial_value: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CreateStmt {
    Table {
        name: Vec<String>,
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
    },
    View {
        name: Vec<String>,
        query: SelectStmt,
    },
    Procedure {
        name: Vec<String>,
        params: Vec<RoutineParam>,
        body: Vec<Statement>,
    },
    Function {
        name: Vec<String>,
        params: Vec<RoutineParam>,
        returns: Option<DataType>,
        body: FunctionBody,
    },
    Trigger {
        name: Vec<String>,
        table: Vec<String>,
        events: Vec<TriggerEvent>,
        is_instead_of: bool,
        body: Vec<Statement>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoutineParam {
    pub name: String,
    pub data_type: DataType,
    pub is_output: bool,
    pub is_readonly: bool,
    pub default: Option<Expr>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FunctionBody {
    ScalarReturn(Expr),
    Block(Vec<Statement>),
    Table(SelectStmt),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: Option<bool>,
    pub is_identity: bool,
    pub identity_spec: Option<(i64, i64)>,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub default_expr: Option<Expr>,
    pub default_constraint_name: Option<String>,
    pub check_expr: Option<Expr>,
    pub check_constraint_name: Option<String>,
    pub computed_expr: Option<Expr>,
    pub foreign_key: Option<ForeignKeyRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ForeignKeyRef {
    pub ref_table: Vec<String>,
    pub ref_columns: Vec<String>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AlterTableAction {
    AddColumn(ColumnDef),
    DropColumn(String),
    AddConstraint(TableConstraint),
    DropConstraint(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: Vec<String>,
        ref_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Check {
        name: Option<String>,
        expr: Expr,
    },
    Default {
        name: Option<String>,
        column: String,
        expr: Expr,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FetchDirection {
    Next,
    Prior,
    First,
    Last,
    Absolute(Expr),
    Relative(Expr),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SelectAssignTarget {
    pub variable: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecArg {
    pub name: Option<String>,
    pub expr: Expr,
    pub is_output: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputColumn {
    pub source: OutputSource,
    pub column: String,
    pub alias: Option<String>,
    pub is_wildcard: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OutputSource {
    Inserted,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SessionOption {
    AnsiNulls,
    QuotedIdentifier,
    NoCount,
    XactAbort,
    DateFirst,
    Language,
    DateFormat,
    LockTimeout,
    RowCount,
    TextSize,
    ConcatNullYieldsNull,
    ArithAbort,
    QueryGovernorCostLimit,
    DeadlockPriority,
    AnsiNullDfltOn,
    AnsiPadding,
    AnsiWarnings,
    CursorCloseOnCommit,
    ImplicitTransactions,
    StatisticsIo,
    StatisticsTime,
    ShowplanAll,
    AnsiDefaults,
    NoExec,
    ParseOnly,
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SessionOptionValue {
    Bool(bool),
    Int(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RoutineParamType {
    Scalar(super::super::data_types::DataTypeSpec),
    TableType(super::super::common::ObjectName),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}
