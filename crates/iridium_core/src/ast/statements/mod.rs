pub mod ddl;
pub mod dml;
pub mod procedural;
pub mod query;
pub mod visitor;

pub use visitor::*;

use crate::ast::data_types::DataTypeSpec;
use crate::ast::expressions::Expr;
use crate::ast::statements::ddl::*;
use crate::ast::statements::dml::*;
use crate::ast::statements::procedural::*;
use crate::ast::statements::query::*;
use serde::{Deserialize, Serialize};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Statement {
    Dml(DmlStatement),
    Ddl(DdlStatement),
    Procedural(ProceduralStatement),
    Transaction(TransactionStatement),
    Cursor(CursorStatement),
    Session(SessionStatement),
    WithCte(WithCteStmt),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DmlStatement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Merge(MergeStmt),
    SetOp(SetOpStmt),
    SelectAssign(SelectAssignStmt),
    BulkInsert(BulkInsertStmt),
    InsertBulk(InsertBulkStmt),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DdlStatement {
    CreateTable(CreateTableStmt),
    CreateIndex(CreateIndexStmt),
    CreateType(CreateTypeStmt),
    CreateSchema(CreateSchemaStmt),
    DropTable(DropTableStmt),
    DropView(DropViewStmt),
    DropProcedure(DropProcedureStmt),
    DropFunction(DropFunctionStmt),
    DropTrigger(DropTriggerStmt),
    DropIndex(DropIndexStmt),
    DropType(DropTypeStmt),
    DropSchema(DropSchemaStmt),
    TruncateTable(TruncateTableStmt),
    AlterTable(AlterTableStmt),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProceduralStatement {
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
    Print(Expr),
    DeclareTableVar(DeclareTableVarStmt),
    DeclareCursor(DeclareCursorStmt),
    CreateProcedure(CreateProcedureStmt),
    CreateFunction(CreateFunctionStmt),
    CreateView(CreateViewStmt),
    CreateTrigger(CreateTriggerStmt),
    Raiserror(RaiserrorStmt),
    TryCatch(TryCatchStmt),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionStatement {
    Begin(Option<String>),
    Commit(Option<String>),
    Rollback(Option<String>),
    Save(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CursorStatement {
    OpenCursor(String),
    FetchCursor(FetchCursorStmt),
    CloseCursor(String),
    DeallocateCursor(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionStatement {
    SetTransactionIsolationLevel(IsolationLevel),
    SetOption(SetOptionStmt),
    SetIdentityInsert(SetIdentityInsertStmt),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum IsolationLevel {
    ReadUncommitted,
    #[default]
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
    FmtOnly,
    NoExec,
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
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SessionOptionValue {
    Bool(bool),
    Int(i64),
    Text(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RoutineParamType {
    Scalar(DataTypeSpec),
    TableType(crate::ast::ObjectName),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineParam {
    pub name: String,
    pub param_type: RoutineParamType,
    pub is_output: bool,
    pub is_readonly: bool,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetIdentityInsertStmt {
    pub table: crate::ast::ObjectName,
    pub on: bool,
}
