pub mod ddl;
pub mod dml;
pub mod procedural;
pub mod query;

use serde::{Deserialize, Serialize};
use crate::ast::expressions::Expr;
use crate::ast::data_types::DataTypeSpec;
use crate::ast::statements::dml::*;
use crate::ast::statements::ddl::*;
use crate::ast::statements::query::*;
use crate::ast::statements::procedural::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Statement {
    BeginTransaction(Option<String>),
    CommitTransaction(Option<String>),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
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
pub struct RoutineParam {
    pub name: String,
    pub data_type: DataTypeSpec,
    pub is_output: bool,
    pub default: Option<Expr>,
}
