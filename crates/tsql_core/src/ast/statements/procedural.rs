use serde::{Deserialize, Serialize};
use crate::ast::ObjectName;
use crate::ast::expressions::Expr;
use crate::ast::data_types::DataTypeSpec;
use crate::ast::statements::query::SelectStmt;
use crate::ast::common::TableRef;
use crate::ast::statements::ddl::{ColumnSpec, TableConstraintSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IfStmt {
    pub condition: Expr,
    pub then_body: Vec<crate::ast::Statement>,
    pub else_body: Option<Vec<crate::ast::Statement>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhileStmt {
    pub condition: Expr,
    pub body: Vec<crate::ast::Statement>,
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
    pub option: crate::ast::SessionOption,
    pub value: crate::ast::SessionOptionValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareTableVarStmt {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub table_constraints: Vec<TableConstraintSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateProcedureStmt {
    pub name: ObjectName,
    pub params: Vec<crate::ast::RoutineParam>,
    pub body: Vec<crate::ast::Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropProcedureStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionBody {
    ScalarReturn(Expr),
    Scalar(Vec<crate::ast::Statement>),
    InlineTable(SelectStmt),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFunctionStmt {
    pub name: ObjectName,
    pub params: Vec<crate::ast::RoutineParam>,
    pub returns: Option<DataTypeSpec>,
    pub body: FunctionBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropFunctionStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithCteStmt {
    pub recursive: bool,
    pub ctes: Vec<CteDef>,
    pub body: Box<crate::ast::Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CteDef {
    pub name: String,
    pub query: crate::ast::Statement,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectAssignStmt {
    pub targets: Vec<SelectAssignTarget>,
    pub from: Option<TableRef>,
    pub joins: Vec<crate::ast::statements::query::JoinClause>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectAssignTarget {
    pub variable: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaiserrorStmt {
    pub message: Expr,
    pub severity: Expr,
    pub state: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TryCatchStmt {
    pub try_body: Vec<crate::ast::Statement>,
    pub catch_body: Vec<crate::ast::Statement>,
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
    pub body: Vec<crate::ast::Statement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropTriggerStmt {
    pub name: ObjectName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}
