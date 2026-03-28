use serde::{Deserialize, Serialize};
use crate::ast::ObjectName;
use crate::ast::expressions::Expr;
use crate::ast::statements::query::{SelectStmt, JoinClause, ApplyClause, TopSpec};
use crate::ast::common::TableRef;
use crate::ast::Statement;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertStmt {
    pub table: ObjectName,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<ObjectName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(Box<SelectStmt>),
    Exec(Box<Statement>),
    DefaultValues,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStmt {
    pub table: ObjectName,
    pub assignments: Vec<Assignment>,
    pub top: Option<TopSpec>,
    pub selection: Option<Expr>,
    pub from: Option<FromClause>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<ObjectName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteStmt {
    pub table: ObjectName,
    pub top: Option<TopSpec>,
    pub selection: Option<Expr>,
    pub from: Option<FromClause>,
    pub output: Option<Vec<OutputColumn>>,
    pub output_into: Option<ObjectName>,
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
