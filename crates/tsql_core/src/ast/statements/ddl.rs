use serde::{Deserialize, Serialize};
use crate::ast::ObjectName;
use crate::ast::expressions::Expr;
use crate::ast::data_types::DataTypeSpec;
use crate::ast::statements::query::SelectStmt;

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
    AddConstraint(TableConstraintSpec),
    DropConstraint(String),
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
    pub foreign_key: Option<ForeignKeyRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyRef {
    pub referenced_table: ObjectName,
    pub referenced_columns: Vec<String>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
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
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    PrimaryKey {
        name: String,
        columns: Vec<String>,
    },
    Unique {
        name: String,
        columns: Vec<String>,
    },
}
