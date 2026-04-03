use super::super::expressions::Expr;
use super::super::common::DataType;
use super::query::{SelectStmt, TableRef};
use crate::ast as old;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Statement<'a> {
    Dml(DmlStatement<'a>),
    Ddl(DdlStatement<'a>),
    Procedural(ProceduralStatement<'a>),
    Transaction(TransactionStatement<'a>),
    Cursor(CursorStatement<'a>),
    Session(SessionStatement<'a>),
    WithCte {
        ctes: Vec<CteDef<'a>>,
        body: Box<Statement<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DmlStatement<'a> {
    Select(Box<SelectStmt<'a>>),
    Insert(Box<InsertStmt<'a>>),
    Update(Box<UpdateStmt<'a>>),
    Delete(Box<DeleteStmt<'a>>),
    Merge(Box<MergeStmt<'a>>),
    SelectAssign {
        assignments: Vec<SelectAssignTarget<'a>>,
        from: Option<Vec<TableRef<'a>>>,
        selection: Option<Expr<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DdlStatement<'a> {
    Create(Box<CreateStmt<'a>>),
    AlterTable {
        table: Vec<Cow<'a, str>>,
        action: AlterTableAction<'a>,
    },
    TruncateTable(Vec<Cow<'a, str>>),
    DropTable(Vec<Cow<'a, str>>),
    DropView(Vec<Cow<'a, str>>),
    DropProcedure(Vec<Cow<'a, str>>),
    DropFunction(Vec<Cow<'a, str>>),
    DropTrigger(Vec<Cow<'a, str>>),
    DropIndex {
        name: Vec<Cow<'a, str>>,
        table: Vec<Cow<'a, str>>,
    },
    DropType(Vec<Cow<'a, str>>),
    DropSchema(Cow<'a, str>),
    CreateIndex {
        name: Vec<Cow<'a, str>>,
        table: Vec<Cow<'a, str>>,
        columns: Vec<Cow<'a, str>>,
    },
    CreateType {
        name: Vec<Cow<'a, str>>,
        columns: Vec<ColumnDef<'a>>,
    },
    CreateSchema(Cow<'a, str>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProceduralStatement<'a> {
    Declare(Vec<DeclareVar<'a>>),
    DeclareTableVar {
        name: Cow<'a, str>,
        columns: Vec<ColumnDef<'a>>,
        constraints: Vec<TableConstraint<'a>>,
    },
    DeclareCursor {
        name: Cow<'a, str>,
        query: SelectStmt<'a>,
    },
    Set {
        variable: Cow<'a, str>,
        expr: Expr<'a>,
    },
    If {
        condition: Expr<'a>,
        then_stmt: Box<Statement<'a>>,
        else_stmt: Option<Box<Statement<'a>>>,
    },
    BeginEnd(Vec<Statement<'a>>),
    While {
        condition: Expr<'a>,
        stmt: Box<Statement<'a>>,
    },
    Break,
    Continue,
    Return(Option<Expr<'a>>),
    Print(Expr<'a>),
    Raiserror {
        message: Expr<'a>,
        severity: Expr<'a>,
        state: Expr<'a>,
    },
    TryCatch {
        try_body: Vec<Statement<'a>>,
        catch_body: Vec<Statement<'a>>,
    },
    ExecDynamic {
        sql_expr: Expr<'a>,
    },
    ExecProcedure {
        name: Vec<Cow<'a, str>>,
        args: Vec<ExecArg<'a>>,
    },
    SpExecuteSql {
        sql_expr: Expr<'a>,
        params_def: Option<Expr<'a>>,
        args: Vec<ExecArg<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionStatement<'a> {
    Begin(Option<Cow<'a, str>>),
    Commit(Option<Cow<'a, str>>),
    Rollback(Option<Cow<'a, str>>),
    Save(Cow<'a, str>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CursorStatement<'a> {
    Open(Cow<'a, str>),
    Fetch {
        name: Cow<'a, str>,
        direction: FetchDirection<'a>,
        into_vars: Option<Vec<Cow<'a, str>>>,
    },
    Close(Cow<'a, str>),
    Deallocate(Cow<'a, str>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SessionStatement<'a> {
    SetTransactionIsolationLevel(crate::ast::IsolationLevel),
    SetOption {
        option: crate::ast::SessionOption,
        value: crate::ast::SessionOptionValue,
    },
    SetIdentityInsert {
        table: Vec<Cow<'a, str>>,
        on: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MergeStmt<'a> {
    pub target: TableRef<'a>,
    pub source: TableRef<'a>,
    pub on_condition: Expr<'a>,
    pub when_clauses: Vec<MergeWhenClause<'a>>,
    pub output: Option<Vec<OutputColumn<'a>>>,
    pub output_into: Option<Vec<Cow<'a, str>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MergeWhenClause<'a> {
    pub when: MergeWhen,
    pub condition: Option<Expr<'a>>,
    pub action: MergeAction<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeWhen {
    Matched,
    NotMatched,
    NotMatchedBySource,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MergeAction<'a> {
    Update { assignments: Vec<UpdateAssignment<'a>> },
    Insert { columns: Vec<Cow<'a, str>>, values: Vec<Expr<'a>> },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CteDef<'a> {
    pub name: Cow<'a, str>,
    pub columns: Vec<Cow<'a, str>>,
    pub query: SelectStmt<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InsertStmt<'a> {
    pub table: Vec<Cow<'a, str>>,
    pub columns: Vec<Cow<'a, str>>,
    pub source: InsertSource<'a>,
    pub output: Option<Vec<OutputColumn<'a>>>,
    pub output_into: Option<Vec<Cow<'a, str>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InsertSource<'a> {
    Values(Vec<Vec<Expr<'a>>>),
    Select(Box<SelectStmt<'a>>),
    Exec {
        procedure: Vec<Cow<'a, str>>,
        args: Vec<Expr<'a>>,
    },
    DefaultValues,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpdateStmt<'a> {
    pub table: TableRef<'a>,
    pub assignments: Vec<UpdateAssignment<'a>>,
    pub top: Option<Expr<'a>>,
    pub from: Option<Vec<TableRef<'a>>>,
    pub selection: Option<Expr<'a>>,
    pub output: Option<Vec<OutputColumn<'a>>>,
    pub output_into: Option<Vec<Cow<'a, str>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpdateAssignment<'a> {
    pub column: Cow<'a, str>,
    pub expr: Expr<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeleteStmt<'a> {
    pub target_alias: Option<Cow<'a, str>>,
    pub top: Option<Expr<'a>>,
    pub from: Vec<TableRef<'a>>,
    pub selection: Option<Expr<'a>>,
    pub output: Option<Vec<OutputColumn<'a>>>,
    pub output_into: Option<Vec<Cow<'a, str>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeclareVar<'a> {
    pub name: Cow<'a, str>,
    pub data_type: DataType<'a>,
    pub initial_value: Option<Expr<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CreateStmt<'a> {
    Table {
        name: Vec<Cow<'a, str>>,
        columns: Vec<ColumnDef<'a>>,
        constraints: Vec<TableConstraint<'a>>,
    },
    View {
        name: Vec<Cow<'a, str>>,
        query: SelectStmt<'a>,
    },
    Procedure {
        name: Vec<Cow<'a, str>>,
        params: Vec<RoutineParam<'a>>,
        body: Vec<Statement<'a>>,
    },
    Function {
        name: Vec<Cow<'a, str>>,
        params: Vec<RoutineParam<'a>>,
        returns: Option<DataType<'a>>,
        body: FunctionBody<'a>,
    },
    Trigger {
        name: Vec<Cow<'a, str>>,
        table: Vec<Cow<'a, str>>,
        events: Vec<old::TriggerEvent>,
        is_instead_of: bool,
        body: Vec<Statement<'a>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoutineParam<'a> {
    pub name: Cow<'a, str>,
    pub data_type: DataType<'a>,
    pub is_output: bool,
    pub default: Option<Expr<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionBody<'a> {
    ScalarReturn(Expr<'a>),
    Block(Vec<Statement<'a>>),
    Table(SelectStmt<'a>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnDef<'a> {
    pub name: Cow<'a, str>,
    pub data_type: DataType<'a>,
    pub is_nullable: Option<bool>,
    pub is_identity: bool,
    pub identity_spec: Option<(i64, i64)>,
    pub is_primary_key: bool,
    pub is_unique: bool,
    pub default_expr: Option<Expr<'a>>,
    pub default_constraint_name: Option<Cow<'a, str>>,
    pub check_expr: Option<Expr<'a>>,
    pub check_constraint_name: Option<Cow<'a, str>>,
    pub computed_expr: Option<Expr<'a>>,
    pub foreign_key: Option<ForeignKeyRef<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ForeignKeyRef<'a> {
    pub ref_table: Vec<Cow<'a, str>>,
    pub ref_columns: Vec<Cow<'a, str>>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AlterTableAction<'a> {
    AddColumn(ColumnDef<'a>),
    DropColumn(Cow<'a, str>),
    AddConstraint(TableConstraint<'a>),
    DropConstraint(Cow<'a, str>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableConstraint<'a> {
    PrimaryKey {
        name: Option<Cow<'a, str>>,
        columns: Vec<Cow<'a, str>>,
    },
    Unique {
        name: Option<Cow<'a, str>>,
        columns: Vec<Cow<'a, str>>,
    },
    ForeignKey {
        name: Option<Cow<'a, str>>,
        columns: Vec<Cow<'a, str>>,
        ref_table: Vec<Cow<'a, str>>,
        ref_columns: Vec<Cow<'a, str>>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
    Check {
        name: Option<Cow<'a, str>>,
        expr: Expr<'a>,
    },
    Default {
        name: Option<Cow<'a, str>>,
        column: Cow<'a, str>,
        expr: Expr<'a>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReferentialAction {
    NoAction,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FetchDirection<'a> {
    Next,
    Prior,
    First,
    Last,
    Absolute(Expr<'a>),
    Relative(Expr<'a>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SelectAssignTarget<'a> {
    pub variable: Cow<'a, str>,
    pub expr: Expr<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExecArg<'a> {
    pub name: Option<Cow<'a, str>>,
    pub expr: Expr<'a>,
    pub is_output: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OutputColumn<'a> {
    pub source: OutputSource,
    pub column: Cow<'a, str>,
    pub alias: Option<Cow<'a, str>>,
    pub is_wildcard: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputSource {
    Inserted,
    Deleted,
}
