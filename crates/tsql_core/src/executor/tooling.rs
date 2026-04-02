use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::ast::{BinaryOp, DataTypeSpec, Expr, FunctionBody, JoinClause, JoinType, ObjectName, OrderByExpr, RoutineParam, RoutineParamType, SelectItem, SelectStmt, SessionOption, SessionOptionValue, SetOptionStmt, Statement, TableName, TableRef, TriggerEvent, UnaryOp};
use crate::parser::parse_sql;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionOptions {
    pub ansi_nulls: bool,
    pub quoted_identifier: bool,
    pub nocount: bool,
    pub xact_abort: bool,
    pub datefirst: i32,
    pub language: String,
    pub dateformat: String,
    pub lock_timeout_ms: i64,
    #[serde(skip)]
    pub identity_insert: HashSet<String>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            ansi_nulls: true,
            quoted_identifier: true,
            nocount: false,
            xact_abort: false,
            datefirst: 7,
            language: "us_english".to_string(),
            dateformat: "mdy".to_string(),
            lock_timeout_ms: 0,
            identity_insert: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpan {
    pub start_line: usize,
    pub start_col: usize,
    pub end_line: usize,
    pub end_col: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementSlice {
    pub index: usize,
    pub sql: String,
    pub normalized_sql: String,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SupportStatus {
    Supported,
    Partial,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityIssue {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityEntry {
    pub index: usize,
    pub sql: String,
    pub normalized_sql: String,
    pub span: SourceSpan,
    pub status: SupportStatus,
    pub feature_tags: Vec<String>,
    pub issues: Vec<CompatibilityIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityReport {
    pub entries: Vec<CompatibilityEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainOperator {
    pub op: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainPlan {
    pub statement_kind: String,
    pub operators: Vec<ExplainOperator>,
    pub read_tables: Vec<String>,
    pub write_tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceStatementEvent {
    pub index: usize,
    pub sql: String,
    pub normalized_sql: String,
    pub span: SourceSpan,
    pub status: String,
    pub warnings: Vec<String>,
    pub error: Option<String>,
    pub row_count: Option<usize>,
    pub read_tables: Vec<String>,
    pub write_tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTrace {
    pub events: Vec<TraceStatementEvent>,
    pub stopped_on_error: bool,
}

#[derive(Debug, Clone)]
pub struct SetOptionApply {
    pub warnings: Vec<String>,
}

pub fn apply_set_option(stmt: &SetOptionStmt, options: &mut SessionOptions) -> Result<SetOptionApply, crate::error::DbError> {
    let mut warnings = Vec::new();
    match (&stmt.option, &stmt.value) {
        (SessionOption::AnsiNulls, SessionOptionValue::Bool(v)) => {
            options.ansi_nulls = *v;
        }
        (SessionOption::QuotedIdentifier, SessionOptionValue::Bool(v)) => {
            options.quoted_identifier = *v;
        }
        (SessionOption::NoCount, SessionOptionValue::Bool(v)) => {
            options.nocount = *v;
        }
        (SessionOption::XactAbort, SessionOptionValue::Bool(v)) => {
            options.xact_abort = *v;
        }
        (SessionOption::DateFirst, SessionOptionValue::Int(v)) => {
            if !(1..=7).contains(v) {
                return Err(crate::error::DbError::Execution(
                    format!("The DATEFIRST value {} is outside the range of allowed values (1-7).", v)
                ));
            }
            options.datefirst = *v;
        }
        (SessionOption::Language, SessionOptionValue::Text(v)) => {
            options.language = v.clone();
            if !v.eq_ignore_ascii_case("us_english") {
                warnings.push(format!(
                    "SET LANGUAGE '{}' is accepted but only us_english behavior is modeled",
                    v
                ));
            }
        }
        (SessionOption::DateFormat, SessionOptionValue::Text(v)) => {
            options.dateformat = v.to_lowercase();
        }
        (SessionOption::LockTimeout, SessionOptionValue::Int(v)) => {
            options.lock_timeout_ms = *v as i64;
        }
        _ => {
            warnings.push("SET option value type mismatch; statement accepted with no state change".to_string());
        }
    }
    Ok(SetOptionApply { warnings })
}

pub fn analyze_sql_batch(sql: &str) -> CompatibilityReport {
    let slices = split_sql_statements(sql);
    let mut entries = Vec::with_capacity(slices.len());
    for slice in slices {
        match parse_sql(&slice.sql) {
            Ok(stmt) => {
                let mut status = SupportStatus::Supported;
                let mut issues = Vec::new();
                for warn in statement_compat_warnings(&stmt) {
                    status = SupportStatus::Partial;
                    issues.push(CompatibilityIssue {
                        code: "WARN_PARTIAL_MODEL".to_string(),
                        message: warn,
                    });
                }
                entries.push(CompatibilityEntry {
                    index: slice.index,
                    sql: slice.sql,
                    normalized_sql: slice.normalized_sql,
                    span: slice.span,
                    status,
                    feature_tags: feature_tags_for_statement(&stmt),
                    issues,
                });
            }
            Err(err) => entries.push(CompatibilityEntry {
                index: slice.index,
                sql: slice.sql,
                normalized_sql: slice.normalized_sql,
                span: slice.span,
                status: SupportStatus::Unsupported,
                feature_tags: vec!["unsupported".to_string()],
                issues: vec![CompatibilityIssue {
                    code: "ERR_UNSUPPORTED_STATEMENT".to_string(),
                    message: err.to_string(),
                }],
            }),
        }
    }
    CompatibilityReport { entries }
}

pub(crate) fn format_data_type_spec(dt: &DataTypeSpec) -> String {
    match dt {
        DataTypeSpec::Bit => "BIT".to_string(),
        DataTypeSpec::TinyInt => "TINYINT".to_string(),
        DataTypeSpec::SmallInt => "SMALLINT".to_string(),
        DataTypeSpec::Int => "INT".to_string(),
        DataTypeSpec::BigInt => "BIGINT".to_string(),
        DataTypeSpec::Float => "FLOAT".to_string(),
        DataTypeSpec::Decimal(p, s) => format!("DECIMAL({},{})", p, s),
        DataTypeSpec::Money => "MONEY".to_string(),
        DataTypeSpec::SmallMoney => "SMALLMONEY".to_string(),
        DataTypeSpec::Char(n) => format!("CHAR({})", n),
        DataTypeSpec::VarChar(n) => format!("VARCHAR({})", n),
        DataTypeSpec::NChar(n) => format!("NCHAR({})", n),
        DataTypeSpec::NVarChar(n) => format!("NVARCHAR({})", n),
        DataTypeSpec::Binary(n) => format!("BINARY({})", n),
        DataTypeSpec::VarBinary(n) => format!("VARBINARY({})", n),
        DataTypeSpec::Date => "DATE".to_string(),
        DataTypeSpec::Time => "TIME".to_string(),
        DataTypeSpec::DateTime => "DATETIME".to_string(),
        DataTypeSpec::DateTime2 => "DATETIME2".to_string(),
        DataTypeSpec::UniqueIdentifier => "UNIQUEIDENTIFIER".to_string(),
        DataTypeSpec::SqlVariant => "SQL_VARIANT".to_string(),
        DataTypeSpec::Numeric(p, s) => format!("NUMERIC({},{})", p, s),
        DataTypeSpec::Xml => "XML".to_string(),
    }
}

pub(crate) fn format_expr(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(name) => name.clone(),
        Expr::QualifiedIdentifier(parts) => parts.join("."),
        Expr::Wildcard => "*".to_string(),
        Expr::Integer(v) => v.to_string(),
        Expr::FloatLiteral(s) => s.clone(),
        Expr::BinaryLiteral(bytes) => crate::types::format_binary(bytes),
        Expr::String(s) => format!("'{}'", s),
        Expr::UnicodeString(s) => format!("N'{}'", s),
        Expr::Null => "NULL".to_string(),
        Expr::FunctionCall { name, args } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            format!("{}({})", name, args_str.join(", "))
        }
        Expr::Binary { left, op, right } => {
            let op_str = match op {
                BinaryOp::Eq => "=",
                BinaryOp::NotEq => "<>",
                BinaryOp::Gt => ">",
                BinaryOp::Lt => "<",
                BinaryOp::Gte => ">=",
                BinaryOp::Lte => "<=",
                BinaryOp::And => "AND",
                BinaryOp::Or => "OR",
                BinaryOp::Add => "+",
                BinaryOp::Subtract => "-",
                BinaryOp::Multiply => "*",
                BinaryOp::Divide => "/",
                BinaryOp::Modulo => "%",
            };
            format!("{} {} {}", format_expr(left), op_str, format_expr(right))
        }
        Expr::Unary { op, expr } => {
            let op_str = match op {
                UnaryOp::Negate => "-",
                UnaryOp::Not => "NOT",
            };
            format!("{}{}", op_str, format_expr(expr))
        }
        Expr::IsNull(inner) => format!("{} IS NULL", format_expr(inner)),
        Expr::IsNotNull(inner) => format!("{} IS NOT NULL", format_expr(inner)),
        Expr::Cast { expr, target } => format!("CAST({} AS {})", format_expr(expr), format_data_type_spec(target)),
        Expr::TryCast { expr, target } => format!("TRY_CAST({} AS {})", format_expr(expr), format_data_type_spec(target)),
        Expr::Convert { target, expr, style } => {
            if let Some(s) = style {
                format!("CONVERT({}, {}, {})", format_data_type_spec(target), format_expr(expr), s)
            } else {
                format!("CONVERT({}, {})", format_data_type_spec(target), format_expr(expr))
            }
        }
        Expr::TryConvert { target, expr, style } => {
            if let Some(s) = style {
                format!("TRY_CONVERT({}, {}, {})", format_data_type_spec(target), format_expr(expr), s)
            } else {
                format!("TRY_CONVERT({}, {})", format_data_type_spec(target), format_expr(expr))
            }
        }
        Expr::Case { operand, when_clauses, else_result } => {
            let mut parts = vec!["CASE".to_string()];
            if let Some(op) = operand {
                parts.push(format_expr(op));
            }
            for clause in when_clauses {
                parts.push(format!("WHEN {} THEN {}", format_expr(&clause.condition), format_expr(&clause.result)));
            }
            if let Some(else_expr) = else_result {
                parts.push(format!("ELSE {}", format_expr(else_expr)));
            }
            parts.push("END".to_string());
            parts.join(" ")
        }
        Expr::InList { expr, list, negated } => {
            let list_str: Vec<String> = list.iter().map(format_expr).collect();
            if *negated {
                format!("{} NOT IN ({})", format_expr(expr), list_str.join(", "))
            } else {
                format!("{} IN ({})", format_expr(expr), list_str.join(", "))
            }
        }
        Expr::Between { expr, low, high, negated } => {
            let not = if *negated { "NOT " } else { "" };
            format!("{} {}BETWEEN {} AND {}", format_expr(expr), not, format_expr(low), format_expr(high))
        }
        Expr::Like { expr, pattern, negated } => {
            let not = if *negated { "NOT " } else { "" };
            format!("{} {}LIKE {}", format_expr(expr), not, format_expr(pattern))
        }
        Expr::Subquery(_) => "(SELECT ...)".to_string(),
        Expr::Exists { subquery: _, negated } => {
            if *negated { "NOT EXISTS (...)" } else { "EXISTS (...)" }.to_string()
        }
        Expr::InSubquery { expr, subquery: _, negated } => {
            let not = if *negated { "NOT " } else { "" };
            format!("{} {}IN (...)", format_expr(expr), not)
        }
        Expr::WindowFunction { func, partition_by, order_by, frame: _, .. } => {
            let func_name_owned: String;
            let func_name = match func {
                crate::ast::WindowFunc::RowNumber => "ROW_NUMBER()",
                crate::ast::WindowFunc::Rank => "RANK()",
                crate::ast::WindowFunc::DenseRank => "DENSE_RANK()",
                crate::ast::WindowFunc::NTile => "NTILE()",
                crate::ast::WindowFunc::Lag => "LAG()",
                crate::ast::WindowFunc::Lead => "LEAD()",
                crate::ast::WindowFunc::FirstValue => "FIRST_VALUE()",
                crate::ast::WindowFunc::LastValue => "LAST_VALUE()",
                crate::ast::WindowFunc::Aggregate(name) => {
                    func_name_owned = format!("{}()", name);
                    &func_name_owned
                }
                crate::ast::WindowFunc::PercentileCont => "PERCENTILE_CONT()",
                crate::ast::WindowFunc::PercentileDisc => "PERCENTILE_DISC()",
                crate::ast::WindowFunc::PercentileRank => "PERCENTILE_RANK()",
            };
            let mut parts: Vec<String> = vec![func_name.to_string()];
            if !partition_by.is_empty() {
                let partition_str: Vec<String> = partition_by.iter().map(format_expr).collect();
                parts.push(format!("PARTITION BY {}", partition_str.join(", ")));
            }
            if !order_by.is_empty() {
                let order_str: Vec<String> = order_by.iter().map(|oe| {
                    let dir = if oe.asc { "" } else { " DESC" };
                    format!("{}{}", format_expr(&oe.expr), dir)
                }).collect();
                parts.push(format!("ORDER BY {}", order_str.join(", ")));
            }
            parts.join(" ")
        }
    }
}

pub(crate) fn format_object_name(name: &ObjectName) -> String {
    match name.schema.as_deref() {
        Some(schema) => format!("{}.{}", schema, name.name),
        None => name.name.clone(),
    }
}

fn format_table_ref(table: &TableRef) -> String {
    let mut out = match &table.name {
        TableName::Object(o) => format_object_name(o),
        TableName::Subquery(stmt) => format!("({})", format_select_stmt(stmt)),
    };
    if let Some(alias) = &table.alias {
        out.push_str(" AS ");
        out.push_str(alias);
    }
    out
}

fn format_order_by(items: &[OrderByExpr]) -> String {
    let parts: Vec<String> = items
        .iter()
        .map(|item| {
            let dir = if item.asc { "" } else { " DESC" };
            format!("{}{}", format_expr(&item.expr), dir)
        })
        .collect();
    parts.join(", ")
}

pub(crate) fn format_select_stmt(stmt: &SelectStmt) -> String {
    let mut out = String::from("SELECT ");
    if stmt.distinct {
        out.push_str("DISTINCT ");
    }
    if let Some(top) = &stmt.top {
        out.push_str("TOP ");
        out.push_str(&format_expr(&top.value));
        out.push(' ');
    }
    out.push_str(&format_select_columns(&stmt.projection));
    if let Some(into) = &stmt.into_table {
        out.push_str(" INTO ");
        out.push_str(&format_object_name(into));
    }
    if let Some(from) = &stmt.from {
        out.push_str(" FROM ");
        out.push_str(&format_table_ref(from));
    }
    for join in &stmt.joins {
        out.push(' ');
        out.push_str(&format_join(join));
    }
    for apply in &stmt.applies {
        out.push(' ');
        let apply_kw = match apply.apply_type {
            crate::ast::ApplyType::Cross => "CROSS APPLY",
            crate::ast::ApplyType::Outer => "OUTER APPLY",
        };
        out.push_str(apply_kw);
        out.push_str(" (");
        out.push_str(&format_select_stmt(&apply.subquery));
        out.push_str(") AS ");
        out.push_str(&apply.alias);
    }
    if let Some(selection) = &stmt.selection {
        out.push_str(" WHERE ");
        out.push_str(&format_expr(selection));
    }
    if !stmt.group_by.is_empty() {
        out.push_str(" GROUP BY ");
        let parts: Vec<String> = stmt.group_by.iter().map(format_expr).collect();
        out.push_str(&parts.join(", "));
    }
    if let Some(having) = &stmt.having {
        out.push_str(" HAVING ");
        out.push_str(&format_expr(having));
    }
    if !stmt.order_by.is_empty() {
        out.push_str(" ORDER BY ");
        out.push_str(&format_order_by(&stmt.order_by));
    }
    if let Some(offset) = &stmt.offset {
        out.push_str(" OFFSET ");
        out.push_str(&format_expr(offset));
        out.push_str(" ROWS");
        if let Some(fetch) = &stmt.fetch {
            out.push_str(" FETCH NEXT ");
            out.push_str(&format_expr(fetch));
            out.push_str(" ROWS ONLY");
        }
    }
    out
}

fn format_param(param: &RoutineParam) -> String {
    let mut out = param.name.clone();
    out.push(' ');
    match &param.param_type {
        RoutineParamType::Scalar(dt) => out.push_str(&format_data_type_spec(dt)),
        RoutineParamType::TableType(name) => out.push_str(&format_object_name(name)),
    }
    if param.is_output {
        out.push_str(" OUTPUT");
    }
    if param.is_readonly {
        out.push_str(" READONLY");
    }
    if let Some(default) = &param.default {
        out.push_str(" = ");
        out.push_str(&format_expr(default));
    }
    out
}

fn format_statement_list(stmts: &[Statement]) -> String {
    stmts
        .iter()
        .map(format_statement)
        .collect::<Vec<_>>()
        .join("; ")
}

pub(crate) fn format_statement(stmt: &Statement) -> String {
    match stmt {
        Statement::Select(s) => format_select_stmt(s),
        Statement::Insert(s) => format!("INSERT INTO {}", format_object_name(&s.table)),
        Statement::Update(s) => {
            let mut out = format!("UPDATE {}", format_object_name(&s.table));
            if !s.assignments.is_empty() {
                let assigns = s
                    .assignments
                    .iter()
                    .map(|a| format!("{} = {}", a.column, format_expr(&a.expr)))
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push_str(" SET ");
                out.push_str(&assigns);
            }
            out
        }
        Statement::Delete(s) => format!("DELETE FROM {}", format_object_name(&s.table)),
        Statement::Set(s) => format!("SET {} = {}", s.name, format_expr(&s.expr)),
        Statement::Declare(s) => {
            let mut out = format!("DECLARE {} {}", s.name, format_data_type_spec(&s.data_type));
            if let Some(default) = &s.default {
                out.push_str(" = ");
                out.push_str(&format_expr(default));
            }
            out
        }
        Statement::Return(Some(expr)) => format!("RETURN {}", format_expr(expr)),
        Statement::Return(None) => "RETURN".to_string(),
        Statement::Print(expr) => format!("PRINT {}", format_expr(expr)),
        Statement::Raiserror(stmt) => format!(
            "RAISERROR({}, {}, {})",
            format_expr(&stmt.message),
            format_expr(&stmt.severity),
            format_expr(&stmt.state)
        ),
        Statement::BeginEnd(stmts) => format!("BEGIN {} END", format_statement_list(stmts)),
        Statement::If(stmt) => {
            let mut out = format!("IF {} ", format_expr(&stmt.condition));
            out.push_str(&format!("BEGIN {} END", format_statement_list(&stmt.then_body)));
            if let Some(else_body) = &stmt.else_body {
                out.push_str(" ELSE ");
                out.push_str(&format!("BEGIN {} END", format_statement_list(else_body)));
            }
            out
        }
        Statement::While(stmt) => format!(
            "WHILE {} BEGIN {} END",
            format_expr(&stmt.condition),
            format_statement_list(&stmt.body)
        ),
        Statement::TryCatch(stmt) => format!(
            "BEGIN TRY {} END TRY BEGIN CATCH {} END CATCH",
            format_statement_list(&stmt.try_body),
            format_statement_list(&stmt.catch_body)
        ),
        Statement::ExecDynamic(stmt) => format!("EXEC({})", format_expr(&stmt.sql_expr)),
        Statement::ExecProcedure(stmt) => {
            let mut out = format!("EXEC {}", format_object_name(&stmt.name));
            if !stmt.args.is_empty() {
                let args = stmt
                    .args
                    .iter()
                    .map(|a| {
                        let mut item = String::new();
                        if let Some(name) = &a.name {
                            item.push_str(name);
                            item.push_str(" = ");
                        }
                        item.push_str(&format_expr(&a.expr));
                        if a.is_output {
                            item.push_str(" OUTPUT");
                        }
                        item
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push(' ');
                out.push_str(&args);
            }
            out
        }
        Statement::SpExecuteSql(stmt) => {
            let mut out = format!("EXEC sp_executesql {}", format_expr(&stmt.sql_expr));
            if let Some(params) = &stmt.params_def {
                out.push_str(", ");
                out.push_str(&format_expr(params));
            }
            out
        }
        Statement::SelectAssign(stmt) => {
            let assigns = stmt
                .targets
                .iter()
                .map(|t| format!("{} = {}", t.variable, format_expr(&t.expr)))
                .collect::<Vec<_>>()
                .join(", ");
            let mut out = format!("SELECT {}", assigns);
            if let Some(from) = &stmt.from {
                out.push_str(" FROM ");
                out.push_str(&format_table_ref(from));
            }
            out
        }
        Statement::Merge(_) => "MERGE".to_string(),
        Statement::DeclareCursor(stmt) => {
            format!("DECLARE {} CURSOR FOR {}", stmt.name, format_select_stmt(&stmt.query))
        }
        Statement::OpenCursor(name) => format!("OPEN {}", name),
        Statement::FetchCursor(stmt) => format!("FETCH {}", stmt.name),
        Statement::CloseCursor(name) => format!("CLOSE {}", name),
        Statement::DeallocateCursor(name) => format!("DEALLOCATE {}", name),
        Statement::CreateTable(stmt) => format!("CREATE TABLE {}", format_object_name(&stmt.name)),
        Statement::CreateView(stmt) => format!("CREATE VIEW {}", format_object_name(&stmt.name)),
        Statement::CreateProcedure(stmt) => {
            let params = stmt
                .params
                .iter()
                .map(format_param)
                .collect::<Vec<_>>()
                .join(", ");
            let mut out = format!("CREATE PROCEDURE {} ", format_object_name(&stmt.name));
            if !params.is_empty() {
                out.push('(');
                out.push_str(&params);
                out.push(')');
            }
            out.push_str(" AS BEGIN ");
            out.push_str(&format_statement_list(&stmt.body));
            out.push_str(" END");
            out
        }
        Statement::CreateFunction(stmt) => {
            let params = stmt
                .params
                .iter()
                .map(format_param)
                .collect::<Vec<_>>()
                .join(", ");
            let mut out = format!("CREATE FUNCTION {} ", format_object_name(&stmt.name));
            if !params.is_empty() {
                out.push('(');
                out.push_str(&params);
                out.push(')');
            }
            if let Some(returns) = &stmt.returns {
                out.push_str(" RETURNS ");
                out.push_str(&format_data_type_spec(returns));
            }
            match &stmt.body {
                FunctionBody::ScalarReturn(expr) => {
                    out.push_str(" AS RETURN ");
                    out.push_str(&format_expr(expr));
                }
                FunctionBody::Scalar(stmts) => {
                    out.push_str(" AS BEGIN ");
                    out.push_str(&format_statement_list(stmts));
                    out.push_str(" END");
                }
                FunctionBody::InlineTable(select) => {
                    out.push_str(" AS RETURN ");
                    out.push_str(&format_select_stmt(select));
                }
            }
            out
        }
        Statement::CreateTrigger(stmt) => {
            let events = stmt
                .events
                .iter()
                .map(|event| match event {
                    TriggerEvent::Insert => "INSERT",
                    TriggerEvent::Update => "UPDATE",
                    TriggerEvent::Delete => "DELETE",
                })
                .collect::<Vec<_>>()
                .join(", ");
            let scope = if stmt.is_instead_of {
                "INSTEAD OF"
            } else {
                "AFTER"
            };
            format!(
                "CREATE TRIGGER {} ON {} {} {} AS BEGIN {} END",
                format_object_name(&stmt.name),
                format_object_name(&stmt.table),
                scope,
                events,
                format_statement_list(&stmt.body)
            )
        }
        _ => statement_kind(stmt).to_string(),
    }
}

pub(crate) fn format_routine_definition(routine: &crate::catalog::RoutineDef) -> String {
    let params = routine
        .params
        .iter()
        .map(format_param)
        .collect::<Vec<_>>()
        .join(", ");
    let name = ObjectName {
        schema: Some(routine.schema.clone()),
        name: routine.name.clone(),
    };
    let mut out = match &routine.kind {
        crate::catalog::RoutineKind::Procedure { body } => {
            let mut s = format!("CREATE PROCEDURE {} ", format_object_name(&name));
            if !params.is_empty() {
                s.push('(');
                s.push_str(&params);
                s.push(')');
            }
            s.push_str(" AS BEGIN ");
            s.push_str(&format_statement_list(body));
            s.push_str(" END");
            s
        }
        crate::catalog::RoutineKind::Function { returns, body } => {
            let mut s = format!("CREATE FUNCTION {} ", format_object_name(&name));
            if !params.is_empty() {
                s.push('(');
                s.push_str(&params);
                s.push(')');
            }
            if let Some(returns) = returns {
                s.push_str(" RETURNS ");
                s.push_str(&format_data_type_spec(returns));
            }
            match body {
                FunctionBody::ScalarReturn(expr) => {
                    s.push_str(" AS RETURN ");
                    s.push_str(&format_expr(expr));
                }
                FunctionBody::Scalar(stmts) => {
                    s.push_str(" AS BEGIN ");
                    s.push_str(&format_statement_list(stmts));
                    s.push_str(" END");
                }
                FunctionBody::InlineTable(select) => {
                    s.push_str(" AS RETURN ");
                    s.push_str(&format_select_stmt(select));
                }
            }
            s
        }
    };
    if out.is_empty() {
        out = format_object_name(&name);
    }
    out
}

pub(crate) fn format_view_definition(view: &crate::catalog::ViewDef) -> String {
    format!(
        "CREATE VIEW {} AS {}",
        format_object_name(&ObjectName {
            schema: Some(view.schema.clone()),
            name: view.name.clone(),
        }),
        match &view.query {
            Statement::Select(select) => format_select_stmt(select),
            other => format_statement(other),
        }
    )
}

pub(crate) fn format_trigger_definition(trigger: &crate::catalog::TriggerDef) -> String {
    let events = trigger
        .events
        .iter()
        .map(|event| match event {
            TriggerEvent::Insert => "INSERT",
            TriggerEvent::Update => "UPDATE",
            TriggerEvent::Delete => "DELETE",
        })
        .collect::<Vec<_>>()
        .join(", ");
    let scope = if trigger.is_instead_of {
        "INSTEAD OF"
    } else {
        "AFTER"
    };
    format!(
        "CREATE TRIGGER {} ON {} {} {} AS BEGIN {} END",
        format_object_name(&ObjectName {
            schema: Some(trigger.schema.clone()),
            name: trigger.name.clone(),
        }),
        format_object_name(&ObjectName {
            schema: Some(trigger.table_schema.clone()),
            name: trigger.table_name.clone(),
        }),
        scope,
        events,
        format_statement_list(&trigger.body)
    )
}

fn format_select_columns(projection: &[SelectItem]) -> String {
    if projection.is_empty() {
        return "*".to_string();
    }
    let cols: Vec<String> = projection.iter().map(|item| {
        if let Some(alias) = &item.alias {
            format!("{} AS {}", format_expr(&item.expr), alias)
        } else {
            format_expr(&item.expr)
        }
    }).collect();
    cols.join(", ")
}

fn format_join(join: &JoinClause) -> String {
    let join_type = match join.join_type {
        JoinType::Inner => "INNER JOIN",
        JoinType::Left => "LEFT JOIN",
        JoinType::Right => "RIGHT JOIN",
        JoinType::Full => "FULL OUTER JOIN",
        JoinType::Cross => "CROSS JOIN",
    };
    if let Some(on_expr) = &join.on {
        format!("{} {} ON {}", join_type, normalize_table_ref(&join.table), format_expr(on_expr))
    } else {
        format!("{} {}", join_type, normalize_table_ref(&join.table))
    }
}

pub fn explain_statement(stmt: &Statement) -> ExplainPlan {
    let mut operators = Vec::new();
    let statement_kind = statement_kind(stmt).to_string();
    match stmt {
        Statement::Select(s) => {
            operators.push(ExplainOperator {
                op: "Scan".to_string(),
                detail: format!("from {}", select_from_name(s)),
            });
            for join in &s.joins {
                operators.push(ExplainOperator {
                    op: "Join".to_string(),
                    detail: format_join(join),
                });
            }
            if let Some(where_expr) = &s.selection {
                operators.push(ExplainOperator {
                    op: "Filter".to_string(),
                    detail: format!("WHERE {}", format_expr(where_expr)),
                });
            }
            if !s.group_by.is_empty() {
                let group_exprs: Vec<String> = s.group_by.iter().map(format_expr).collect();
                let mut detail = format!("GROUP BY {}", group_exprs.join(", "));
                if let Some(having) = &s.having {
                    detail = format!("{} HAVING {}", detail, format_expr(having));
                }
                operators.push(ExplainOperator {
                    op: "Aggregate".to_string(),
                    detail,
                });
            } else if s.having.is_some() {
                if let Some(having) = &s.having {
                    operators.push(ExplainOperator {
                        op: "Aggregate".to_string(),
                        detail: format!("HAVING {}", format_expr(having)),
                    });
                }
            }
            if !s.order_by.is_empty() {
                let order_exprs: Vec<String> = s.order_by.iter().map(|oe| {
                    let dir = if oe.asc { "" } else { " DESC" };
                    format!("{}{}", format_expr(&oe.expr), dir)
                }).collect();
                operators.push(ExplainOperator {
                    op: "Sort".to_string(),
                    detail: format!("ORDER BY {}", order_exprs.join(", ")),
                });
            }
            operators.push(ExplainOperator {
                op: "Project".to_string(),
                detail: format_select_columns(&s.projection),
            });
        }
        Statement::Insert(i) => operators.push(ExplainOperator {
            op: "Insert".to_string(),
            detail: normalize_object_name(&i.table),
        }),
        Statement::Update(u) => {
            let mut detail = normalize_object_name(&u.table);
            if !u.assignments.is_empty() {
                let assigns: Vec<String> = u.assignments.iter().map(|a| {
                    format!("{} = {}", a.column, format_expr(&a.expr))
                }).collect();
                detail = format!("{} SET {}", detail, assigns.join(", "));
            }
            operators.push(ExplainOperator {
                op: "Update".to_string(),
                detail,
            });
        }
        Statement::Delete(d) => operators.push(ExplainOperator {
            op: "Delete".to_string(),
            detail: format!("FROM {}", normalize_object_name(&d.table)),
        }),
        Statement::CreateTable(c) => operators.push(ExplainOperator {
            op: "DDL".to_string(),
            detail: format!("CREATE TABLE {}", normalize_object_name(&c.name)),
        }),
        Statement::AlterTable(a) => operators.push(ExplainOperator {
            op: "DDL".to_string(),
            detail: format!("ALTER TABLE {}", normalize_object_name(&a.table)),
        }),
        _ => operators.push(ExplainOperator {
            op: "Statement".to_string(),
            detail: statement_kind.clone(),
        }),
    }

    let mut read_tables: Vec<String> = collect_read_tables(stmt).into_iter().collect();
    let mut write_tables: Vec<String> = collect_write_tables(stmt).into_iter().collect();
    read_tables.sort();
    write_tables.sort();

    ExplainPlan {
        statement_kind,
        operators,
        read_tables,
        write_tables,
    }
}

pub fn statement_compat_warnings(stmt: &Statement) -> Vec<String> {
    if let Statement::SetOption(opt) = stmt {
        match (&opt.option, &opt.value) {
            (SessionOption::DateFirst, SessionOptionValue::Int(v)) if !(1..=7).contains(v) => {
                return vec![format!(
                    "DATEFIRST {} outside SQL Server range 1..7 (accepted for compatibility)",
                    v
                )]
            }
            (SessionOption::Language, SessionOptionValue::Text(v))
                if !v.eq_ignore_ascii_case("us_english") =>
            {
                return vec![format!(
                    "LANGUAGE '{}' accepted, but only us_english behavior is modeled",
                    v
                )]
            }
            _ => {}
        }
    }
    Vec::new()
}

pub fn split_sql_statements(sql: &str) -> Vec<StatementSlice> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_string = false;
    let mut paren_depth = 0usize;
    let mut block_depth = 0usize;
    let mut line = 1usize;
    let mut col = 1usize;
    let mut start_line = 1usize;
    let mut start_col = 1usize;
    let chars: Vec<char> = sql.chars().collect();
    let mut idx = 0usize;

    while idx < chars.len() {
        let ch = chars[idx];
        if buf.is_empty() && ch.is_whitespace() {
            advance_pos(ch, &mut line, &mut col);
            idx += 1;
            continue;
        }
        if buf.is_empty() {
            start_line = line;
            start_col = col;
        }

        if ch == '\'' {
            in_string = !in_string;
        } else if !in_string {
            if ch == '(' {
                paren_depth += 1;
            } else if ch == ')' {
                paren_depth = paren_depth.saturating_sub(1);
            }
        }

        if !in_string && paren_depth == 0 {
            if starts_keyword(&chars, idx, "BEGIN") && !starts_keyword(&chars, idx, "BEGIN TRAN") {
                block_depth += 1;
            } else if starts_keyword(&chars, idx, "END") && block_depth > 0 {
                block_depth -= 1;
            }
        }

        if ch == ';' && !in_string && paren_depth == 0 && block_depth == 0 {
            push_slice(&mut out, &buf, start_line, start_col, line, col);
            buf.clear();
            advance_pos(ch, &mut line, &mut col);
            idx += 1;
            continue;
        }

        buf.push(ch);
        advance_pos(ch, &mut line, &mut col);
        idx += 1;
    }
    push_slice(
        &mut out,
        &buf,
        start_line,
        start_col,
        line,
        col.saturating_sub(1),
    );

    for (i, item) in out.iter_mut().enumerate() {
        item.index = i;
    }
    out
}

fn push_slice(
    out: &mut Vec<StatementSlice>,
    buf: &str,
    start_line: usize,
    start_col: usize,
    end_line: usize,
    end_col: usize,
) {
    let trimmed = buf.trim();
    if trimmed.is_empty() {
        return;
    }
    out.push(StatementSlice {
        index: 0,
        sql: trimmed.to_string(),
        normalized_sql: normalize_sql(trimmed),
        span: SourceSpan {
            start_line,
            start_col,
            end_line,
            end_col,
        },
    });
}

fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn advance_pos(ch: char, line: &mut usize, col: &mut usize) {
    if ch == '\n' {
        *line += 1;
        *col = 1;
    } else {
        *col += 1;
    }
}

fn starts_keyword(chars: &[char], idx: usize, kw: &str) -> bool {
    let target: Vec<char> = kw.chars().collect();
    if idx + target.len() > chars.len() {
        return false;
    }
    let mut got = String::new();
    for ch in &chars[idx..idx + target.len()] {
        got.push(ch.to_ascii_uppercase());
    }
    got == kw.to_ascii_uppercase()
}

fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::Select(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE_TABLE",
        Statement::CreateView(_) => "CREATE_VIEW",
        Statement::DropView(_) => "DROP_VIEW",
        Statement::CreateIndex(_) => "CREATE_INDEX",
        Statement::DropTable(_) => "DROP_TABLE",
        Statement::AlterTable(_) => "ALTER_TABLE",
        Statement::SetOption(_) => "SET_OPTION",
        Statement::Set(_) => "SET_VARIABLE",
        Statement::BeginTransaction(_) => "BEGIN_TRANSACTION",
        Statement::CommitTransaction(_) => "COMMIT",
        Statement::RollbackTransaction(_) => "ROLLBACK",
        _ => "STATEMENT",
    }
}

fn feature_tags_for_statement(stmt: &Statement) -> Vec<String> {
    let mut tags = Vec::new();
    match stmt {
        Statement::SetOption(_) => tags.push("set-option".to_string()),
        Statement::Select(_) | Statement::SetOp(_) | Statement::WithCte(_) => {
            tags.push("query".to_string())
        }
        Statement::CreateTable(_)
        | Statement::CreateView(_)
        | Statement::DropView(_)
        | Statement::AlterTable(_)
        | Statement::DropTable(_)
        | Statement::CreateSchema(_)
        | Statement::DropSchema(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_) => tags.push("ddl".to_string()),
        Statement::BeginTransaction(_)
        | Statement::CommitTransaction(_)
        | Statement::RollbackTransaction(_)
        | Statement::SaveTransaction(_)
        | Statement::SetTransactionIsolationLevel(_) => tags.push("transaction".to_string()),
        Statement::CreateProcedure(_)
        | Statement::DropProcedure(_)
        | Statement::CreateFunction(_)
        | Statement::DropFunction(_)
        | Statement::ExecDynamic(_)
        | Statement::ExecProcedure(_)
        | Statement::SpExecuteSql(_)
        | Statement::If(_)
        | Statement::While(_)
        | Statement::BeginEnd(_)
        | Statement::Declare(_)
        | Statement::Set(_)
        | Statement::DeclareTableVar(_)
        | Statement::SelectAssign(_) => tags.push("procedural".to_string()),
        _ => {}
    }
    tags
}

pub fn collect_read_tables(stmt: &Statement) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    match stmt {
        Statement::Select(s) => collect_tables_from_select(s, &mut out),
        Statement::Update(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Delete(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::SelectAssign(s) => {
            if let Some(from) = &s.from {
                out.insert(normalize_table_ref(from));
            }
            for join in &s.joins {
                out.insert(normalize_table_ref(&join.table));
            }
        }
        Statement::SetOp(s) => {
            collect_tables_from_statement(&s.left, &mut out);
            collect_tables_from_statement(&s.right, &mut out);
        }
        Statement::WithCte(s) => {
            for cte in &s.ctes {
                collect_tables_from_statement(&cte.query, &mut out);
            }
            out.extend(collect_read_tables(&s.body));
        }
        _ => {}
    }
    out
}

fn collect_tables_from_statement(stmt: &Statement, out: &mut std::collections::HashSet<String>) {
    match stmt {
        Statement::Select(s) => collect_tables_from_select(s, out),
        Statement::SetOp(s) => {
            collect_tables_from_statement(&s.left, out);
            collect_tables_from_statement(&s.right, out);
        }
        Statement::WithCte(s) => {
            for cte in &s.ctes {
                collect_tables_from_statement(&cte.query, out);
            }
            collect_tables_from_statement(&s.body, out);
        }
        _ => {}
    }
}

pub fn collect_write_tables(stmt: &Statement) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    match stmt {
        Statement::Insert(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Update(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::Delete(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::CreateTable(s) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::CreateView(s) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::DropTable(s) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::DropView(s) => {
            out.insert(normalize_object_name(&s.name));
        }
        Statement::AlterTable(s) => {
            out.insert(normalize_object_name(&s.table));
        }
        Statement::TruncateTable(s) => {
            out.insert(normalize_object_name(&s.name));
        }
        _ => {}
    }
    out
}

fn collect_tables_from_select(stmt: &SelectStmt, out: &mut std::collections::HashSet<String>) {
    if let Some(from) = &stmt.from {
        out.insert(normalize_table_ref(from));
    }
    for join in &stmt.joins {
        out.insert(normalize_table_ref(&join.table));
    }
}

fn normalize_table_ref(table: &TableRef) -> String {
    match &table.name {
        crate::ast::TableName::Object(o) => normalize_object_name(o),
        crate::ast::TableName::Subquery(_) => "(SUBQUERY)".to_string(),
    }
}

fn normalize_object_name(name: &ObjectName) -> String {
    format!(
        "{}.{}",
        name.schema.as_deref().unwrap_or("dbo").to_uppercase(),
        name.name.to_uppercase()
    )
}

fn select_from_name(stmt: &SelectStmt) -> String {
    stmt.from
        .as_ref()
        .map(normalize_table_ref)
        .unwrap_or_else(|| "<none>".to_string())
}
