use super::normalize_table_ref;
use crate::ast::{
    BinaryOp, DataTypeSpec, Expr, FromNode, FunctionBody, JoinClause, JoinType, ObjectName,
    OrderByExpr, RoutineParam, RoutineParamType, SelectItem, SelectStmt, Statement, TableFactor,
    TableRef, TriggerEvent, UnaryOp,
};

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
        DataTypeSpec::Vector(n) => format!("VECTOR({})", n),
        DataTypeSpec::Date => "DATE".to_string(),
        DataTypeSpec::Time => "TIME".to_string(),
        DataTypeSpec::DateTime => "DATETIME".to_string(),
        DataTypeSpec::DateTime2 => "DATETIME2".to_string(),
        DataTypeSpec::SmallDateTime => "SMALLDATETIME".to_string(),
        DataTypeSpec::DateTimeOffset => "DATETIMEOFFSET".to_string(),
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
        Expr::QualifiedWildcard(parts) => format!("{}.*", parts.join(".")),
        Expr::Integer(v) => v.to_string(),
        Expr::FloatLiteral(s) => s.clone(),
        Expr::BinaryLiteral(bytes) => crate::types::format_binary(bytes),
        Expr::String(s) => format!("'{}'", s),
        Expr::UnicodeString(s) => format!("N'{}'", s),
        Expr::Null => "NULL".to_string(),
        Expr::FunctionCall {
            name,
            args,
            within_group,
        } => {
            let args_str: Vec<String> = args.iter().map(format_expr).collect();
            let mut out = format!("{}({})", name, args_str.join(", "));
            if !within_group.is_empty() {
                let order_by = within_group
                    .iter()
                    .map(|oe| {
                        let dir = if oe.asc { "" } else { " DESC" };
                        format!("{}{}", format_expr(&oe.expr), dir)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                out.push_str(" WITHIN GROUP (ORDER BY ");
                out.push_str(&order_by);
                out.push(')');
            }
            out
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
                BinaryOp::BitwiseAnd => "&",
                BinaryOp::BitwiseOr => "|",
                BinaryOp::BitwiseXor => "^",
            };
            format!("{} {} {}", format_expr(left), op_str, format_expr(right))
        }
        Expr::Unary { op, expr } => {
            let op_str = match op {
                UnaryOp::Negate => "-",
                UnaryOp::Not => "NOT ",
                UnaryOp::BitwiseNot => "~",
            };
            format!("{}{}", op_str, format_expr(expr))
        }
        Expr::IsNull(inner) => format!("{} IS NULL", format_expr(inner)),
        Expr::IsNotNull(inner) => format!("{} IS NOT NULL", format_expr(inner)),
        Expr::Cast { expr, target } => format!(
            "CAST({} AS {})",
            format_expr(expr),
            format_data_type_spec(target)
        ),
        Expr::TryCast { expr, target } => format!(
            "TRY_CAST({} AS {})",
            format_expr(expr),
            format_data_type_spec(target)
        ),
        Expr::Convert {
            target,
            expr,
            style,
        } => {
            if let Some(s) = style {
                format!(
                    "CONVERT({}, {}, {})",
                    format_data_type_spec(target),
                    format_expr(expr),
                    s
                )
            } else {
                format!(
                    "CONVERT({}, {})",
                    format_data_type_spec(target),
                    format_expr(expr)
                )
            }
        }
        Expr::TryConvert {
            target,
            expr,
            style,
        } => {
            if let Some(s) = style {
                format!(
                    "TRY_CONVERT({}, {}, {})",
                    format_data_type_spec(target),
                    format_expr(expr),
                    s
                )
            } else {
                format!(
                    "TRY_CONVERT({}, {})",
                    format_data_type_spec(target),
                    format_expr(expr)
                )
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            let mut parts = vec!["CASE".to_string()];
            if let Some(op) = operand {
                parts.push(format_expr(op));
            }
            for clause in when_clauses {
                parts.push(format!(
                    "WHEN {} THEN {}",
                    format_expr(&clause.condition),
                    format_expr(&clause.result)
                ));
            }
            if let Some(else_expr) = else_result {
                parts.push(format!("ELSE {}", format_expr(else_expr)));
            }
            parts.push("END".to_string());
            parts.join(" ")
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let list_str: Vec<String> = list.iter().map(format_expr).collect();
            if *negated {
                format!("{} NOT IN ({})", format_expr(expr), list_str.join(", "))
            } else {
                format!("{} IN ({})", format_expr(expr), list_str.join(", "))
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let not = if *negated { "NOT " } else { "" };
            format!(
                "{} {}BETWEEN {} AND {}",
                format_expr(expr),
                not,
                format_expr(low),
                format_expr(high)
            )
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let not = if *negated { "NOT " } else { "" };
            format!("{} {}LIKE {}", format_expr(expr), not, format_expr(pattern))
        }
        Expr::Subquery(_) => "(SELECT ...)".to_string(),
        Expr::Exists {
            subquery: _,
            negated,
        } => if *negated {
            "NOT EXISTS (...)"
        } else {
            "EXISTS (...)"
        }
        .to_string(),
        Expr::InSubquery {
            expr,
            subquery: _,
            negated,
        } => {
            let not = if *negated { "NOT " } else { "" };
            format!("{} {}IN (...)", format_expr(expr), not)
        }
        Expr::WindowFunction {
            func,
            partition_by,
            order_by,
            frame: _,
            ..
        } => {
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
                let order_str: Vec<String> = order_by
                    .iter()
                    .map(|oe| {
                        let dir = if oe.asc { "" } else { " DESC" };
                        format!("{}{}", format_expr(&oe.expr), dir)
                    })
                    .collect();
                parts.push(format!("ORDER BY {}", order_str.join(", ")));
            }
            parts.join(" ")
        }
        Expr::NextValueFor { sequence_name } => {
            format!("NEXT VALUE FOR {}", format_object_name(sequence_name))
        }
    }
}

pub(crate) fn format_object_name(name: &ObjectName) -> String {
    match name.schema.as_deref() {
        Some(schema) => format!("{}.{}", schema, name.name),
        None => name.name.clone(),
    }
}

pub(crate) fn format_table_ref(table: &TableRef) -> String {
    let mut out = match &table.factor {
        TableFactor::Named(o) => format_object_name(o),
        TableFactor::Derived(stmt) => format!("({})", format_select_stmt(stmt)),
        TableFactor::Values { .. } => "(VALUES (...))".to_string(),
    };
    if let Some(alias) = &table.alias {
        out.push_str(" AS ");
        out.push_str(alias);
    }
    out
}

pub(crate) fn format_order_by(items: &[OrderByExpr]) -> String {
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
    if let Some(from) = &stmt.from_clause {
        out.push_str(" FROM ");
        out.push_str(&format_from_node(from));
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
    if let Some(set_op) = &stmt.set_op {
        let op_kw = match set_op.kind {
            crate::ast::SetOpKind::Union => " UNION ",
            crate::ast::SetOpKind::UnionAll => " UNION ALL ",
            crate::ast::SetOpKind::Intersect => " INTERSECT ",
            crate::ast::SetOpKind::Except => " EXCEPT ",
        };
        out.push_str(op_kw);
        out.push_str(&format_select_stmt(&set_op.right));
    }
    out
}

pub(crate) fn format_from_node(node: &FromNode) -> String {
    match node {
        FromNode::Table(table) => format_table_ref(table),
        FromNode::Aliased { source, alias } => {
            format!("({}) AS {}", format_from_node(source), alias)
        }
        FromNode::Join {
            left,
            join_type,
            right,
            on,
        } => {
            let join_kw = match join_type {
                JoinType::Inner => "INNER JOIN",
                JoinType::Left => "LEFT JOIN",
                JoinType::Right => "RIGHT JOIN",
                JoinType::Full => "FULL OUTER JOIN",
                JoinType::Cross => "CROSS JOIN",
            };
            if let Some(on_expr) = on {
                format!(
                    "{} {} {} ON {}",
                    format_from_node(left),
                    join_kw,
                    format_from_node(right),
                    format_expr(on_expr)
                )
            } else {
                format!(
                    "{} {} {}",
                    format_from_node(left),
                    join_kw,
                    format_from_node(right)
                )
            }
        }
    }
}

pub(crate) fn format_param(param: &RoutineParam) -> String {
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

pub(crate) fn format_statement_list(stmts: &[Statement]) -> String {
    stmts
        .iter()
        .map(format_statement)
        .collect::<Vec<_>>()
        .join("; ")
}

pub(crate) fn format_statement(stmt: &Statement) -> String {
    match stmt {
        Statement::Dml(crate::ast::DmlStatement::Select(s)) => format_select_stmt(s),
        Statement::Dml(crate::ast::DmlStatement::Insert(s)) => {
            format!("INSERT INTO {}", format_object_name(&s.table))
        }
        Statement::Dml(crate::ast::DmlStatement::Update(s)) => {
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
        Statement::Dml(crate::ast::DmlStatement::Delete(s)) => {
            format!("DELETE FROM {}", format_object_name(&s.table))
        }
        Statement::Procedural(crate::ast::ProceduralStatement::Set(s)) => {
            format!("SET {} = {}", s.name, format_expr(&s.expr))
        }
        Statement::Procedural(crate::ast::ProceduralStatement::Declare(s)) => {
            let mut out = format!("DECLARE {} {}", s.name, format_data_type_spec(&s.data_type));
            if let Some(default) = &s.default {
                out.push_str(" = ");
                out.push_str(&format_expr(default));
            }
            out
        }
        Statement::Procedural(crate::ast::ProceduralStatement::Return(Some(expr))) => {
            format!("RETURN {}", format_expr(expr))
        }
        Statement::Procedural(crate::ast::ProceduralStatement::Return(None)) => {
            "RETURN".to_string()
        }
        Statement::Procedural(crate::ast::ProceduralStatement::Print(expr)) => {
            format!("PRINT {}", format_expr(expr))
        }
        Statement::Procedural(crate::ast::ProceduralStatement::Raiserror(stmt)) => format!(
            "RAISERROR({}, {}, {})",
            format_expr(&stmt.message),
            format_expr(&stmt.severity),
            format_expr(&stmt.state)
        ),
        Statement::Procedural(crate::ast::ProceduralStatement::BeginEnd(stmts)) => {
            format!("BEGIN {} END", format_statement_list(stmts))
        }
        Statement::Procedural(crate::ast::ProceduralStatement::If(stmt)) => {
            let mut out = format!("IF {} ", format_expr(&stmt.condition));
            out.push_str(&format!(
                "BEGIN {} END",
                format_statement_list(&stmt.then_body)
            ));
            if let Some(else_body) = &stmt.else_body {
                out.push_str(" ELSE ");
                out.push_str(&format!("BEGIN {} END", format_statement_list(else_body)));
            }
            out
        }
        Statement::Procedural(crate::ast::ProceduralStatement::While(stmt)) => format!(
            "WHILE {} BEGIN {} END",
            format_expr(&stmt.condition),
            format_statement_list(&stmt.body)
        ),
        Statement::Procedural(crate::ast::ProceduralStatement::TryCatch(stmt)) => format!(
            "BEGIN TRY {} END TRY BEGIN CATCH {} END CATCH",
            format_statement_list(&stmt.try_body),
            format_statement_list(&stmt.catch_body)
        ),
        Statement::Procedural(crate::ast::ProceduralStatement::ExecDynamic(stmt)) => {
            format!("EXEC({})", format_expr(&stmt.sql_expr))
        }
        Statement::Procedural(crate::ast::ProceduralStatement::ExecProcedure(stmt)) => {
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
        Statement::Procedural(crate::ast::ProceduralStatement::SpExecuteSql(stmt)) => {
            let mut out = format!("EXEC sp_executesql {}", format_expr(&stmt.sql_expr));
            if let Some(params) = &stmt.params_def {
                out.push_str(", ");
                out.push_str(&format_expr(params));
            }
            out
        }
        Statement::Dml(crate::ast::DmlStatement::SelectAssign(stmt)) => {
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
            for join in &stmt.joins {
                out.push(' ');
                out.push_str(&format_join(join));
            }
            out
        }
        Statement::Dml(crate::ast::DmlStatement::Merge(_)) => "MERGE".to_string(),
        Statement::Procedural(crate::ast::ProceduralStatement::DeclareCursor(stmt)) => {
            format!(
                "DECLARE {} CURSOR FOR {}",
                stmt.name,
                format_select_stmt(&stmt.query)
            )
        }
        Statement::Cursor(crate::ast::CursorStatement::OpenCursor(name)) => {
            format!("OPEN {}", name)
        }
        Statement::Cursor(crate::ast::CursorStatement::FetchCursor(stmt)) => {
            format!("FETCH {}", stmt.name)
        }
        Statement::Cursor(crate::ast::CursorStatement::CloseCursor(name)) => {
            format!("CLOSE {}", name)
        }
        Statement::Cursor(crate::ast::CursorStatement::DeallocateCursor(name)) => {
            format!("DEALLOCATE {}", name)
        }
        Statement::Ddl(crate::ast::DdlStatement::CreateTable(stmt)) => {
            format!("CREATE TABLE {}", format_object_name(&stmt.name))
        }
        Statement::Procedural(crate::ast::ProceduralStatement::CreateView(stmt)) => {
            format!("CREATE VIEW {}", format_object_name(&stmt.name))
        }
        Statement::Procedural(crate::ast::ProceduralStatement::CreateProcedure(stmt)) => {
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
        Statement::Procedural(crate::ast::ProceduralStatement::CreateFunction(stmt)) => {
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
        Statement::Procedural(crate::ast::ProceduralStatement::CreateTrigger(stmt)) => {
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
        Statement::Ddl(crate::ast::DdlStatement::DropView(stmt)) => {
            format!("DROP VIEW {}", format_object_name(&stmt.name))
        }
        Statement::Ddl(crate::ast::DdlStatement::DropTable(stmt)) => {
            format!("DROP TABLE {}", format_object_name(&stmt.name))
        }
        Statement::Ddl(crate::ast::DdlStatement::AlterTable(stmt)) => {
            format!("ALTER TABLE {}", format_object_name(&stmt.table))
        }
        Statement::Ddl(crate::ast::DdlStatement::CreateSchema(stmt)) => {
            format!("CREATE SCHEMA {}", stmt.name)
        }
        Statement::Ddl(crate::ast::DdlStatement::DropSchema(stmt)) => {
            format!("DROP SCHEMA {}", stmt.name)
        }
        Statement::Ddl(crate::ast::DdlStatement::CreateIndex(stmt)) => {
            format!(
                "CREATE INDEX {} ON {}",
                format_object_name(&stmt.name),
                format_object_name(&stmt.table)
            )
        }
        Statement::Ddl(crate::ast::DdlStatement::DropIndex(stmt)) => {
            format!(
                "DROP INDEX {} ON {}",
                format_object_name(&stmt.name),
                format_object_name(&stmt.table)
            )
        }
        Statement::Ddl(crate::ast::DdlStatement::TruncateTable(stmt)) => {
            format!("TRUNCATE TABLE {}", format_object_name(&stmt.name))
        }
        _ => super::formatting_kind::statement_kind(stmt).to_string(),
    }
}

pub fn format_routine_definition(routine: &crate::catalog::RoutineDef) -> String {
    let params = routine
        .params
        .iter()
        .map(format_param)
        .collect::<Vec<_>>()
        .join(", ");
    let name = ObjectName {
        database: None,
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

pub fn format_view_definition(view: &crate::catalog::ViewDef) -> String {
    format!(
        "CREATE VIEW {} AS {}",
        format_object_name(&ObjectName {
            database: None,
            schema: Some(view.schema.clone()),
            name: view.name.clone(),
        }),
        match &view.query {
            Statement::Dml(crate::ast::DmlStatement::Select(select)) => format_select_stmt(select),
            other => format_statement(other),
        }
    )
}

pub fn format_trigger_definition(trigger: &crate::catalog::TriggerDef) -> String {
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
            database: None,
            schema: Some(trigger.schema.clone()),
            name: trigger.name.clone(),
        }),
        format_object_name(&ObjectName {
            database: None,
            schema: Some(trigger.table_schema.clone()),
            name: trigger.table_name.clone(),
        }),
        scope,
        events,
        format_statement_list(&trigger.body)
    )
}

pub(crate) fn format_select_columns(projection: &[SelectItem]) -> String {
    if projection.is_empty() {
        return "*".to_string();
    }
    let cols: Vec<String> = projection
        .iter()
        .map(|item| {
            if let Some(alias) = &item.alias {
                format!("{} AS {}", format_expr(&item.expr), alias)
            } else {
                format_expr(&item.expr)
            }
        })
        .collect();
    cols.join(", ")
}

pub(crate) fn format_join(join: &JoinClause) -> String {
    let join_type = match join.join_type {
        JoinType::Inner => "INNER JOIN",
        JoinType::Left => "LEFT JOIN",
        JoinType::Right => "RIGHT JOIN",
        JoinType::Full => "FULL OUTER JOIN",
        JoinType::Cross => "CROSS JOIN",
    };
    if let Some(on_expr) = &join.on {
        format!(
            "{} {} ON {}",
            join_type,
            normalize_table_ref(&join.table),
            format_expr(on_expr)
        )
    } else {
        format!("{} {}", join_type, normalize_table_ref(&join.table))
    }
}
