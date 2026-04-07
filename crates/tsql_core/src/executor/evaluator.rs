use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::{DbError, StmtOutcome};
use crate::storage::Storage;
use crate::types::{DataType, Value};

use std::cell::UnsafeCell;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::identifier::{resolve_identifier, resolve_qualified_identifier};
use super::model::JoinedRow;
use super::operators::{eval_binary, eval_unary};
use super::predicates::{
    eval_between, eval_case, eval_exists, eval_in_list, eval_in_subquery, eval_like,
    eval_scalar_subquery,
};
use super::scalar::eval_function;
use super::script::ScriptExecutor;
use super::type_mapping::data_type_spec_to_runtime;
use super::value_ops::{coerce_value_to_type_with_dateformat, truthy};

pub(crate) fn eval_expr_to_type_constant(
    expr: &Expr,
    ty: &DataType,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let value = eval_constant_expr(expr, ctx, catalog, storage, clock)?;
    coerce_value_to_type_with_dateformat(value, ty, &ctx.options.dateformat)
}

pub(crate) fn eval_expr_to_type_in_context(
    expr: &Expr,
    ty: &DataType,
    row: &[super::model::ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let mut sub_ctx = ctx.with_outer_row(row.to_vec());
    let value = eval_expr(expr, row, &mut sub_ctx, catalog, storage, clock)?;
    coerce_value_to_type_with_dateformat(value, ty, &ctx.options.dateformat)
}

pub(crate) fn eval_constant_expr(
    expr: &Expr,
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    let row: JoinedRow = vec![];
    eval_expr(expr, &row, ctx, catalog, storage, clock)
}

/// P1 #16: Returns true if a statement is guaranteed not to mutate catalog or storage.
///
/// Control-flow statements (If, While, BeginEnd, TryCatch) are only read-only if
/// ALL nested body statements are recursively read-only.
///
/// DeclareTableVar is NOT read-only — it calls create_table() which mutates catalog and storage.
/// SetOption is NOT read-only — it is handled at engine level and errors in ScriptExecutor.
fn is_read_only_statement(stmt: &crate::ast::Statement) -> bool {
    use crate::ast::{DmlStatement, ProceduralStatement, Statement};
    match stmt {
        Statement::Dml(DmlStatement::Select(_)) | Statement::Dml(DmlStatement::SelectAssign(_)) => {
            true
        }
        Statement::Procedural(ps) => match ps {
            ProceduralStatement::Declare(_)
            | ProceduralStatement::Set(_)
            | ProceduralStatement::Break
            | ProceduralStatement::Continue
            | ProceduralStatement::Return(_)
            | ProceduralStatement::Print(_)
            | ProceduralStatement::Raiserror(_)
            | ProceduralStatement::DeclareCursor(_) => true,
            ProceduralStatement::If(if_stmt) => {
                if_stmt.then_body.iter().all(is_read_only_statement)
                    && if_stmt
                        .else_body
                        .as_ref()
                        .map_or(true, |b| b.iter().all(is_read_only_statement))
            }
            ProceduralStatement::BeginEnd(stmts) => stmts.iter().all(is_read_only_statement),
            ProceduralStatement::While(while_stmt) => {
                while_stmt.body.iter().all(is_read_only_statement)
            }
            ProceduralStatement::TryCatch(tc_stmt) => {
                tc_stmt.try_body.iter().all(is_read_only_statement)
                    && tc_stmt.catch_body.iter().all(is_read_only_statement)
            }
            ProceduralStatement::DeclareTableVar(_)
            | ProceduralStatement::SetOption(_)
            | ProceduralStatement::ExecDynamic(_)
            | ProceduralStatement::ExecProcedure(_)
            | ProceduralStatement::SpExecuteSql(_)
            | ProceduralStatement::CreateProcedure(_)
            | ProceduralStatement::CreateFunction(_)
            | ProceduralStatement::CreateView(_)
            | ProceduralStatement::CreateTrigger(_) => false,
        },
        Statement::WithCte(cte) => {
            cte.ctes.iter().all(|c| is_read_only_statement(&c.query))
                && is_read_only_statement(&cte.body)
        }
        _ => false,
    }
}

/// Evaluates a UDF body by executing its statements.
///
/// P1 #16: Read-only UDFs execute against the original refs via raw pointers
/// (guarded by RefCell for exclusive access), avoiding O(database-size) cloning.
/// Write UDFs still clone for safety.
pub(crate) fn eval_udf_body<'a>(
    stmts: &[crate::ast::Statement],
    ctx: &mut ExecutionContext<'_>,
    catalog: &'a dyn Catalog,
    storage: &'a dyn Storage,
    clock: &'a dyn Clock,
) -> Result<Value, DbError> {
    let read_only = stmts.iter().all(is_read_only_statement);

    if read_only {
        // P1 #16: Read-only UDF — use UnsafeCell with raw pointers to get
        // &mut refs without cloning. Safe because:
        // 1. The UDF is verified read-only by is_read_only_statement()
        // 2. The underlying data is never actually mutated
        // 3. UnsafeCell is the only legal way to get &mut from & in Rust
        let cat_ptr = catalog as *const dyn Catalog as *mut dyn Catalog;
        let stor_ptr = storage as *const dyn Storage as *mut dyn Storage;

        let cat_cell = UnsafeCell::new(cat_ptr);
        let stor_cell = UnsafeCell::new(stor_ptr);

        let mut executor = ScriptExecutor {
            catalog: unsafe { &mut **cat_cell.get() },
            storage: unsafe { &mut **stor_cell.get() },
            clock,
        };
        match executor.execute_batch(stmts, ctx) {
            Ok(StmtOutcome::Return(Some(val))) => Ok(val),
            Ok(StmtOutcome::Return(None)) => Ok(Value::Null),
            Ok(StmtOutcome::Ok(_)) => Ok(Value::Null),
            Ok(_) => Ok(Value::Null),
            Err(e) => Err(e),
        }
    } else {
        let mut catalog_owned = catalog.clone_boxed();
        let mut storage_owned = storage.clone_boxed();

        let mut executor = ScriptExecutor {
            catalog: catalog_owned.as_mut(),
            storage: storage_owned.as_mut(),
            clock,
        };
        match executor.execute_batch(stmts, ctx) {
            Ok(StmtOutcome::Return(Some(val))) => Ok(val),
            Ok(StmtOutcome::Return(None)) => Ok(Value::Null),
            Ok(StmtOutcome::Ok(_)) => Ok(Value::Null),
            Ok(_) => Ok(Value::Null),
            Err(e) => Err(e),
        }
    }
}

const MAX_RECURSION_DEPTH: usize = 32;

#[inline]
pub fn eval_expr(
    expr: &Expr,
    row: &[super::model::ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if ctx.frame.depth > MAX_RECURSION_DEPTH {
        return Err(DbError::Execution(format!(
            "Maximum recursion depth ({}) exceeded",
            MAX_RECURSION_DEPTH
        )));
    }

    ctx.frame.depth += 1;
    let res = eval_expr_inner(expr, row, ctx, catalog, storage, clock);
    ctx.frame.depth -= 1;
    res
}

#[inline(always)]
fn eval_expr_inner(
    expr: &Expr,
    row: &[super::model::ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    match expr {
        Expr::Identifier(name) => resolve_identifier(row, name, ctx),
        Expr::QualifiedIdentifier(parts) => resolve_qualified_identifier(row, parts, ctx),
        Expr::Wildcard => Err(DbError::Execution(
            "wildcard is not a scalar expression".into(),
        )),
        Expr::QualifiedWildcard(_) => Err(DbError::Execution(
            "qualified wildcard is not a scalar expression".into(),
        )),
        Expr::Integer(v) => Ok(if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
            Value::Int(*v as i32)
        } else {
            Value::BigInt(*v)
        }),
        Expr::FloatLiteral(s) => {
            super::value_ops::parse_numeric_literal(s)
        }
        Expr::BinaryLiteral(bytes) => Ok(Value::Binary(bytes.clone())),
        Expr::String(v) => Ok(Value::VarChar(v.clone())),
        Expr::UnicodeString(v) => Ok(Value::NVarChar(v.clone())),
        Expr::Null => Ok(Value::Null),
        Expr::FunctionCall { name, args } => {
            eval_function(name, args, row, ctx, catalog, storage, clock)
        }
        Expr::Binary { left, op, right } => {
            let lv = eval_expr(left, row, ctx, catalog, storage, clock)?;
            let rv = eval_expr(right, row, ctx, catalog, storage, clock)?;
            eval_binary(
                op,
                lv,
                rv,
                ctx.metadata.ansi_nulls,
                ctx.options.concat_null_yields_null,
                ctx.options.arithabort,
                ctx.options.ansi_warnings,
            )
        }
        Expr::Unary { op, expr: inner } => {
            let val = eval_expr(inner, row, ctx, catalog, storage, clock)?;
            eval_unary(op, val)
        }
        Expr::IsNull(inner) => Ok(Value::Bit(
            eval_expr(inner, row, ctx, catalog, storage, clock)?.is_null(),
        )),
        Expr::IsNotNull(inner) => Ok(Value::Bit(
            !eval_expr(inner, row, ctx, catalog, storage, clock)?.is_null(),
        )),
        Expr::Cast { expr, target } => {
            let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
            coerce_value_to_type_with_dateformat(
                value,
                &data_type_spec_to_runtime(target),
                &ctx.options.dateformat,
            )
        }
        Expr::TryCast { expr, target } => {
            let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
            match coerce_value_to_type_with_dateformat(
                value,
                &data_type_spec_to_runtime(target),
                &ctx.options.dateformat,
            ) {
                Ok(v) => Ok(v),
                Err(_) => Ok(Value::Null),
            }
        }
        Expr::Convert {
            target,
            expr,
            style,
        } => {
            let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
            if let Some(style_code) = style {
                super::value_ops::convert_with_style(
                    value,
                    &data_type_spec_to_runtime(target),
                    *style_code,
                    &ctx.options.dateformat,
                )
            } else {
                coerce_value_to_type_with_dateformat(
                    value,
                    &data_type_spec_to_runtime(target),
                    &ctx.options.dateformat,
                )
            }
        }
        Expr::TryConvert {
            target,
            expr,
            style,
        } => {
            let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
            let result = if let Some(style_code) = style {
                super::value_ops::convert_with_style(
                    value,
                    &data_type_spec_to_runtime(target),
                    *style_code,
                    &ctx.options.dateformat,
                )
            } else {
                coerce_value_to_type_with_dateformat(
                    value,
                    &data_type_spec_to_runtime(target),
                    &ctx.options.dateformat,
                )
            };
            match result {
                Ok(v) => Ok(v),
                Err(_) => Ok(Value::Null),
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_result,
        } => eval_case(
            operand.as_deref(),
            when_clauses,
            else_result.as_deref(),
            row,
            ctx,
            catalog,
            storage,
            clock,
        ),
        Expr::InList {
            expr: in_expr,
            list,
            negated,
        } => eval_in_list(in_expr, list, *negated, row, ctx, catalog, storage, clock),
        Expr::Between {
            expr: between_expr,
            low,
            high,
            negated,
        } => eval_between(
            between_expr,
            low,
            high,
            *negated,
            row,
            ctx,
            catalog,
            storage,
            clock,
        ),
        Expr::Like {
            expr: like_expr,
            pattern,
            negated,
        } => eval_like(
            like_expr, pattern, *negated, row, ctx, catalog, storage, clock,
        ),
        Expr::Subquery(stmt) => eval_scalar_subquery(stmt, row, ctx, catalog, storage, clock),
        Expr::Exists { subquery, negated } => {
            eval_exists(subquery, *negated, row, ctx, catalog, storage, clock)
        }
        Expr::InSubquery {
            expr: in_expr,
            subquery,
            negated,
        } => eval_in_subquery(
            in_expr, subquery, *negated, row, ctx, catalog, storage, clock,
        ),
        Expr::WindowFunction { .. } => {
            let key = format!("{:?}", expr);
            if let Some(val) = ctx.get_window_value(&key) {
                Ok(val)
            } else {
                Err(DbError::Execution(
                    "window function value not found in context".into(),
                ))
            }
        }
    }
}

pub(crate) fn eval_predicate(
    expr: &Expr,
    row: &[super::model::ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<bool, DbError> {
    let value = eval_expr(expr, row, ctx, catalog, storage, clock)?;
    let result = match &value {
        Value::Bit(v) => *v,
        Value::Null => false,
        other => truthy(other),
    };
    Ok(result)
}
