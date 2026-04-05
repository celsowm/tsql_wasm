use crate::ast::Expr;
use crate::catalog::{Catalog, RoutineKind};
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::common::{
    eval_expr_to_value, resolve_object, table_has_check_constraint, table_has_default_constraint,
    table_has_foreign_key, table_has_identity, table_has_index, table_has_primary_key,
    table_has_unique_constraint, value_to_object_id,
};
use crate::executor::clock::Clock;
use crate::executor::context::ExecutionContext;
use crate::executor::model::ContextTable;

pub(crate) fn eval_objectproperty(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    eval_objectproperty_common(args, row, ctx, catalog, storage, clock)
}

pub(crate) fn eval_objectpropertyex(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    eval_objectproperty_common(args, row, ctx, catalog, storage, clock)
}

fn eval_objectproperty_common(
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "OBJECTPROPERTY expects 2 arguments".into(),
        ));
    }
    let object_val = eval_expr_to_value(&args[0], row, ctx, catalog, storage, clock)?;
    let prop_val = eval_expr_to_value(&args[1], row, ctx, catalog, storage, clock)?;
    if object_val.is_null() || prop_val.is_null() {
        return Ok(Value::Null);
    }
    let Some(object_id) = value_to_object_id(&object_val, catalog, None) else {
        return Ok(Value::Null);
    };
    let Some(object) = resolve_object(catalog, object_id) else {
        return Ok(Value::Null);
    };
    let prop = prop_val.to_string_value().to_ascii_uppercase();
    Ok(match object {
        super::common::ResolvedObject::Table(table) => match prop.as_str() {
            "ISTABLE" | "ISUSERTABLE" => Value::Int(1),
            "ISVIEW" | "ISPROCEDURE" | "ISTRIGGER" | "ISSCALARFUNCTION" | "ISTABLEFUNCTION" => {
                Value::Int(0)
            }
            "TABLEHASPRIMARYKEY" => Value::Int(if table_has_primary_key(table) { 1 } else { 0 }),
            "TABLEHASIDENTITY" => Value::Int(if table_has_identity(table) { 1 } else { 0 }),
            "TABLEHASINDEX" => Value::Int(if table_has_index(catalog, table) {
                1
            } else {
                0
            }),
            "TABLEHASFOREIGNKEY" => Value::Int(if table_has_foreign_key(table) { 1 } else { 0 }),
            "TABLEHASDEFAULTCNST" => Value::Int(if table_has_default_constraint(table) {
                1
            } else {
                0
            }),
            "TABLEHASCHECKCNST" => Value::Int(if table_has_check_constraint(table) {
                1
            } else {
                0
            }),
            "TABLEHASUNIQUECNST" => Value::Int(if table_has_unique_constraint(table) {
                1
            } else {
                0
            }),
            _ => Value::Null,
        },
        super::common::ResolvedObject::Routine(routine) => match prop.as_str() {
            "ISPROCEDURE" => {
                Value::Int(matches!(&routine.kind, RoutineKind::Procedure { .. }) as i32)
            }
            "ISSCALARFUNCTION" => Value::Int(
                (matches!(
                    &routine.kind,
                    RoutineKind::Function {
                        body: crate::ast::FunctionBody::ScalarReturn(_),
                        ..
                    }
                ) || matches!(
                    &routine.kind,
                    RoutineKind::Function {
                        body: crate::ast::FunctionBody::Scalar(_),
                        ..
                    }
                )) as i32,
            ),
            "ISTABLEFUNCTION" => Value::Int(matches!(
                &routine.kind,
                RoutineKind::Function {
                    body: crate::ast::FunctionBody::InlineTable(_),
                    ..
                }
            ) as i32),
            "ISINLINEFUNCTION" => Value::Int(matches!(
                &routine.kind,
                RoutineKind::Function {
                    body: crate::ast::FunctionBody::InlineTable(_),
                    ..
                }
            ) as i32),
            "EXECISANSINULLSON" => Value::Int(1),
            "EXECISQUOTEDIDENTON" => Value::Int(1),
            _ => Value::Null,
        },
        super::common::ResolvedObject::View => match prop.as_str() {
            "ISVIEW" => Value::Int(1),
            "ISTABLE" | "ISUSERTABLE" | "ISPROCEDURE" | "ISTRIGGER" => Value::Int(0),
            "ISSCHEMABOUND" => Value::Int(0),
            _ => Value::Null,
        },
        super::common::ResolvedObject::Trigger => match prop.as_str() {
            "ISTRIGGER" => Value::Int(1),
            "ISVIEW" | "ISTABLE" | "ISPROCEDURE" => Value::Int(0),
            _ => Value::Null,
        },
    })
}
