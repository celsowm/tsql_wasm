mod builtin_registry;
pub(crate) mod datetime;
pub(crate) mod logic;
pub(crate) mod math;
pub(crate) mod metadata;
pub(crate) mod string;
pub(crate) mod system;
pub(crate) mod vector;
pub(crate) mod udf;

use crate::ast::Expr;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::aggregates::{dispatch_aggregate, is_aggregate_function};
use super::clock::Clock;
use super::context::ExecutionContext;
use super::model::ContextTable;
use super::string_norm::normalize_identifier;
use builtin_registry::{lookup_builtin_handler, lookup_system_variable};

pub(crate) fn eval_function(
    name: &str,
    args: &[Expr],
    row: &[ContextTable],
    ctx: &mut ExecutionContext<'_>,
    catalog: &dyn Catalog,
    storage: &dyn Storage,
    clock: &dyn Clock,
) -> Result<Value, DbError> {
    if is_aggregate_function(name) {
        if let Some(group) = ctx.current_group().clone() {
            if let Some(res) = dispatch_aggregate(name, args, &group, ctx, catalog, storage, clock)
            {
                return res;
            }
        }
    }

    let upper = normalize_identifier(name);
    let upper_str = upper.as_str();

    // Check built-in functions first
    if let Some(handler) = lookup_builtin_handler(upper_str) {
        return handler(args, row, ctx, catalog, storage, clock);
    }

    // Check system variables (@@VAR)
    if let Some(result) = lookup_system_variable(upper_str, ctx) {
        return result;
    }

    // Handle aggregates not in grouped context
    match upper_str {
        "COUNT" | "SUM" | "AVG" | "COUNT_BIG" => Err(DbError::Execution(format!(
            "{} is only supported in grouped projection",
            name
        ))),
        "MIN" | "MAX" => Err(DbError::Execution(
            "MIN/MAX require a FROM clause when used in scalar context (use in GROUP BY)".into(),
        )),
        _ => udf::eval_user_scalar_function(name, args, row, ctx, catalog, storage, clock),
    }
}
