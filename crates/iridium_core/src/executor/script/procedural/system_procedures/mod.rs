use super::super::ScriptExecutor;
use crate::ast::ExecProcedureStmt;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::result::QueryResult;
use crate::types::{DataType, Value};
pub mod metadata;
pub mod session;
pub mod security;

use metadata::*;
use session::*;
use security::*;

const SYSTEM_PROCEDURES: &[&str] = &[
    "sp_rename",
    "sp_help",
    "sp_helptext",
    "sp_columns",
    "sp_tables",
    "sp_helpindex",
    "sp_helpconstraint",
    "sp_set_session_context",
    "xp_instance_regread",
    "sp_msgetversion",
    "sp_who",
    "sp_databases",
    "sp_helpdb",
    "sp_server_info",
    "sp_monitor",
    "sp_helpuser",
    "sp_helprole",
    "sp_helprolemember",
    "sp_helpsrvrole",
    "sp_helpsrvrolemember",
    "sp_helpfile",
    "sp_helpfilegroup",
];

pub(crate) fn is_system_procedure(name: &str) -> bool {
    SYSTEM_PROCEDURES
        .iter()
        .any(|sp| name.eq_ignore_ascii_case(sp))
}

pub(crate) fn execute_system_procedure(
    exec: &mut ScriptExecutor<'_>,
    stmt: &ExecProcedureStmt,
    ctx: &mut ExecutionContext<'_>,
) -> Result<Option<QueryResult>, DbError> {
    let name = &stmt.name.name;
    let args = eval_args(exec, &stmt.args, ctx)?;

    let result = if name.eq_ignore_ascii_case("sp_rename") {
        execute_sp_rename(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_help") {
        execute_sp_help(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_helptext") {
        execute_sp_helptext(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_columns") {
        execute_sp_columns(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_tables") {
        execute_sp_tables(exec)?
    } else if name.eq_ignore_ascii_case("sp_helpindex") {
        execute_sp_helpindex(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_helpconstraint") {
        execute_sp_helpconstraint(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_set_session_context") {
        execute_sp_set_session_context(stmt, ctx, exec)?
    } else if name.eq_ignore_ascii_case("sp_who") {
        execute_sp_who(ctx)?
    } else if name.eq_ignore_ascii_case("sp_databases") {
        execute_sp_databases()?
    } else if name.eq_ignore_ascii_case("sp_helpdb") {
        execute_sp_helpdb(exec, &args)?
    } else if name.eq_ignore_ascii_case("sp_server_info") {
        execute_sp_server_info()?
    } else if name.eq_ignore_ascii_case("sp_monitor") {
        execute_sp_monitor(exec)?
    } else if name.eq_ignore_ascii_case("sp_helpuser") {
        execute_sp_helpuser(exec, ctx)?
    } else if name.eq_ignore_ascii_case("sp_helprole") {
        execute_sp_helprole(exec, ctx)?
    } else if name.eq_ignore_ascii_case("sp_helprolemember") {
        execute_sp_helprolemember(exec, ctx)?
    } else if name.eq_ignore_ascii_case("sp_helpsrvrole") {
        execute_sp_helpsrvrole(exec, ctx)?
    } else if name.eq_ignore_ascii_case("sp_helpsrvrolemember") {
        execute_sp_helpsrvrolemember(exec, ctx)?
    } else if name.eq_ignore_ascii_case("sp_helpfile") {
        execute_sp_helpfile(exec, ctx)?
    } else if name.eq_ignore_ascii_case("sp_helpfilegroup") {
        execute_sp_helpfilegroup(exec, ctx)?
    } else if name.eq_ignore_ascii_case("xp_instance_regread") {
        // Stub for registry reads. If it has an output parameter, set it to a default.
        for arg in &stmt.args {
            if arg.is_output {
                if let crate::ast::Expr::Identifier(ref var_name) = arg.expr {
                    if let Some((ty, val)) = ctx.session.variables.get_mut(var_name) {
                        *val = crate::executor::value_ops::coerce_value_to_type_with_dateformat(
                            Value::Int(0),
                            ty,
                            &ctx.options.dateformat,
                        )?;
                    }
                }
            }
        }
        QueryResult::default()
    } else if name.eq_ignore_ascii_case("sp_msgetversion") {
        // Stub for version check
        QueryResult {
            columns: vec!["Character_Value".into()],
            column_types: vec![DataType::NVarChar { max_len: 128 }],
            column_nullabilities: vec![false],
            rows: vec![vec![Value::NVarChar("16.0.1000.0".into())]],
            ..Default::default()
        }
    } else {
        return Err(DbError::Execution(format!(
            "unknown system procedure '{}'",
            name
        )));
    };

    let mut res = result;
    res.return_status = Some(0);
    res.is_procedure = true;
    Ok(Some(res))
}

fn eval_args(
    exec: &mut ScriptExecutor<'_>,
    args: &[crate::ast::ExecArgument],
    ctx: &mut ExecutionContext<'_>,
) -> Result<Vec<String>, DbError> {
    let mut result = Vec::new();
    for arg in args {
        let val = eval_expr(&arg.expr, &[], ctx, exec.catalog, exec.storage, exec.clock)?;
        result.push(val.to_string_value());
    }
    Ok(result)
}
