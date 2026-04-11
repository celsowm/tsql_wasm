use super::super::ScriptExecutor;
use super::shared::{find_param_def, resolve_table_identifier, validate_table_matches_type};
use crate::ast::{RoutineParamType, SpExecuteSqlStmt};
use crate::error::{DbError, StmtOutcome};
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::result::QueryResult;
use crate::executor::value_ops::coerce_value_to_type_with_dateformat;

pub(crate) fn execute_sp_executesql(
    exec: &mut ScriptExecutor<'_>,
    stmt: SpExecuteSqlStmt,
    ctx: &mut ExecutionContext<'_>,
) -> Result<Option<QueryResult>, DbError> {
    let sql_val = eval_expr(
        &stmt.sql_expr,
        &[],
        ctx,
        exec.catalog,
        exec.storage,
        exec.clock,
    )?;
    let sql_text = sql_val.to_string_value();
    let declared_params = if let Some(def_expr) = &stmt.params_def {
        let def_text = eval_expr(def_expr, &[], ctx, exec.catalog, exec.storage, exec.clock)?
            .to_string_value();
        crate::parser::statements::procedural::parse_routine_params(&def_text)?
    } else {
        vec![]
    };

    let scope_depth = ctx.frame.scope_vars.len();
    let result = (|| {
        ctx.enter_scope();
        let mut output_vars = vec![];
        for (idx, arg) in stmt.args.into_iter().enumerate() {
            let pname = arg
                .name
                .clone()
                .or_else(|| declared_params.get(idx).map(|p| p.name.clone()))
                .unwrap_or_else(|| "".to_string());
            if pname.trim().is_empty() {
                continue;
            }
            let key = pname.trim().to_string();
            match find_param_def(&declared_params, &key) {
                Some(def) => match &def.param_type {
                    RoutineParamType::Scalar(dt) => {
                        let val = eval_expr(&arg.expr, &[], ctx, exec.catalog, exec.storage, exec.clock)?;
                        let ty = crate::executor::type_mapping::data_type_spec_to_runtime(dt);
                        let coerced = coerce_value_to_type_with_dateformat(
                            val,
                            &ty,
                            &ctx.options.dateformat,
                        )?;
                        ctx.session.variables.insert(key.clone(), (ty, coerced));
                        ctx.register_declared_var(&key);
                        if arg.is_output {
                            if let crate::ast::Expr::Identifier(ref caller_var) = arg.expr {
                                output_vars.push((key.clone(), caller_var.clone()));
                            }
                        }
                    }
                    RoutineParamType::TableType(type_name) => {
                        let logical = resolve_table_identifier(&arg.expr)?.to_string();
                        let Some(physical) = ctx.resolve_table_name(&logical) else {
                            return Err(DbError::Execution(format!(
                                "TVP argument '{}' is not a table variable",
                                logical
                            )));
                        };
                        let table = exec.catalog.find_table("dbo", &physical).ok_or_else(|| {
                            DbError::Execution(format!("table '{}' not found", physical))
                        })?;
                        let tdef = exec
                            .catalog
                            .find_table_type(type_name.schema_or_dbo(), &type_name.name)
                            .ok_or_else(|| {
                                DbError::Execution(format!(
                                    "table type '{}.{}' not found",
                                    type_name.schema_or_dbo(),
                                    type_name.name
                                ))
                            })?;
                        validate_table_matches_type(exec.catalog, table, tdef)?;
                        ctx.register_table_var(&key, &physical);
                        ctx.mark_table_var_readonly(&key);
                    }
                },
                None => {
                    let val = eval_expr(&arg.expr, &[], ctx, exec.catalog, exec.storage, exec.clock)?;
                    let ty = val.data_type().unwrap_or(crate::types::DataType::Int);
                    ctx.session.variables.insert(key.clone(), (ty, val.clone()));
                    ctx.register_declared_var(&key);
                    if arg.is_output {
                        if let crate::ast::Expr::Identifier(ref caller_var) = arg.expr {
                            output_vars.push((key.clone(), caller_var.clone()));
                        }
                    }
                }
            }
        }
        let batch = crate::parser::parse_batch(&sql_text)?;
        let exec_result = exec.execute_batch(&batch, ctx);

        let mut outs = vec![];
        for (inner, outer) in output_vars {
            if let Some((_, v)) = ctx.session.variables.get(&inner) {
                outs.push((outer, v.clone()));
            }
        }
        exec.leave_scope_and_cleanup(ctx)?;
        for (outer, val) in outs {
            if let Some((ty, out)) = ctx.session.variables.get_mut(&outer) {
                *out = coerce_value_to_type_with_dateformat(
                    val,
                    ty,
                    &ctx.options.dateformat,
                )?;
            }
        }
        // Swallow control flow signals at procedure boundary
        match exec_result {
            Ok(StmtOutcome::Return(_)) | Ok(StmtOutcome::Break) | Ok(StmtOutcome::Continue) => Ok(None),
            Ok(StmtOutcome::Ok(r)) => Ok(r),
            Err(e) => Err(e),
        }
    })();
    while ctx.frame.scope_vars.len() > scope_depth {
        ctx.leave_scope();
    }
    result
}
