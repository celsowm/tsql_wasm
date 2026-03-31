use super::super::ScriptExecutor;
use crate::ast::{ExecProcedureStmt, RoutineParamType, SpExecuteSqlStmt};
use crate::catalog::{RoutineKind, TableTypeDef};
use crate::error::{DbError, StmtOutcome};
use crate::executor::context::ExecutionContext;
use crate::executor::evaluator::eval_expr;
use crate::executor::value_ops::coerce_value_to_type;
use crate::types::Value;

fn find_param_def<'a>(
    params: &'a [crate::ast::RoutineParam],
    name: &str,
) -> Option<&'a crate::ast::RoutineParam> {
    params.iter().find(|p| p.name.eq_ignore_ascii_case(name))
}

fn resolve_table_identifier(expr: &crate::ast::Expr) -> Result<&str, DbError> {
    match expr {
        crate::ast::Expr::Identifier(name) => Ok(name.as_str()),
        _ => Err(DbError::Execution(
            "table-valued parameter arguments must be table variables or temp-table identifiers"
                .into(),
        )),
    }
}

fn validate_table_matches_type(
    catalog: &dyn crate::catalog::Catalog,
    table: &crate::catalog::TableDef,
    tdef: &TableTypeDef,
) -> Result<(), DbError> {
    if table.columns.len() != tdef.columns.len() {
        return Err(DbError::Execution(format!(
            "TVP type mismatch for '{}.{}': expected {} columns, got {}",
            tdef.schema,
            tdef.name,
            tdef.columns.len(),
            table.columns.len()
        )));
    }
    for (idx, (actual, expected)) in table.columns.iter().zip(tdef.columns.iter()).enumerate() {
        let expected_ty =
            crate::executor::type_mapping::data_type_spec_to_runtime(&expected.data_type);
        if actual.data_type != expected_ty {
            return Err(DbError::Execution(format!(
                "TVP type mismatch at column {} ('{}'): expected {:?}, got {:?}",
                idx + 1,
                expected.name,
                expected_ty,
                actual.data_type
            )));
        }
    }
    let _ = catalog;
    Ok(())
}

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_procedure(
        &mut self,
        stmt: ExecProcedureStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<crate::executor::result::QueryResult>, DbError> {
        let schema = stmt.name.schema_or_dbo().to_string();
        let Some(routine) = self.catalog.find_routine(&schema, &stmt.name.name).cloned() else {
            return Err(DbError::Semantic(format!(
                "procedure '{}.{}' not found",
                schema, stmt.name.name
            )));
        };
        let RoutineKind::Procedure { body } = routine.kind else {
            return Err(DbError::Semantic(format!(
                "'{}.{}' is not a procedure",
                schema, stmt.name.name
            )));
        };

        ctx.enter_scope();
        let mut output_bindings: Vec<(String, String)> = vec![];
        for (idx, param) in routine.params.iter().enumerate() {
            let arg = stmt.args.get(idx);
            let Some(arg) = arg else {
                if let Some(def) = &param.default {
                    let RoutineParamType::Scalar(dt) = &param.param_type else {
                        return Err(DbError::Execution(format!(
                            "missing argument for table-valued parameter '{}'",
                            param.name
                        )));
                    };
                    let val = eval_expr(def, &[], ctx, self.catalog, self.storage, self.clock)?;
                    let ty = crate::executor::type_mapping::data_type_spec_to_runtime(dt);
                    let coerced = coerce_value_to_type(val, &ty)?;
                    ctx.variables.insert(param.name.clone(), (ty, coerced));
                    ctx.register_declared_var(&param.name);
                    continue;
                }
                return Err(DbError::Execution(format!(
                    "missing argument for parameter '{}'",
                    param.name
                )));
            };
            match &param.param_type {
                RoutineParamType::Scalar(dt) => {
                    let val =
                        eval_expr(&arg.expr, &[], ctx, self.catalog, self.storage, self.clock)?;
                    let ty = crate::executor::type_mapping::data_type_spec_to_runtime(dt);
                    let coerced = coerce_value_to_type(val, &ty)?;
                    ctx.variables.insert(param.name.clone(), (ty, coerced));
                    ctx.register_declared_var(&param.name);
                    if param.is_output && arg.is_output {
                        if let crate::ast::Expr::Identifier(ref caller) = arg.expr {
                            output_bindings.push((param.name.clone(), caller.clone()));
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
                    let table = self.catalog.find_table("dbo", &physical).ok_or_else(|| {
                        DbError::Execution(format!("table '{}' not found", physical))
                    })?;
                    let tdef = self
                        .catalog
                        .find_table_type(type_name.schema_or_dbo(), &type_name.name)
                        .ok_or_else(|| {
                            DbError::Execution(format!(
                                "table type '{}.{}' not found",
                                type_name.schema_or_dbo(),
                                type_name.name
                            ))
                        })?;
                    validate_table_matches_type(self.catalog, table, tdef)?;
                    ctx.register_table_var(&param.name, &physical);
                    if param.is_readonly {
                        ctx.mark_table_var_readonly(&param.name);
                    }
                }
            }
        }

        let proc_result = self.execute_batch(&body, ctx);
        let mut out_values: Vec<(String, Value)> = vec![];
        for (inner_name, caller_var) in &output_bindings {
            if let Some((_, v)) = ctx.variables.get(inner_name) {
                out_values.push((caller_var.clone(), v.clone()));
            }
        }
        self.leave_scope_and_cleanup(ctx)?;
        for (caller_var, val) in out_values {
            if let Some((ty, out_var)) = ctx.variables.get_mut(&caller_var) {
                *out_var = coerce_value_to_type(val, ty)?;
            }
        }

        match proc_result {
            Ok(StmtOutcome::Return(_)) | Ok(StmtOutcome::Break) | Ok(StmtOutcome::Continue) => Ok(None),
            Ok(StmtOutcome::Ok(r)) => Ok(r),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn execute_sp_executesql(
        &mut self,
        stmt: SpExecuteSqlStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<crate::executor::result::QueryResult>, DbError> {
        let sql_val = eval_expr(
            &stmt.sql_expr,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;
        let sql_text = sql_val.to_string_value();
        let declared_params = if let Some(def_expr) = &stmt.params_def {
            let def_text = eval_expr(def_expr, &[], ctx, self.catalog, self.storage, self.clock)?
                .to_string_value();
            crate::parser::statements::procedural::parse_routine_params(&def_text)?
        } else {
            vec![]
        };

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
                        let val =
                            eval_expr(&arg.expr, &[], ctx, self.catalog, self.storage, self.clock)?;
                        let ty = crate::executor::type_mapping::data_type_spec_to_runtime(dt);
                        let coerced = coerce_value_to_type(val, &ty)?;
                        ctx.variables.insert(key.clone(), (ty, coerced));
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
                        let table = self.catalog.find_table("dbo", &physical).ok_or_else(|| {
                            DbError::Execution(format!("table '{}' not found", physical))
                        })?;
                        let tdef = self
                            .catalog
                            .find_table_type(type_name.schema_or_dbo(), &type_name.name)
                            .ok_or_else(|| {
                                DbError::Execution(format!(
                                    "table type '{}.{}' not found",
                                    type_name.schema_or_dbo(),
                                    type_name.name
                                ))
                            })?;
                        validate_table_matches_type(self.catalog, table, tdef)?;
                        ctx.register_table_var(&key, &physical);
                        ctx.mark_table_var_readonly(&key);
                    }
                },
                None => {
                    let val =
                        eval_expr(&arg.expr, &[], ctx, self.catalog, self.storage, self.clock)?;
                    let ty = val.data_type().unwrap_or(crate::types::DataType::Int);
                    ctx.variables.insert(key.clone(), (ty, val.clone()));
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
        let exec_result = self.execute_batch(&batch, ctx);

        let mut outs = vec![];
        for (inner, outer) in output_vars {
            if let Some((_, v)) = ctx.variables.get(&inner) {
                outs.push((outer, v.clone()));
            }
        }
        self.leave_scope_and_cleanup(ctx)?;
        for (outer, val) in outs {
            if let Some((ty, out)) = ctx.variables.get_mut(&outer) {
                *out = coerce_value_to_type(val, ty)?;
            }
        }
        // Swallow control flow signals at procedure boundary
        match exec_result {
            Ok(StmtOutcome::Return(_)) | Ok(StmtOutcome::Break) | Ok(StmtOutcome::Continue) => Ok(None),
            Ok(StmtOutcome::Ok(r)) => Ok(r),
            Err(e) => Err(e),
        }
    }
}
