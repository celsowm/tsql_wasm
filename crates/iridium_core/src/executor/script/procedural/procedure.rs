use super::super::ScriptExecutor;
use super::routine::{execute_xp_msver, procedure_return_value};
use super::shared::{resolve_table_identifier, validate_table_matches_type};
use crate::ast::{ExecProcedureStmt, RoutineParamType};
use crate::catalog::RoutineKind;
use crate::error::DbError;
use crate::executor::context::{ExecutionContext, ModuleFrame, ModuleKind};
use crate::executor::evaluator::eval_expr;
use crate::executor::result::QueryResult;
use crate::executor::value_ops::coerce_value_to_type_with_dateformat;
use crate::types::Value;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_procedure(
        &mut self,
        stmt: ExecProcedureStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<crate::executor::result::QueryResult>, DbError> {
        if stmt.name.name.eq_ignore_ascii_case("xp_msver") {
            self.assign_exec_return_value(&stmt.return_variable, Value::Int(0), ctx)?;
            let mut res = execute_xp_msver();
            res.return_status = Some(0);
            res.is_procedure = true;
            return Ok(Some(res));
        }
        if stmt.name.name.eq_ignore_ascii_case("xp_qv") {
            let (mut result, return_code) = self.execute_xp_qv(&stmt.args, ctx)?;
            self.assign_exec_return_value(&stmt.return_variable, Value::Int(return_code), ctx)?;
            if let Some(res) = &mut result {
                res.return_status = Some(return_code);
                res.is_procedure = true;
            } else {
                result = Some(QueryResult {
                    return_status: Some(return_code),
                    is_procedure: true,
                    ..Default::default()
                });
            }
            return Ok(result);
        }
        if stmt
            .name
            .name
            .eq_ignore_ascii_case("sp_MSIsContainedAGSession")
        {
            self.assign_exec_return_value(&stmt.return_variable, Value::Int(0), ctx)?;
            return Ok(None);
        }
        if stmt.name.name.eq_ignore_ascii_case("sp_set_session_context") {
            let return_code = self.execute_sp_set_session_context(&stmt.args, ctx)?;
            self.assign_exec_return_value(&stmt.return_variable, Value::Int(return_code), ctx)?;
            return Ok(Some(QueryResult {
                return_status: Some(return_code),
                is_procedure: true,
                ..Default::default()
            }));
        }

        let schema = stmt.name.schema_or_dbo().to_string();
        let Some(routine) = self.catalog.find_routine(&schema, &stmt.name.name).cloned() else {
            return Err(DbError::object_not_found(format!(
                "procedure '{}.{}'",
                schema, stmt.name.name
            )));
        };
        let crate::catalog::RoutineDef {
            object_id,
            schema: routine_schema,
            name: routine_name,
            params,
            kind,
            ..
        } = routine;
        let RoutineKind::Procedure { body } = kind else {
            return Err(DbError::object_not_found(format!(
                "'{}.{}' is not a procedure",
                schema, stmt.name.name
            )));
        };
        ctx.push_module(ModuleFrame {
            object_id,
            schema: routine_schema.clone(),
            name: routine_name.clone(),
            kind: ModuleKind::Procedure,
        });
        let scope_depth = ctx.frame.scope_vars.len();
        let result = (|| {
            ctx.enter_scope();
            let mut output_bindings: Vec<(String, String)> = vec![];
            for (idx, param) in params.iter().enumerate() {
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
                        let coerced = coerce_value_to_type_with_dateformat(
                            val,
                            &ty,
                            &ctx.options.dateformat,
                        )?;
                        ctx.session
                            .variables
                            .insert(param.name.clone(), (ty, coerced));
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
                        let coerced = coerce_value_to_type_with_dateformat(
                            val,
                            &ty,
                            &ctx.options.dateformat,
                        )?;
                        ctx.session
                            .variables
                            .insert(param.name.clone(), (ty, coerced));
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
                if let Some((_, v)) = ctx.session.variables.get(inner_name) {
                    out_values.push((caller_var.clone(), v.clone()));
                }
            }
            self.leave_scope_and_cleanup(ctx)?;
            for (caller_var, val) in out_values {
                if let Some((ty, out_var)) = ctx.session.variables.get_mut(&caller_var) {
                    *out_var =
                        coerce_value_to_type_with_dateformat(val, ty, &ctx.options.dateformat)?;
                }
            }

            let (result, return_value) = match proc_result {
                Ok(crate::error::StmtOutcome::Return(v)) => (None, procedure_return_value(v)),
                Ok(crate::error::StmtOutcome::Break) | Ok(crate::error::StmtOutcome::Continue) => {
                    (None, Value::Int(0))
                }
                Ok(crate::error::StmtOutcome::Ok(r)) => (r, Value::Int(0)),
                Err(e) => return Err(e),
            };
            self.assign_exec_return_value(&stmt.return_variable, return_value.clone(), ctx)?;
            let mut final_result = result;
            if let Some(res) = &mut final_result {
                res.return_status = Some(return_value.to_integer_i64().unwrap_or(0) as i32);
                res.is_procedure = true;
            } else {
                final_result = Some(QueryResult {
                    return_status: Some(return_value.to_integer_i64().unwrap_or(0) as i32),
                    is_procedure: true,
                    ..Default::default()
                });
            }
            Ok(final_result)
        })();
        while ctx.frame.scope_vars.len() > scope_depth {
            ctx.leave_scope();
        }
        ctx.pop_module();
        result
    }

    fn assign_exec_return_value(
        &mut self,
        return_variable: &Option<String>,
        value: Value,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<(), DbError> {
        let Some(return_variable) = return_variable else {
            return Ok(());
        };
        if let Some((ty, out_var)) = ctx.session.variables.get_mut(return_variable) {
            *out_var = coerce_value_to_type_with_dateformat(value, ty, &ctx.options.dateformat)?;
            return Ok(());
        }
        Err(DbError::invalid_identifier(return_variable))
    }

    fn execute_xp_qv(
        &mut self,
        args: &[crate::ast::ExecArgument],
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<(Option<QueryResult>, i32), DbError> {
        let query_id = match args.first() {
            Some(arg) => eval_expr(&arg.expr, &[], ctx, self.catalog, self.storage, self.clock)?
                .to_string_value(),
            None => {
                return Err(DbError::Execution(
                    "xp_qv requires at least one query identifier argument".into(),
                ))
            }
        };

        match query_id.as_str() {
            // SSMS Object Explorer AlwaysOn probe:
            // DECLARE @alwayson INT; EXEC @alwayson = master.dbo.xp_qv N'3641190370', @@SERVICENAME;
            // SELECT ISNULL(@alwayson, -1) AS [AlwaysOn]
            "3641190370" => Ok((None, -1)),
            _ => Ok((None, -1)),
        }
    }

    fn execute_sp_set_session_context(
        &mut self,
        args: &[crate::ast::ExecArgument],
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<i32, DbError> {
        let key = match args.get(0) {
            Some(arg) => eval_expr(&arg.expr, &[], ctx, self.catalog, self.storage, self.clock)?
                .to_string_value(),
            None => {
                return Err(DbError::Execution(
                    "sp_set_session_context requires at least a key argument".into(),
                ))
            }
        };

        let value = match args.get(1) {
            Some(arg) => eval_expr(&arg.expr, &[], ctx, self.catalog, self.storage, self.clock)?,
            None => {
                return Err(DbError::Execution(
                    "sp_set_session_context requires a value argument".into(),
                ))
            }
        };

        let read_only = match args.get(2) {
            Some(arg) => eval_expr(&arg.expr, &[], ctx, self.catalog, self.storage, self.clock)?
                .to_bool()
                .unwrap_or(false),
            None => false,
        };

        if let Some((_, is_read_only)) = ctx.session.session_context.get(&key) {
            if *is_read_only {
                return Err(DbError::Execution(format!(
                    "Cannot set session context for key '{}' because it is read-only.",
                    key
                )));
            }
        }

        ctx.session.session_context.insert(key, (value, read_only));

        Ok(0)
    }
}
