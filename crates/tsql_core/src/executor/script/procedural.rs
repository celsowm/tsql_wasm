use crate::ast::{SelectAssignStmt, SelectStmt, SpExecuteSqlStmt, ExecProcedureStmt};
use crate::catalog::{Catalog, RoutineKind};
use crate::error::DbError;
use crate::storage::Storage;
use crate::types::Value;

use super::super::context::ExecutionContext;
use super::super::evaluator::eval_expr;
use super::super::query::QueryExecutor;
use super::super::result::QueryResult;
use super::super::value_ops::coerce_value_to_type;
use super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_select_assign(
        &mut self,
        stmt: SelectAssignStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if stmt.targets.is_empty() {
            return Ok(None);
        }
        if stmt.from.is_none() {
            for t in stmt.targets {
                let val = eval_expr(
                    &t.expr,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                if let Some((ty, var)) = ctx.variables.get_mut(&t.variable) {
                    *var = coerce_value_to_type(val, ty)?;
                } else {
                    return Err(DbError::Semantic(format!(
                        "variable '{}' not declared",
                        t.variable
                    )));
                }
            }
            return Ok(None);
        }

        let q = SelectStmt {
            from: stmt.from,
            joins: stmt.joins,
            applies: vec![],
            projection: stmt
                .targets
                .iter()
                .map(|t| crate::ast::SelectItem {
                    expr: t.expr.clone(),
                    alias: None,
                })
                .collect(),
            distinct: false,
            top: None,
            selection: stmt.selection,
            group_by: vec![],
            having: None,
            order_by: vec![],
            offset: None,
            fetch: None,
        };
        let result = QueryExecutor {
            catalog: self.catalog as &dyn Catalog,
            storage: self.storage as &dyn Storage,
            clock: self.clock,
        }
        .execute_select(q, ctx)?;
        if let Some(last) = result.rows.last() {
            for (idx, t) in stmt.targets.iter().enumerate() {
                if let Some((ty, var)) = ctx.variables.get_mut(&t.variable) {
                    *var = coerce_value_to_type(last[idx].clone(), ty)?;
                } else {
                    return Err(DbError::Semantic(format!(
                        "variable '{}' not declared",
                        t.variable
                    )));
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn execute_procedure(
        &mut self,
        stmt: ExecProcedureStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
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
                    let val = eval_expr(
                        def,
                        &[],
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?;
                    let ty = super::super::type_mapping::data_type_spec_to_runtime(&param.data_type);
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
            let val = eval_expr(
                &arg.expr,
                &[],
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
            let ty = super::super::type_mapping::data_type_spec_to_runtime(&param.data_type);
            let coerced = coerce_value_to_type(val, &ty)?;
            ctx.variables.insert(param.name.clone(), (ty, coerced));
            ctx.register_declared_var(&param.name);
            if param.is_output && arg.is_output {
                if let crate::ast::Expr::Identifier(ref caller) = arg.expr {
                    output_bindings.push((param.name.clone(), caller.clone()));
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
            Err(DbError::Return(_)) => Ok(None),
            other => other,
        }
    }

    pub(crate) fn execute_sp_executesql(
        &mut self,
        stmt: SpExecuteSqlStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        let sql_val = eval_expr(
            &stmt.sql_expr,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;
        let sql_text = sql_val.to_string_value();

        ctx.enter_scope();
        let mut output_vars = vec![];
        for arg in stmt.args {
            let val = eval_expr(
                &arg.expr,
                &[],
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
            let pname = arg.name.unwrap_or_else(|| "".to_string());
            if pname.is_empty() {
                continue;
            }
            let key = pname.trim().to_string();
            let ty = val.data_type().unwrap_or(crate::types::DataType::Int);
            ctx.variables.insert(key.clone(), (ty, val.clone()));
            ctx.register_declared_var(&key);
            if arg.is_output {
                if let crate::ast::Expr::Identifier(ref caller_var) = arg.expr {
                    output_vars.push((key.clone(), caller_var.clone()));
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
        exec_result
    }

    fn leave_scope_and_cleanup(&mut self, ctx: &mut ExecutionContext) -> Result<(), DbError> {
        let dropped_physical = ctx.leave_scope_collect_table_vars();
        for physical in dropped_physical {
            if self.catalog.find_table("dbo", &physical).is_none() {
                continue;
            }
            crate::executor::schema::SchemaExecutor {
                catalog: self.catalog,
                storage: self.storage,
            }
            .drop_table(crate::ast::DropTableStmt {
                name: crate::ast::ObjectName {
                    schema: Some("dbo".to_string()),
                    name: physical,
                },
            })?;
        }
        Ok(())
    }
}
