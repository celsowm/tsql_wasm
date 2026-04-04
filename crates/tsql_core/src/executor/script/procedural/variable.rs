use crate::ast::{DeclareStmt, SetStmt};
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use super::super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_declare(
        &mut self,
        stmt: DeclareStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let declared_name = stmt.name.clone();
        let ty = crate::executor::type_mapping::data_type_spec_to_runtime(&stmt.data_type);
        let value = if let Some(ref default_expr) = stmt.default {
            crate::executor::evaluator::eval_expr(
                default_expr,
                &[],
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?
        } else {
            crate::types::Value::Null
        };
        ctx.session.variables.insert(stmt.name, (ty, value));
        ctx.register_declared_var(&declared_name);
        Ok(None)
    }

    pub(crate) fn execute_declare_table_var(
        &mut self,
        stmt: crate::ast::DeclareTableVarStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let unique = ctx.next_table_var_id();
        let physical = format!(
            "__tablevar_{}_{}_{}",
            ctx.frame.depth,
            stmt.name.trim_start_matches('@'),
            unique
        );
        let create = crate::ast::CreateTableStmt {
            name: crate::ast::ObjectName {
                schema: Some("dbo".to_string()),
                name: physical.clone(),
            },
            columns: stmt.columns,
            table_constraints: stmt.table_constraints,
        };
        self.schema().create_table(create)?;
        ctx.register_table_var(&stmt.name, &physical);
        Ok(None)
    }

    pub(crate) fn execute_set(
        &mut self,
        stmt: SetStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let val = crate::executor::evaluator::eval_expr(
            &stmt.expr,
            &[],
            ctx,
            self.catalog,
            self.storage,
            self.clock,
        )?;
        if let Some((ty, var)) = ctx.session.variables.get_mut(&stmt.name) {
            let coerced = crate::executor::value_ops::coerce_value_to_type(val, ty)?;
            *var = coerced;
        } else {
            return Err(DbError::Semantic(format!(
                "variable '{}' not declared",
                stmt.name
            )));
        }
        Ok(None)
    }
}
