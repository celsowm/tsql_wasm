use super::clock::Clock;
use super::context::ExecutionContext;
use super::engine;
use super::mutation::MutationExecutor;
use super::query::QueryExecutor;
use super::result::QueryResult;
use super::schema::SchemaExecutor;
use super::value_ops::coerce_value_to_type;
use crate::ast::{DropTableStmt, ObjectName, Statement};
use crate::catalog::{Catalog, RoutineDef, RoutineKind};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};
use crate::types::Value;

pub struct ScriptExecutor<'a> {
    pub catalog: &'a mut dyn Catalog,
    pub storage: &'a mut dyn Storage,
    pub clock: &'a dyn Clock,
}

impl<'a> ScriptExecutor<'a> {
    pub fn execute(
        &mut self,
        stmt: Statement,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        match stmt {
            Statement::BeginTransaction(_)
            | Statement::CommitTransaction
            | Statement::RollbackTransaction(_)
            | Statement::SaveTransaction(_)
            | Statement::SetTransactionIsolationLevel(_) => Err(DbError::Execution(
                "transaction control statements are only supported at top-level execution".into(),
            )),
            Statement::CreateTable(mut stmt) => {
                if stmt.name.name.starts_with('#') {
                    let logical = stmt.name.name.clone();
                    let physical = format!("__temp_{}", logical.trim_start_matches('#'));
                    ctx.temp_table_map
                        .insert(logical.to_uppercase(), physical.clone());
                    stmt.name.schema = Some("dbo".to_string());
                    stmt.name.name = physical;
                }
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_table(stmt)?;
                Ok(None)
            }
            Statement::DropTable(mut stmt) => {
                if stmt.name.name.starts_with('#') {
                    let key = stmt.name.name.to_uppercase();
                    if let Some(mapped) = ctx.temp_table_map.remove(&key) {
                        stmt.name.schema = Some("dbo".to_string());
                        stmt.name.name = mapped;
                    }
                } else if stmt.name.name.starts_with('@') {
                    if let Some(mapped) = ctx.resolve_table_name(&stmt.name.name) {
                        stmt.name.schema = Some("dbo".to_string());
                        stmt.name.name = mapped;
                    }
                }
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .drop_table(stmt)?;
                Ok(None)
            }
            Statement::CreateIndex(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_index(stmt)?;
                Ok(None)
            }
            Statement::DropIndex(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .drop_index(stmt)?;
                Ok(None)
            }
            Statement::CreateSchema(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_schema(stmt)?;
                Ok(None)
            }
            Statement::DropSchema(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .drop_schema(stmt)?;
                Ok(None)
            }
            Statement::Insert(stmt) => {
                MutationExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                }
                .execute_insert_with_context(stmt, ctx)?;
                Ok(None)
            }
            Statement::Select(stmt) => QueryExecutor {
                catalog: self.catalog as &dyn Catalog,
                storage: self.storage as &dyn Storage,
                clock: self.clock,
            }
            .execute_select(stmt, ctx)
            .map(Some),
            Statement::Update(stmt) => {
                MutationExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                }
                .execute_update_with_context(stmt, ctx)?;
                Ok(None)
            }
            Statement::Delete(stmt) => {
                MutationExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                    clock: self.clock,
                }
                .execute_delete_with_context(stmt, ctx)?;
                Ok(None)
            }
            Statement::TruncateTable(mut stmt) => {
                if let Some(mapped) = ctx.resolve_table_name(&stmt.name.name) {
                    stmt.name.name = mapped;
                    if stmt.name.schema.is_none() {
                        stmt.name.schema = Some("dbo".to_string());
                    }
                }
                let schema = stmt.name.schema_or_dbo();
                let table_name = &stmt.name.name;
                let table = self
                    .catalog
                    .find_table(schema, table_name)
                    .ok_or_else(|| {
                        DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
                    })?
                    .clone();
                self.storage.clear_table(table.id)?;
                Ok(None)
            }
            Statement::AlterTable(mut stmt) => {
                if let Some(mapped) = ctx.resolve_table_name(&stmt.table.name) {
                    stmt.table.name = mapped;
                    if stmt.table.schema.is_none() {
                        stmt.table.schema = Some("dbo".to_string());
                    }
                }
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .alter_table(stmt)?;
                Ok(None)
            }
            Statement::Declare(stmt) => {
                let declared_name = stmt.name.clone();
                let ty = super::type_mapping::data_type_spec_to_runtime(&stmt.data_type);
                let value = if let Some(ref default_expr) = stmt.default {
                    super::evaluator::eval_expr(
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
                ctx.variables.insert(stmt.name, (ty, value));
                ctx.register_declared_var(&declared_name);
                Ok(None)
            }
            Statement::DeclareTableVar(stmt) => {
                let unique = ctx.next_table_var_id();
                let physical = format!(
                    "__tablevar_{}_{}_{}",
                    ctx.depth,
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
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_table(create)?;
                ctx.register_table_var(&stmt.name, &physical);
                Ok(None)
            }
            Statement::Set(stmt) => {
                let val = super::evaluator::eval_expr(
                    &stmt.expr,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                if let Some((ty, var)) = ctx.variables.get_mut(&stmt.name) {
                    let coerced = coerce_value_to_type(val, ty)?;
                    *var = coerced;
                } else {
                    return Err(DbError::Semantic(format!(
                        "variable '{}' not declared",
                        stmt.name
                    )));
                }
                Ok(None)
            }
            Statement::If(stmt) => {
                let cond = super::evaluator::eval_expr(
                    &stmt.condition,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                let truthy = super::value_ops::truthy(&cond);
                if truthy {
                    self.execute_batch(&stmt.then_body, ctx)
                } else if let Some(ref else_body) = stmt.else_body {
                    self.execute_batch(else_body, ctx)
                } else {
                    Ok(None)
                }
            }
            Statement::BeginEnd(stmts) => self.execute_batch(&stmts, ctx),
            Statement::While(stmt) => {
                ctx.loop_depth += 1;
                let loop_result = (|| {
                    let mut last_batch: Result<Option<QueryResult>, DbError> = Ok(None);
                    loop {
                        let cond = super::evaluator::eval_expr(
                            &stmt.condition,
                            &[],
                            ctx,
                            self.catalog,
                            self.storage,
                            self.clock,
                        )?;
                        if !super::value_ops::truthy(&cond) {
                            break;
                        }

                        match self.execute_batch(&stmt.body, ctx) {
                            Err(DbError::Break) => {
                                last_batch = Ok(None);
                                break;
                            }
                            Err(DbError::Continue) => {
                                last_batch = Ok(None);
                                continue;
                            }
                            Err(DbError::Return(v)) => return Err(DbError::Return(v)),
                            other => {
                                last_batch = other;
                            }
                        }
                    }
                    last_batch
                })();
                ctx.loop_depth -= 1;
                loop_result
            }
            Statement::Break => {
                if ctx.loop_depth > 0 {
                    Err(DbError::Break)
                } else {
                    Err(DbError::Execution("BREAK outside of WHILE".into()))
                }
            }
            Statement::Continue => {
                if ctx.loop_depth > 0 {
                    Err(DbError::Continue)
                } else {
                    Err(DbError::Execution("CONTINUE outside of WHILE".into()))
                }
            }
            Statement::Return(expr) => {
                let value = if let Some(ref e) = expr {
                    Some(super::evaluator::eval_expr(
                        e,
                        &[],
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?)
                } else {
                    None
                };
                Err(DbError::Return(value))
            }
            Statement::ExecDynamic(stmt) => {
                let sql_val = super::evaluator::eval_expr(
                    &stmt.sql_expr,
                    &[],
                    ctx,
                    self.catalog,
                    self.storage,
                    self.clock,
                )?;
                let sql_str = sql_val.to_string_value();
                let batch = crate::parser::parse_batch(&sql_str)?;
                self.execute_batch(&batch, ctx)
            }
            Statement::ExecProcedure(stmt) => self.execute_procedure(stmt, ctx),
            Statement::SpExecuteSql(stmt) => self.execute_sp_executesql(stmt, ctx),
            Statement::SelectAssign(stmt) => self.execute_select_assign(stmt, ctx),
            Statement::CreateProcedure(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.create_routine(RoutineDef {
                    schema,
                    name: stmt.name.name,
                    params: stmt.params,
                    kind: RoutineKind::Procedure { body: stmt.body },
                })?;
                Ok(None)
            }
            Statement::DropProcedure(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.drop_routine(&schema, &stmt.name.name, false)?;
                Ok(None)
            }
            Statement::CreateFunction(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.create_routine(RoutineDef {
                    schema,
                    name: stmt.name.name,
                    params: stmt.params,
                    kind: RoutineKind::Function {
                        returns: stmt.returns,
                        body: stmt.body,
                    },
                })?;
                Ok(None)
            }
            Statement::DropFunction(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                self.catalog.drop_routine(&schema, &stmt.name.name, true)?;
                Ok(None)
            }
            Statement::WithCte(stmt) => {
                ctx.ctes = super::cte::CteStorage::new();

                for cte_def in &stmt.ctes {
                    let result = QueryExecutor {
                        catalog: self.catalog as &dyn Catalog,
                        storage: self.storage as &dyn Storage,
                        clock: self.clock,
                    }
                    .execute_select(cte_def.query.clone(), ctx)?;

                    let table_def = crate::catalog::TableDef {
                        id: 0,
                        schema_id: 1,
                        name: cte_def.name.clone(),
                        columns: result
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(i, name)| crate::catalog::ColumnDef {
                                id: (i + 1) as u32,
                                name: name.clone(),
                                data_type: crate::types::DataType::VarChar { max_len: 4000 },
                                nullable: true,
                                primary_key: false,
                                unique: false,
                                identity: None,
                                default: None,
                                default_constraint_name: None,
                                check: None,
                                check_constraint_name: None,
                                computed_expr: None,
                            })
                            .collect(),
                        check_constraints: vec![],
                    };

                    let rows: Vec<StoredRow> = result
                        .rows
                        .into_iter()
                        .map(|values| StoredRow {
                            values,
                            deleted: false,
                        })
                        .collect();

                    ctx.ctes.insert(&cte_def.name, table_def, rows);
                }

                self.execute_batch(&[(*stmt.body).clone()], ctx)
            }
            Statement::SetOp(stmt) => {
                let left_result = self.execute(*stmt.left, ctx)?;
                let right_result = self.execute(*stmt.right, ctx)?;

                match (left_result, right_result) {
                    (Some(left), Some(right)) => {
                        let result = engine::execute_set_op(left, right, stmt.op)?;
                        Ok(Some(result))
                    }
                    _ => Err(DbError::Execution(
                        "set operations require both sides to return results".into(),
                    )),
                }
            }
        }
    }

    pub fn execute_batch(
        &mut self,
        stmts: &[Statement],
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        ctx.enter_scope();
        let mut last_result = Ok(None);
        let mut early_err: Option<DbError> = None;
        for stmt in stmts {
            match self.execute(stmt.clone(), ctx) {
                Ok(r) => {
                    last_result = Ok(r);
                }
                Err(DbError::Return(v)) => {
                    early_err = Some(DbError::Return(v));
                    break;
                }
                Err(e) => {
                    early_err = Some(e);
                    break;
                }
            };
        }
        self.cleanup_scope_table_vars(ctx)?;
        if let Some(err) = early_err {
            Err(err)
        } else {
            last_result
        }
    }

    fn execute_select_assign(
        &mut self,
        stmt: crate::ast::SelectAssignStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        if stmt.targets.is_empty() {
            return Ok(None);
        }
        if stmt.from.is_none() {
            for t in stmt.targets {
                let val = super::evaluator::eval_expr(
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

        let q = crate::ast::SelectStmt {
            from: stmt.from,
            joins: stmt.joins,
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

    fn execute_procedure(
        &mut self,
        stmt: crate::ast::ExecProcedureStmt,
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
                    let val = super::evaluator::eval_expr(
                        def,
                        &[],
                        ctx,
                        self.catalog,
                        self.storage,
                        self.clock,
                    )?;
                    let ty = super::type_mapping::data_type_spec_to_runtime(&param.data_type);
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
            let val = super::evaluator::eval_expr(
                &arg.expr,
                &[],
                ctx,
                self.catalog,
                self.storage,
                self.clock,
            )?;
            let ty = super::type_mapping::data_type_spec_to_runtime(&param.data_type);
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
        self.cleanup_scope_table_vars(ctx)?;
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

    fn execute_sp_executesql(
        &mut self,
        stmt: crate::ast::SpExecuteSqlStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        let sql_val = super::evaluator::eval_expr(
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
            let val = super::evaluator::eval_expr(
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
        self.cleanup_scope_table_vars(ctx)?;
        for (outer, val) in outs {
            if let Some((ty, out)) = ctx.variables.get_mut(&outer) {
                *out = coerce_value_to_type(val, ty)?;
            }
        }
        exec_result
    }

    fn cleanup_scope_table_vars(&mut self, ctx: &mut ExecutionContext) -> Result<(), DbError> {
        let dropped_physical = ctx.leave_scope_collect_table_vars();
        for physical in dropped_physical {
            if self.catalog.find_table("dbo", &physical).is_none() {
                continue;
            }
            SchemaExecutor {
                catalog: self.catalog,
                storage: self.storage,
            }
            .drop_table(DropTableStmt {
                name: ObjectName {
                    schema: Some("dbo".to_string()),
                    name: physical,
                },
            })?;
        }
        Ok(())
    }
}
