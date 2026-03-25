mod dml;
mod procedural;

use super::clock::Clock;
use super::context::ExecutionContext;
use super::engine;
use super::mutation::MutationExecutor;
use super::query::QueryExecutor;
use super::result::QueryResult;
use super::schema::SchemaExecutor;
use crate::ast::{DropTableStmt, ObjectName, Statement};
use crate::catalog::{Catalog, RoutineDef, RoutineKind, TableDef};
use crate::error::DbError;
use crate::storage::{Storage, StoredRow};

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
            Statement::CreateView(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_view(stmt)?;
                Ok(None)
            }
            Statement::DropView(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .drop_view(stmt)?;
                Ok(None)
            }
            Statement::Insert(stmt) => MutationExecutor {
                catalog: self.catalog,
                storage: self.storage,
                clock: self.clock,
            }
            .execute_insert_with_context(stmt, ctx),
            Statement::Select(stmt) => QueryExecutor {
                catalog: self.catalog as &dyn Catalog,
                storage: self.storage as &dyn Storage,
                clock: self.clock,
            }
            .execute_select(stmt, ctx)
            .map(Some),
            Statement::Update(stmt) => MutationExecutor {
                catalog: self.catalog,
                storage: self.storage,
                clock: self.clock,
            }
            .execute_update_with_context(stmt, ctx),
            Statement::Delete(stmt) => MutationExecutor {
                catalog: self.catalog,
                storage: self.storage,
                clock: self.clock,
            }
            .execute_delete_with_context(stmt, ctx),
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
                    let coerced = super::value_ops::coerce_value_to_type(val, ty)?;
                    *var = coerced;
                } else {
                    return Err(DbError::Semantic(format!(
                        "variable '{}' not declared",
                        stmt.name
                    )));
                }
                Ok(None)
            }
            Statement::SetOption(_) => Err(DbError::Execution(
                "SET option statements are handled at engine level".into(),
            )),
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

                    let table_def = TableDef {
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
                                data_type: result.column_types[i].clone(),
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
                        foreign_keys: vec![],
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
            Statement::Merge(stmt) => self.execute_merge(stmt, ctx),
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
