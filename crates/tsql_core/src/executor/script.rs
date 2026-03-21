use crate::ast::Statement;
use crate::catalog::Catalog;
use crate::error::DbError;
use crate::storage::Storage;
use super::context::ExecutionContext;
use super::clock::Clock;
use super::result::QueryResult;
use super::schema::SchemaExecutor;
use super::mutation::MutationExecutor;
use super::query::QueryExecutor;
use super::value_ops::coerce_value_to_type;

pub struct ScriptExecutor<'a> {
    pub catalog: &'a mut dyn Catalog,
    pub storage: &'a mut dyn Storage,
    pub clock: &'a dyn Clock,
}

impl<'a> ScriptExecutor<'a> {
    pub fn execute(&mut self, stmt: Statement, ctx: &mut ExecutionContext) -> Result<Option<QueryResult>, DbError> {
        match stmt {
             Statement::CreateTable(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .create_table(stmt)?;
                Ok(None)
            }
             Statement::DropTable(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .drop_table(stmt)?;
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
             Statement::TruncateTable(stmt) => {
                let schema = stmt.name.schema_or_dbo();
                let table_name = &stmt.name.name;
                let table = self.catalog.find_table(schema, table_name)
                    .ok_or_else(|| DbError::Semantic(format!("table '{}.{}' not found", schema, table_name)))?
                    .clone();
                self.storage.clear_table(table.id)?;
                Ok(None)
            }
             Statement::AlterTable(stmt) => {
                SchemaExecutor {
                    catalog: self.catalog,
                    storage: self.storage,
                }
                .alter_table(stmt)?;
                Ok(None)
            }
            Statement::Declare(stmt) => {
                  let ty = super::type_mapping::data_type_spec_to_runtime(&stmt.data_type);
                  let value = if let Some(ref default_expr) = stmt.default {
                      super::evaluator::eval_expr(default_expr, &[], ctx, self.catalog, self.storage, self.clock)?
                  } else {
                      crate::types::Value::Null
                  };
                  ctx.variables.insert(stmt.name, (ty, value));
                  Ok(None)
            }
            Statement::Set(stmt) => {
                  let val = super::evaluator::eval_expr(&stmt.expr, &[], ctx, self.catalog, self.storage, self.clock)?;
                  if let Some((ty, var)) = ctx.variables.get_mut(&stmt.name) {
                      let coerced = coerce_value_to_type(val, ty)?;
                      *var = coerced;
                  } else {
                      return Err(DbError::Semantic(format!("variable '{}' not declared", stmt.name)));
                  }
                  Ok(None)
            }
             Statement::If(stmt) => {
                let cond = super::evaluator::eval_expr(&stmt.condition, &[], ctx, self.catalog, self.storage, self.clock)?;
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
                let mut result = None;
                loop {
                    let cond = super::evaluator::eval_expr(&stmt.condition, &[], ctx, self.catalog, self.storage, self.clock)?;
                    if !super::value_ops::truthy(&cond) {
                        break;
                    }
                    result = self.execute_batch(&stmt.body, ctx)?;
                }
                Ok(result)
            }
             Statement::Break => Err(DbError::Execution("BREAK outside of WHILE".into())),
            Statement::Continue => Err(DbError::Execution("CONTINUE outside of WHILE".into())),
            Statement::Return => Ok(None),
            Statement::Exec(stmt) => {
                let sql_val = super::evaluator::eval_expr(&stmt.sql_expr, &[], ctx, self.catalog, self.storage, self.clock)?;
                let sql_str = sql_val.to_string_value();
                let batch = crate::parser::parse_batch(&sql_str)?;
                self.execute_batch(&batch, ctx)
            }
             Statement::WithCte(stmt) => {
                 Err(DbError::Execution("CTE refactoring pending in ScriptExecutor".into()))
             }
             Statement::SetOp(stmt) => {
                let left_result = self.execute(*stmt.left, ctx)?;
                let right_result = self.execute(*stmt.right, ctx)?;

                match (left_result, right_result) {
                    (Some(left), Some(right)) => {
                        // This logic should probably be moved to a SetOpExecutor or similar
                        Err(DbError::Execution("SET operation refactoring pending".into()))
                    }
                    _ => Err(DbError::Execution("set operations require both sides to return results".into())),
                }
            }
        }
    }

    pub fn execute_batch(&mut self, stmts: &[Statement], ctx: &mut ExecutionContext) -> Result<Option<QueryResult>, DbError> {
        let mut last_result = None;
        for stmt in stmts {
            last_result = self.execute(stmt.clone(), ctx)?;
        }
        Ok(last_result)
    }
}
