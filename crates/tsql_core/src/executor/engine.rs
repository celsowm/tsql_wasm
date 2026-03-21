use std::collections::HashMap;
use std::fmt;

use crate::ast::{SetOpKind, Statement};
use crate::catalog::{Catalog, ColumnDef};
use crate::error::DbError;
use crate::storage::InMemoryStorage;
use crate::types::{DataType, Value};

use super::clock::{Clock, SystemClock};
use super::mutation::MutationExecutor;
use super::query::QueryExecutor;
use super::result::QueryResult;
use super::schema::SchemaExecutor;
use super::type_mapping::data_type_spec_to_runtime;
use super::value_ops::coerce_value_to_type;

pub struct Engine {
    pub catalog: Catalog,
    pub storage: InMemoryStorage,
    clock: Box<dyn Clock>,
    variables: HashMap<String, (DataType, Value)>,
    last_identity: Option<i64>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("catalog", &self.catalog)
            .field("storage", &self.storage)
            .field("clock", &"dyn Clock")
            .field("variables", &self.variables)
            .finish()
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Self {
        Self::with_clock(Box::new(SystemClock))
    }

    pub fn with_clock(clock: Box<dyn Clock>) -> Self {
        Self {
            catalog: Catalog::new(),
            storage: InMemoryStorage::default(),
            clock,
            variables: HashMap::new(),
            last_identity: None,
        }
    }

    pub fn reset(&mut self) {
        self.catalog = Catalog::new();
        self.storage = InMemoryStorage::default();
        self.variables.clear();
        self.last_identity = None;
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<Option<QueryResult>, DbError> {
        // Set thread-local variables for the evaluator (once at top level)
        super::evaluator::set_eval_variables(&self.variables);

        let result = self.execute_inner(stmt);

        // Clear after top-level execution
        super::evaluator::clear_eval_variables();

        result
    }

    fn execute_inner(&mut self, stmt: Statement) -> Result<Option<QueryResult>, DbError> {
        // Ensure variables are available for nested calls
        super::evaluator::set_eval_variables(&self.variables);

        match stmt {
            Statement::CreateTable(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .create_table(stmt)?;
                Ok(None)
            }
            Statement::DropTable(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .drop_table(stmt)?;
                Ok(None)
            }
            Statement::CreateSchema(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .create_schema(stmt)?;
                Ok(None)
            }
            Statement::DropSchema(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .drop_schema(stmt)?;
                Ok(None)
            }
            Statement::Insert(stmt) => {
                MutationExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                    clock: self.clock.as_ref(),
                }
                .execute_insert(stmt)?;
                Ok(None)
            }
            Statement::Select(stmt) => QueryExecutor {
                catalog: &self.catalog,
                storage: &self.storage,
                clock: self.clock.as_ref(),
            }
            .execute_select(stmt)
            .map(Some),
            Statement::Update(stmt) => {
                MutationExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                    clock: self.clock.as_ref(),
                }
                .execute_update(stmt)?;
                Ok(None)
            }
            Statement::Delete(stmt) => {
                MutationExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                    clock: self.clock.as_ref(),
                }
                .execute_delete(stmt)?;
                Ok(None)
            }
            Statement::TruncateTable(stmt) => {
                let schema = stmt.name.schema_or_dbo().to_string();
                let table_name = stmt.name.name.clone();
                let schema_id = self
                    .catalog
                    .get_schema_id(&schema)
                    .ok_or_else(|| DbError::Semantic(format!("schema '{}' not found", schema)))?;
                let table = self
                    .catalog
                    .tables
                    .iter()
                    .find(|t| t.schema_id == schema_id && t.name.eq_ignore_ascii_case(&table_name))
                    .ok_or_else(|| {
                        DbError::Semantic(format!("table '{}.{}' not found", schema, table_name))
                    })?
                    .clone();
                if let Some(rows) = self.storage.tables.get_mut(&table.id) {
                    rows.clear();
                }
                Ok(None)
            }
            Statement::AlterTable(stmt) => {
                SchemaExecutor {
                    catalog: &mut self.catalog,
                    storage: &mut self.storage,
                }
                .alter_table(stmt)?;
                Ok(None)
            }
            Statement::WithCte(stmt) => self.execute_cte(stmt),
            Statement::Declare(stmt) => {
                let ty = data_type_spec_to_runtime(&stmt.data_type);
                let value = if let Some(ref default_expr) = stmt.default {
                    eval_expr_static(default_expr, self.clock.as_ref())?
                } else {
                    Value::Null
                };
                self.variables.insert(stmt.name, (ty, value));
                Ok(None)
            }
            Statement::Set(stmt) => {
                let val = eval_expr_static(&stmt.expr, self.clock.as_ref())?;
                if let Some((ty, var)) = self.variables.get_mut(&stmt.name) {
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
                let cond = eval_expr_static(&stmt.condition, self.clock.as_ref())?;
                let truthy = super::value_ops::truthy(&cond);
                if truthy {
                    self.execute_batch(&stmt.then_body)
                } else if let Some(ref else_body) = stmt.else_body {
                    self.execute_batch(else_body)
                } else {
                    Ok(None)
                }
            }
            Statement::BeginEnd(stmts) => self.execute_batch(&stmts),
            Statement::While(stmt) => {
                let mut result = None;
                loop {
                    super::evaluator::set_eval_variables(&self.variables);
                    let cond = eval_expr_static(&stmt.condition, self.clock.as_ref())?;
                    if !super::value_ops::truthy(&cond) {
                        break;
                    }
                    result = self.execute_batch(&stmt.body)?;
                }
                Ok(result)
            }
            Statement::Break => Err(DbError::Execution("BREAK outside of WHILE".into())),
            Statement::Continue => Err(DbError::Execution("CONTINUE outside of WHILE".into())),
            Statement::Return => Ok(None),
            Statement::Exec(stmt) => {
                let sql_val = eval_expr_static(&stmt.sql_expr, self.clock.as_ref())?;
                let sql_str = sql_val.to_string_value();
                let batch = super::super::parser::parse_batch(&sql_str)?;
                self.execute_batch(&batch)
            }
            Statement::SetOp(stmt) => {
                let left_result = self.execute(*stmt.left)?;
                let right_result = self.execute(*stmt.right)?;

                match (left_result, right_result) {
                    (Some(left), Some(right)) => {
                        let result = execute_set_op(left, right, stmt.op)?;
                        Ok(Some(result))
                    }
                    _ => Err(DbError::Execution(
                        "set operations require both sides to return results".into(),
                    )),
                }
            }
        }
    }

    pub fn execute_batch(&mut self, stmts: &[Statement]) -> Result<Option<QueryResult>, DbError> {
        let mut last_result = None;
        for stmt in stmts {
            last_result = self.execute(stmt.clone())?;
        }
        Ok(last_result)
    }

    fn execute_cte(
        &mut self,
        stmt: crate::ast::WithCteStmt,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut temp_table_ids: Vec<(u32, String)> = Vec::new();

        for cte in &stmt.ctes {
            // Execute the CTE query to get results
            let result = QueryExecutor {
                catalog: &self.catalog,
                storage: &self.storage,
                clock: self.clock.as_ref(),
            }
            .execute_select(cte.query.clone())?;

            // Create a temporary table for this CTE
            let table_id = self.catalog.alloc_table_id();
            let columns: Vec<ColumnDef> = result
                .columns
                .iter()
                .enumerate()
                .map(|(_i, name)| ColumnDef {
                    id: self.catalog.alloc_column_id(),
                    name: name.clone(),
                    data_type: DataType::NVarChar { max_len: 4000 },
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    identity: None,
                    default: None,
                })
                .collect();

            let schema_id = self.catalog.get_schema_id("dbo").unwrap_or(0);

            self.catalog.tables.push(crate::catalog::TableDef {
                id: table_id,
                schema_id,
                name: cte.name.clone(),
                columns,
            });

            // Convert result rows to stored rows
            let stored_rows: Vec<crate::storage::StoredRow> = result
                .rows
                .into_iter()
                .map(|row| crate::storage::StoredRow {
                    values: row
                        .into_iter()
                        .map(|jv| match jv {
                            crate::types::JsonValue::Null => Value::Null,
                            crate::types::JsonValue::Bool(b) => Value::Bit(b),
                            crate::types::JsonValue::Number(n) => {
                                if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                                    Value::Int(n as i32)
                                } else {
                                    Value::BigInt(n)
                                }
                            }
                            crate::types::JsonValue::String(s) => Value::NVarChar(s),
                        })
                        .collect(),
                    deleted: false,
                })
                .collect();

            self.storage.tables.insert(table_id, stored_rows);
            temp_table_ids.push((table_id, cte.name.clone()));
        }

        // Execute the body statement
        let result = self.execute(*stmt.body)?;

        // Clean up temporary CTE tables
        for (table_id, _) in &temp_table_ids {
            self.catalog.tables.retain(|t| t.id != *table_id);
            self.storage.tables.remove(table_id);
        }

        Ok(result)
    }
}

fn execute_set_op(
    left: QueryResult,
    right: QueryResult,
    op: SetOpKind,
) -> Result<QueryResult, DbError> {
    if left.columns.len() != right.columns.len() {
        return Err(DbError::Execution(
            "set operations require the same number of columns".into(),
        ));
    }

    let rows = match op {
        SetOpKind::UnionAll => {
            let mut r = left.rows;
            r.extend(right.rows);
            r
        }
        SetOpKind::Union => {
            let mut r = left.rows;
            for row in right.rows {
                if !r.iter().any(|existing| existing == &row) {
                    r.push(row);
                }
            }
            r
        }
        SetOpKind::Intersect => left
            .rows
            .into_iter()
            .filter(|row| right.rows.iter().any(|r| r == row))
            .collect(),
        SetOpKind::Except => left
            .rows
            .into_iter()
            .filter(|row| !right.rows.iter().any(|r| r == row))
            .collect(),
    };

    Ok(QueryResult {
        columns: left.columns,
        rows,
    })
}

fn eval_expr_static(expr: &crate::ast::Expr, clock: &dyn Clock) -> Result<Value, DbError> {
    let empty_row: super::model::JoinedRow = vec![];
    super::evaluator::eval_expr(expr, &empty_row, clock)
}
