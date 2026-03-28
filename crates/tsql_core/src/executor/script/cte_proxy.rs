use crate::ast::WithCteStmt;
use crate::catalog::TableDef;
use crate::error::DbError;
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use crate::storage::StoredRow;
use super::ScriptExecutor;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_with_cte(
        &mut self,
        stmt: WithCteStmt,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<QueryResult>, DbError> {
        let mut ctes = super::super::cte::CteStorage::new();

        for cte_def in &stmt.ctes {
            if stmt.recursive {
                // Recursive CTE logic
                if let crate::ast::Statement::SetOp(set_op) = &cte_def.query {
                    if let crate::ast::SetOpKind::UnionAll = set_op.op {
                        let anchor_stmt = *set_op.left.clone();
                        let recursive_stmt = *set_op.right.clone();

                        // 2. Execute Anchor Member
                        let result = self.execute(anchor_stmt, ctx)?.ok_or_else(|| {
                            DbError::Execution("CTE anchor member must return a result set".into())
                        })?;

                        let table_def = TableDef {
                            id: 0,
                            schema_id: 1,
                            name: cte_def.name.clone(),
                            columns: result.columns.iter().enumerate().map(|(i, name)| {
                                crate::catalog::ColumnDef {
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
                                }
                            }).collect(),
                            check_constraints: vec![],
                            foreign_keys: vec![],
                        };

                        let mut all_rows: Vec<StoredRow> = result.rows.iter().map(|v| StoredRow { values: v.clone(), deleted: false }).collect();
                        let mut working_set = all_rows.clone();
                        
                        // 3. Iterative execution of recursive member
                        let mut iteration = 0;
                        let max_recursion = 100; // Default MS SQL limit

                        while !working_set.is_empty() && iteration < max_recursion {
                            iteration += 1;
                            
                            // Temporarily put only the working set into the CTE storage
                            // so the recursive member sees only the rows from the previous iteration
                            ctes.insert(&cte_def.name, table_def.clone(), working_set.clone());
                            let old_ctes = std::mem::replace(&mut ctx.ctes, ctes.clone());

                            let step_result = self.execute(recursive_stmt.clone(), ctx)?;
                            
                            ctx.ctes = old_ctes;

                            let Some(res) = step_result else {
                                break;
                            };

                            if res.rows.is_empty() {
                                break;
                            }

                            working_set = res.rows.iter().map(|v| StoredRow { values: v.clone(), deleted: false }).collect();
                            all_rows.extend(working_set.clone());
                        }

                        if iteration >= max_recursion {
                            return Err(DbError::Execution(format!("The maximum recursion 100 has been exhausted before statement completion.")));
                        }

                        ctes.insert(&cte_def.name, table_def, all_rows);
                    } else {
                        return Err(DbError::Execution("Recursive CTE must use UNION ALL".into()));
                    }
                } else {
                    return Err(DbError::Execution("Recursive CTE must be a UNION ALL of anchor and recursive members".into()));
                }
            } else {
                // Standard non-recursive CTE
                let result = self.execute(cte_def.query.clone(), ctx)?.ok_or_else(|| {
                    DbError::Execution("CTE query must return a result set".into())
                })?;

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

                ctes.insert(&cte_def.name, table_def, rows);
            }
        }

        ctx.ctes = ctes;
        self.execute_batch(&[(*stmt.body).clone()], ctx)
    }
}
