use super::super::ScriptExecutor;
use crate::ast::WithCteStmt;
use crate::catalog::TableDef;
use crate::error::{DbError, StmtOutcome};
use crate::executor::context::ExecutionContext;
use crate::executor::result::QueryResult;
use crate::storage::StoredRow;

impl<'a> ScriptExecutor<'a> {
    pub(crate) fn execute_with_cte(
        &mut self,
        stmt: WithCteStmt,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<QueryResult>, DbError> {
        let old_ctes = ctx.row.ctes.clone();

        for cte_def in &stmt.ctes {
            if stmt.recursive {
                // Recursive CTE logic
                if let crate::ast::Statement::Dml(crate::ast::DmlStatement::SetOp(set_op)) =
                    &cte_def.query
                {
                    if let crate::ast::SetOpKind::UnionAll = set_op.op {
                        let anchor_stmt = *set_op.left.clone();
                        let recursive_stmt = *set_op.right.clone();

                        // 2. Execute Anchor Member
                        let anchor_outcome = self.execute(anchor_stmt, ctx)?;
                        let result = match anchor_outcome {
                            StmtOutcome::Ok(Some(r)) => r,
                            StmtOutcome::Ok(None) => {
                                return Err(DbError::Execution(
                                    "CTE anchor member must return a result set".into(),
                                ))
                            }
                            // Propagate control flow
                            other => return other.into_result(),
                        };

                        let table_def = TableDef {
                            id: 0,
                            schema_id: 1,
                            schema_name: "dbo".to_string(),
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
                                    computed_expr: None,
                                    check: None,
                                    check_constraint_name: None,
                                    ansi_padding_on: true,
                                })
                                .collect(),
                            check_constraints: vec![],
                            foreign_keys: vec![],
                        };

                        let mut all_rows: Vec<StoredRow> = result
                            .rows
                            .iter()
                            .map(|v| StoredRow {
                                values: v.clone(),
                                deleted: false,
                            })
                            .collect();
                        let mut working_set = all_rows.clone();

                        // 3. Iterative execution of recursive member
                        let mut iteration = 0;
                        let max_recursion = 100; // Default MS SQL limit

                        // Hoist table_def clone out of the loop
                        let table_def_inner = table_def.clone();

                        while !working_set.is_empty() && iteration < max_recursion {
                            iteration += 1;

                            // Temporarily put only the working set into the CTE storage
                            // so the recursive member sees only the rows from the previous iteration
                            let mut iteration_ctes = old_ctes.clone();
                            iteration_ctes.insert(
                                &cte_def.name,
                                table_def_inner.clone(),
                                working_set,
                            );
                            let prev_ctes = std::mem::replace(&mut ctx.row.ctes, iteration_ctes);

                            let step_outcome = self.execute(recursive_stmt.clone(), ctx)?;

                            ctx.row.ctes = prev_ctes;

                            let res = match step_outcome {
                                StmtOutcome::Ok(Some(r)) => r,
                                StmtOutcome::Ok(None) => break,
                                // Propagate control flow
                                other => return other.into_result(),
                            };

                            if res.rows.is_empty() {
                                break;
                            }

                            working_set = res
                                .rows
                                .iter()
                                .map(|v| StoredRow {
                                    values: v.clone(),
                                    deleted: false,
                                })
                                .collect();
                            all_rows.extend(working_set.iter().cloned());
                        }

                        if iteration >= max_recursion {
                            return Err(DbError::Execution("The maximum recursion 100 has been exhausted before statement completion.".to_string()));
                        }

                        ctx.row.ctes.insert(&cte_def.name, table_def, all_rows);
                    } else {
                        return Err(DbError::Execution(
                            "Recursive CTE must use UNION ALL".into(),
                        ));
                    }
                } else {
                    return Err(DbError::Execution(
                        "Recursive CTE must be a UNION ALL of anchor and recursive members".into(),
                    ));
                }
            } else {
                // Standard non-recursive CTE
                let cte_outcome = self.execute(cte_def.query.clone(), ctx)?;
                let result = match cte_outcome {
                    StmtOutcome::Ok(Some(r)) => r,
                    StmtOutcome::Ok(None) => {
                        return Err(DbError::Execution(
                            "CTE query must return a result set".into(),
                        ))
                    }
                    // Propagate control flow
                    other => return other.into_result(),
                };

                let table_def = TableDef {
                    id: 0,
                    schema_id: 1,
                    schema_name: "dbo".to_string(),
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
                            computed_expr: None,
                            check: None,
                            check_constraint_name: None,
                            ansi_padding_on: true,
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

                ctx.row.ctes.insert(&cte_def.name, table_def, rows);
            }
        }

        let res = self.execute(*stmt.body, ctx);
        ctx.row.ctes = old_ctes;
        // Propagate control flow from the body; unwrap Ok values
        res.map(|outcome| outcome.into_result()).and_then(|r| r)
    }
}
