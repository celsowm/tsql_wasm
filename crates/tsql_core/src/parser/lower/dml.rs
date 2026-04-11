use crate::parser::ast;
use crate::ast as executor_ast;
use crate::error::DbError;
use super::common::{lower_expr, lower_object_name, lower_object_name_owned, lower_order_by_expr};

pub fn lower_dml(dml: ast::DmlStatement) -> Result<executor_ast::Statement, DbError> {
    match dml {
        ast::DmlStatement::Select(s) => {
            if let Some(ref op) = s.set_op {
                let mut left_parser = (*s).clone();
                left_parser.set_op = None;
                let left = lower_select(left_parser)?;
                let right = lower_select(op.right.clone())?;
                let kind = match op.kind {
                    ast::SetOpKind::Union => executor_ast::statements::query::SetOpKind::Union,
                    ast::SetOpKind::UnionAll => executor_ast::statements::query::SetOpKind::UnionAll,
                    ast::SetOpKind::Intersect => executor_ast::statements::query::SetOpKind::Intersect,
                    ast::SetOpKind::Except => executor_ast::statements::query::SetOpKind::Except,
                };
                let set_op = executor_ast::statements::query::SetOpStmt {
                    left: Box::new(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Select(left))),
                    op: kind,
                    right: Box::new(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Select(right))),
                };
                return Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::SetOp(set_op)));
            }
            Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Select(lower_select(*s)?)))
        }
        ast::DmlStatement::Insert(s) => Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Insert(lower_insert(*s)?))),
        ast::DmlStatement::Update(s) => Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Update(lower_update(*s)?))),
        ast::DmlStatement::Delete(s) => Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Delete(lower_delete(*s)?))),
        ast::DmlStatement::Merge(s) => Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::Merge(lower_merge(*s)?))),
        ast::DmlStatement::SelectAssign { assignments, from, selection } => {
            let mut joins = Vec::new();
            let from_tr = if let Some(from_ref) = from {
                let (tr, mut j) = lower_table_ref_recursive(from_ref)?;
                joins.append(&mut j);
                Some(tr)
            } else {
                None
            };
            Ok(executor_ast::Statement::Dml(executor_ast::statements::DmlStatement::SelectAssign(executor_ast::statements::procedural::SelectAssignStmt {
                targets: assignments.into_iter().map(|a| Ok(executor_ast::statements::procedural::SelectAssignTarget {
                    variable: a.variable,
                    expr: lower_expr(a.expr)?,
                })).collect::<Result<Vec<_>, _>>()?,
                from: from_tr,
                joins,
                selection: selection.map(lower_expr).transpose()?,
            })))
        }
    }
}

pub fn lower_select(s: ast::SelectStmt) -> Result<executor_ast::statements::query::SelectStmt, DbError> {
    if s.set_op.is_some() {
        return Err(DbError::Parse("Subqueries with UNION/INTERSECT/EXCEPT not yet supported in this version".into()));
    }

    let from_clause = lower_select_from_clause(s.from, s.joins)?;

    Ok(executor_ast::statements::query::SelectStmt {
        distinct: s.distinct,
        top: s.top.map(|top| Ok(executor_ast::statements::query::TopSpec { value: lower_expr(top.value)? })).transpose()?,
        projection: s.projection.into_iter().map(|i| Ok(executor_ast::statements::query::SelectItem {
            expr: lower_expr(i.expr)?,
            alias: i.alias,
        })).collect::<Result<Vec<_>, DbError>>()?,
        into_table: s.into_table.map(lower_object_name_owned),
        from_clause,
        applies: s.applies.into_iter().map(|a| {
            let (tr, extra_joins) = lower_table_ref_recursive(a.table)?;
            if !extra_joins.is_empty() {
                return Err(DbError::Parse("Joins inside APPLY are not yet supported in this version".into()));
            }
            let subquery = match tr.factor {
                executor_ast::common::TableFactor::Derived(s) => *s,
                executor_ast::common::TableFactor::Values { rows, columns } => {
                    executor_ast::statements::query::SelectStmt {
                        from_clause: Some(executor_ast::statements::query::FromNode::Table(executor_ast::common::TableRef {
                            factor: executor_ast::common::TableFactor::Values { rows, columns },
                            alias: tr.alias.clone(),
                            pivot: None,
                            unpivot: None,
                            hints: Vec::new(),
                        })),
                        applies: Vec::new(),
                        projection: vec![executor_ast::statements::query::SelectItem {
                            expr: executor_ast::expressions::Expr::Wildcard,
                            alias: None,
                        }],
                        into_table: None,
                        distinct: false,
                        top: None,
                        selection: None,
                        group_by: Vec::new(),
                        having: None,
                        order_by: Vec::new(),
                        offset: None,
                        fetch: None,
                    }
                }
                _ => return Err(DbError::Parse("Only subqueries and VALUES are supported in APPLY in this version".into())),
            };
            Ok(executor_ast::statements::query::ApplyClause {
                apply_type: match a.apply_type {
                    ast::ApplyType::Cross => executor_ast::statements::query::ApplyType::Cross,
                    ast::ApplyType::Outer => executor_ast::statements::query::ApplyType::Outer,
                },
                subquery,
                alias: tr.alias.unwrap_or_default(),
            })
        }).collect::<Result<Vec<_>, DbError>>()?,
        selection: s.selection.map(lower_expr).transpose()?,
        group_by: s.group_by.into_iter().map(lower_expr).collect::<Result<Vec<_>, DbError>>()?,
        having: s.having.map(lower_expr).transpose()?,
        order_by: s.order_by.into_iter().map(lower_order_by_expr).collect::<Result<Vec<_>, DbError>>()?,
        offset: s.offset.map(lower_expr).transpose()?,
        fetch: s.fetch.map(lower_expr).transpose()?,
    })
}

fn lower_select_from_clause(
    from: Option<ast::TableRef>,
    joins: Vec<ast::JoinClause>,
) -> Result<Option<executor_ast::statements::query::FromNode>, DbError> {
    let Some(from_ref) = from else {
        return Ok(None);
    };
    let mut node = lower_table_ref_to_from_node(from_ref)?;
    for join in joins {
        node = executor_ast::statements::query::FromNode::Join {
            left: Box::new(node),
            join_type: lower_join_type(join.join_type),
            right: Box::new(lower_table_ref_to_from_node(join.table)?),
            on: join.on.map(lower_expr).transpose()?,
        };
    }
    Ok(Some(node))
}

fn lower_table_ref_to_from_node(
    tr: ast::TableRef,
) -> Result<executor_ast::statements::query::FromNode, DbError> {
    let alias = tr.alias.clone();
    let hints = tr.hints.clone();
    let pivot = tr.pivot.clone();
    let unpivot = tr.unpivot.clone();
    match tr.factor {
        ast::TableFactor::JoinedGroup { base, joins } => {
            if !hints.is_empty() || pivot.is_some() || unpivot.is_some() {
                return Err(DbError::Parse(
                    "hints/PIVOT/UNPIVOT on grouped joins are not supported in this version".into(),
                ));
            }
            let mut node = lower_table_ref_to_from_node(*base)?;
            for join in joins {
                node = executor_ast::statements::query::FromNode::Join {
                    left: Box::new(node),
                    join_type: lower_join_type(join.join_type),
                    right: Box::new(lower_table_ref_to_from_node(join.table)?),
                    on: join.on.map(lower_expr).transpose()?,
                };
            }
            if let Some(alias) = alias {
                Ok(executor_ast::statements::query::FromNode::Aliased {
                    source: Box::new(node),
                    alias,
                })
            } else {
                Ok(node)
            }
        }
        _ => Ok(executor_ast::statements::query::FromNode::Table(lower_table_ref_flat(tr)?)),
    }
}

fn lower_table_ref_flat(tr: ast::TableRef) -> Result<executor_ast::common::TableRef, DbError> {
    let alias = tr.alias;
    let hints = tr.hints;
    let pivot = tr.pivot.map(|p| Box::new(executor_ast::common::PivotSpec {
        aggregate_func: p.aggregate_func,
        aggregate_col: p.aggregate_col,
        pivot_col: p.pivot_col,
        pivot_values: p.pivot_values,
    }));
    let unpivot = tr.unpivot.map(|u| Box::new(executor_ast::common::UnpivotSpec {
        value_col: u.value_col,
        pivot_col: u.pivot_col,
        column_list: u.column_list,
    }));

    let factor = match tr.factor {
        ast::TableFactor::Named(name) => {
            executor_ast::common::TableFactor::Named(lower_object_name_owned(name))
        }
        ast::TableFactor::Values { rows, columns } => executor_ast::common::TableFactor::Values {
            rows: rows
                .into_iter()
                .map(|r| {
                    r.into_iter()
                        .map(lower_expr)
                        .collect::<Result<Vec<_>, DbError>>()
                })
                .collect::<Result<Vec<_>, DbError>>()?,
            columns,
        },
        ast::TableFactor::Derived(subquery) => {
            executor_ast::common::TableFactor::Derived(Box::new(lower_select(*subquery)?))
        }
        ast::TableFactor::TableValuedFunction {
            name,
            args,
            alias: tvf_alias,
        } => {
            let func_name = match name.last() {
                Some(last) => last.to_string(),
                None => return Err(DbError::Parse("table-valued function name is empty".into())),
            };
            let arg_strs: Vec<String> = args
                .into_iter()
                .map(|a| {
                    lower_expr(a).map(|expr| crate::executor::tooling::formatting::format_expr(&expr))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let full_name = format!("{}({})", func_name, arg_strs.join(", "));
            return Ok(executor_ast::common::TableRef {
                factor: executor_ast::common::TableFactor::Named(executor_ast::common::ObjectName {
                    schema: if name.len() > 1 {
                        Some(name[0].to_string())
                    } else {
                        None
                    },
                    name: full_name,
                }),
                alias: tvf_alias.or(alias),
                pivot,
                unpivot,
                hints: Vec::new(),
            });
        }
        ast::TableFactor::JoinedGroup { .. } => {
            return Err(DbError::Parse(
                "internal error: grouped joins must be lowered through FromNode".into(),
            ));
        }
    };

    Ok(executor_ast::common::TableRef {
        factor,
        alias,
        pivot,
        unpivot,
        hints,
    })
}

fn lower_join_type(join_type: ast::JoinType) -> executor_ast::statements::query::JoinType {
    match join_type {
        ast::JoinType::Inner => executor_ast::statements::query::JoinType::Inner,
        ast::JoinType::Left => executor_ast::statements::query::JoinType::Left,
        ast::JoinType::Right => executor_ast::statements::query::JoinType::Right,
        ast::JoinType::Full => executor_ast::statements::query::JoinType::Full,
        ast::JoinType::Cross => executor_ast::statements::query::JoinType::Cross,
    }
}

pub fn lower_join_clause(join: ast::JoinClause) -> Result<executor_ast::statements::query::JoinClause, DbError> {
    Ok(executor_ast::statements::query::JoinClause {
        join_type: lower_join_type(join.join_type),
        table: lower_table_ref_recursive(join.table)?.0,
        on: join.on.map(lower_expr).transpose()?,
    })
}

pub fn lower_from_clause_internal(tables: Vec<ast::TableRef>) -> Result<(executor_ast::common::TableRef, Vec<executor_ast::statements::query::JoinClause>), DbError> {
    if tables.is_empty() {
        return Err(DbError::Parse("FROM clause must have at least one table".into()));
    }
    let mut iter = tables.into_iter();
    let first = match iter.next() {
        Some(first) => first,
        None => return Err(DbError::Parse("FROM clause must have at least one table".into())),
    };
    let (tr, mut joins) = lower_table_ref_recursive(first)?;
    for t in iter {
        let (next_tr, mut next_j) = lower_table_ref_recursive(t)?;
        joins.push(executor_ast::statements::query::JoinClause {
            join_type: executor_ast::statements::query::JoinType::Cross,
            table: next_tr,
            on: None,
        });
        joins.append(&mut next_j);
    }
    Ok((tr, joins))
}

pub fn lower_table_ref_recursive(tr: ast::TableRef) -> Result<(executor_ast::common::TableRef, Vec<executor_ast::statements::query::JoinClause>), DbError> {
    let alias = tr.alias;
    let hints = tr.hints;
    let pivot = tr.pivot.map(|p| Box::new(executor_ast::common::PivotSpec {
        aggregate_func: p.aggregate_func,
        aggregate_col: p.aggregate_col,
        pivot_col: p.pivot_col,
        pivot_values: p.pivot_values,
    }));
    let unpivot = tr.unpivot.map(|u| Box::new(executor_ast::common::UnpivotSpec {
        value_col: u.value_col,
        pivot_col: u.pivot_col,
        column_list: u.column_list,
    }));

    match tr.factor {
        ast::TableFactor::Named(name) => Ok((executor_ast::common::TableRef {
            factor: executor_ast::common::TableFactor::Named(lower_object_name_owned(name)),
            alias,
            pivot,
            unpivot,
            hints,
        }, Vec::new())),
        ast::TableFactor::JoinedGroup { base, joins } => {
            if alias.is_some() {
                return Err(DbError::Parse("aliases on grouped joins are not supported in this version".into()));
            }
            let (base_tr, mut extra_joins) = lower_table_ref_recursive(*base)?;
            for join in joins {
                extra_joins.push(lower_join_clause(join)?);
            }
            Ok((base_tr, extra_joins))
        }
        ast::TableFactor::Values { rows, columns } => Ok((executor_ast::common::TableRef {
            factor: executor_ast::common::TableFactor::Values {
                rows: rows.into_iter().map(|r| r.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()).collect::<Result<Vec<_>, _>>()?,
                columns: columns,
            },
            alias,
            pivot,
            unpivot,
            hints,
        }, Vec::new())),
        ast::TableFactor::Derived(subquery) => Ok((executor_ast::common::TableRef {
            factor: executor_ast::common::TableFactor::Derived(Box::new(lower_select(*subquery)?)),
            alias,
            pivot,
            unpivot,
            hints,
        }, Vec::new())),
        ast::TableFactor::TableValuedFunction { name, args, alias: tvf_alias } => {
            let func_name = match name.last() {
                Some(last) => last.to_string(),
                None => return Err(DbError::Parse("table-valued function name is empty".into())),
            };
            let arg_strs: Vec<String> = args
                .into_iter()
                .map(|a| lower_expr(a).map(|expr| crate::executor::tooling::formatting::format_expr(&expr)))
                .collect::<Result<Vec<_>, _>>()?;
            let full_name = format!("{}({})", func_name, arg_strs.join(", "));
            Ok((executor_ast::common::TableRef {
                factor: executor_ast::common::TableFactor::Named(executor_ast::common::ObjectName {
                    schema: if name.len() > 1 { Some(name[0].to_string()) } else { None },
                    name: full_name,
                }),
                alias: tvf_alias.or(alias),
                pivot,
                unpivot,
                hints: Vec::new(),
            }, Vec::new()))
        }
    }
}

pub fn lower_insert(s: ast::InsertStmt) -> Result<executor_ast::statements::dml::InsertStmt, DbError> {
    Ok(executor_ast::statements::dml::InsertStmt {
        table: lower_object_name(s.table),
        columns: if s.columns.is_empty() { None } else { Some(s.columns) },
        source: match s.source {
            ast::InsertSource::Values(rows) => executor_ast::statements::dml::InsertSource::Values(
                rows.into_iter().map(|r| r.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()).collect::<Result<Vec<_>, _>>()?
            ),
            ast::InsertSource::Select(sel) => executor_ast::statements::dml::InsertSource::Select(Box::new(lower_select(*sel)?)),
            ast::InsertSource::Exec { procedure, args } => executor_ast::statements::dml::InsertSource::Exec(Box::new(executor_ast::Statement::Procedural(executor_ast::statements::ProceduralStatement::ExecProcedure(executor_ast::statements::procedural::ExecProcedureStmt {
                return_variable: None,
                name: lower_object_name(procedure),
                args: args.into_iter().map(|e| Ok(executor_ast::statements::procedural::ExecArgument {
                    name: None, 
                    expr: lower_expr(e)?,
                    is_output: false,
                })).collect::<Result<Vec<_>, DbError>>()?,
            })))),
            ast::InsertSource::DefaultValues => executor_ast::statements::dml::InsertSource::DefaultValues,
        },
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_update(s: ast::UpdateStmt) -> Result<executor_ast::statements::dml::UpdateStmt, DbError> {
    let (table_tr, mut extra_joins) = lower_table_ref_recursive(s.table)?;
    let table = match table_tr.factor {
        executor_ast::common::TableFactor::Named(ref o) => o.clone(),
        _ => return Err(DbError::Parse("UPDATE target must be an object".into())),
    };
    
    for join in s.joins {
        extra_joins.push(lower_join_clause(join)?);
    }
    
    let mut from_clause = None;
    if let Some(from_refs) = s.from {
        let (tr, mut j) = lower_from_clause_internal(from_refs)?;
        extra_joins.append(&mut j);
        from_clause = Some(executor_ast::statements::dml::FromClause {
            tables: vec![tr],
            joins: extra_joins,
            applies: Vec::new(),
        });
    } else if !extra_joins.is_empty() {
        from_clause = Some(executor_ast::statements::dml::FromClause {
            tables: vec![table_tr],
            joins: extra_joins,
            applies: Vec::new(),
        });
    }

    Ok(executor_ast::statements::dml::UpdateStmt {
        table,
        assignments: s.assignments.into_iter().map(|a| Ok(executor_ast::statements::dml::Assignment {
            column: a.column,
            expr: lower_expr(a.expr)?,
        })).collect::<Result<Vec<_>, _>>()?,
        top: s.top.map(|e| Ok(executor_ast::statements::query::TopSpec { value: lower_expr(e)? })).transpose()?,
        from: from_clause,
        selection: s.selection.map(lower_expr).transpose()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_delete(s: ast::DeleteStmt) -> Result<executor_ast::statements::dml::DeleteStmt, DbError> {
    let table = lower_object_name(s.table);
    let (tr, mut joins) = lower_from_clause_internal(s.from)?;
    
    for join in s.joins {
        joins.push(lower_join_clause(join)?);
    }

    Ok(executor_ast::statements::dml::DeleteStmt {
        table,
        top: s.top.map(|e| Ok(executor_ast::statements::query::TopSpec { value: lower_expr(e)? })).transpose()?,
        from: Some(executor_ast::statements::dml::FromClause {
            tables: vec![tr],
            joins,
            applies: Vec::new(),
        }),
        selection: s.selection.map(lower_expr).transpose()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_merge(s: ast::MergeStmt) -> Result<executor_ast::statements::dml::MergeStmt, DbError> {
    let (target, _) = lower_table_ref_recursive(s.target)?;
    let (source_tr, _) = lower_table_ref_recursive(s.source)?;
    Ok(executor_ast::statements::dml::MergeStmt {
        target,
        source: executor_ast::statements::dml::MergeSource::Table(source_tr),
        on_condition: lower_expr(s.on_condition)?,
        when_clauses: s.when_clauses.into_iter().map(|w| Ok(executor_ast::statements::dml::MergeWhenClause {
            when: match w.when {
                ast::MergeWhen::Matched => executor_ast::statements::dml::MergeWhen::Matched,
                ast::MergeWhen::NotMatched => executor_ast::statements::dml::MergeWhen::NotMatched,
                ast::MergeWhen::NotMatchedBySource => executor_ast::statements::dml::MergeWhen::NotMatchedBySource,
            },
            condition: w.condition.map(lower_expr).transpose()?,
            action: match w.action {
                ast::MergeAction::Update { assignments } => executor_ast::statements::dml::MergeAction::Update {
                    assignments: assignments.into_iter().map(|a| Ok(executor_ast::statements::dml::Assignment {
                        column: a.column,
                        expr: lower_expr(a.expr)?,
                    })).collect::<Result<Vec<_>, _>>()?,
                },
                ast::MergeAction::Insert { columns, values } => executor_ast::statements::dml::MergeAction::Insert {
                    columns: columns,
                    values: values.into_iter().map(lower_expr).collect::<Result<Vec<_>, _>>()?,
                },
                ast::MergeAction::Delete => executor_ast::statements::dml::MergeAction::Delete,
            },
        })).collect::<Result<Vec<_>, _>>()?,
        output: s.output.map(|cols| cols.into_iter().map(lower_output_column).collect()),
        output_into: s.output_into.map(lower_object_name),
    })
}

pub fn lower_output_column(c: ast::OutputColumn) -> executor_ast::statements::dml::OutputColumn {
    executor_ast::statements::dml::OutputColumn {
        source: match c.source {
            ast::OutputSource::Inserted => executor_ast::statements::dml::OutputSource::Inserted,
            ast::OutputSource::Deleted => executor_ast::statements::dml::OutputSource::Deleted,
        },
        column: c.column,
        alias: c.alias,
        is_wildcard: c.is_wildcard,
    }
}
