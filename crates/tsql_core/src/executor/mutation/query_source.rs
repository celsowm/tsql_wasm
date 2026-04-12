use crate::ast::{
    Expr, FromClause, FromNode, ObjectName, SelectItem, TableFactor, TableRef, TopSpec,
};
use crate::catalog::TableDef;
use crate::executor::query::plan::{
    FilterPlan, PaginationPlan, ProjectionPlan, RelationalQuery, SortPlan,
};

pub(crate) fn build_mutation_query(
    from: Option<&FromClause>,
    target: &ObjectName,
    table: &TableDef,
    resolved_name: &str,
    selection: Option<Expr>,
    top: Option<TopSpec>,
) -> RelationalQuery {
    RelationalQuery {
        from_clause: build_from_node(from, target, table, resolved_name),
        applies: from.map(|f| f.applies.clone()).unwrap_or_default(),
        projection: ProjectionPlan {
            items: vec![SelectItem {
                expr: Expr::Wildcard,
                alias: None,
            }],
            distinct: false,
        },
        into_table: None,
        filter: FilterPlan {
            selection,
            group_by: vec![],
            having: None,
        },
        sort: SortPlan { order_by: vec![] },
        pagination: PaginationPlan {
            top,
            offset: None,
            fetch: None,
        },
        set_op: None,
    }
}

pub(crate) fn resolve_table_for_mutation(
    from: Option<&FromClause>,
    target: &ObjectName,
    table_lookup: impl Fn(&str, &str) -> Option<TableDef>,
) -> Result<(TableDef, String), crate::error::DbError> {
    let target_name = &target.name;
    if let Some(from_clause) = from {
        if let Some(found) = find_in_from_clause(from_clause, target_name, &table_lookup) {
            return Ok(found);
        }
    }

    let schema = target.schema_or_dbo().to_string();
    let table_name = target.name.clone();
    let t = table_lookup(&schema, &table_name)
        .ok_or_else(|| crate::error::DbError::table_not_found(&schema, &table_name))?;
    Ok((t, table_name))
}

fn find_in_from_clause(
    from_clause: &FromClause,
    target_name: &str,
    table_lookup: &impl Fn(&str, &str) -> Option<TableDef>,
) -> Option<(TableDef, String)> {
    for tref in &from_clause.tables {
        if let Some(found) = match_table_ref(tref, target_name, table_lookup) {
            return Some(found);
        }
    }
    for join in &from_clause.joins {
        if let Some(found) = match_table_ref(&join.table, target_name, table_lookup) {
            return Some(found);
        }
    }
    None
}

fn match_table_ref(
    tref: &TableRef,
    target_name: &str,
    table_lookup: &impl Fn(&str, &str) -> Option<TableDef>,
) -> Option<(TableDef, String)> {
    let tname = tref
        .factor
        .as_object_name()
        .map(|o| o.name.as_str())
        .unwrap_or("");
    let alias = tref.alias.as_deref().unwrap_or(tname);
    if !alias.eq_ignore_ascii_case(target_name)
        && (tref.factor.is_derived() || !tname.eq_ignore_ascii_case(target_name))
    {
        return None;
    }

    let schema = tref
        .factor
        .as_object_name()
        .map(|o| o.schema_or_dbo())
        .unwrap_or("dbo");
    let table = match table_lookup(schema, tname) {
        Some(t) => t,
        None => {
            return None;
        }
    };
    Some((table, tname.to_string()))
}

pub(crate) fn build_from_node(
    from: Option<&FromClause>,
    target: &ObjectName,
    table: &TableDef,
    resolved_name: &str,
) -> Option<FromNode> {
    let base = from.and_then(|f| f.tables.first().cloned()).or_else(|| {
        let factor = if from.is_some() && from.map(|f| f.tables.is_empty()).unwrap_or(true) {
            TableFactor::Named(target.clone())
        } else {
            TableFactor::Named(ObjectName {
                schema: Some(table.schema_or_dbo().to_string()),
                name: resolved_name.to_string(),
            })
        };
        Some(TableRef {
            factor,
            alias: None,
            pivot: None,
            unpivot: None,
            hints: Vec::new(),
        })
    })?;

    let mut node = FromNode::Table(base.clone());
    if let Some(from_clause) = from {
        for extra_table in from_clause.tables.iter().skip(1) {
            node = FromNode::Join {
                left: Box::new(node),
                join_type: crate::ast::JoinType::Cross,
                right: Box::new(FromNode::Table(extra_table.clone())),
                on: None,
            };
        }
        for join in &from_clause.joins {
            node = FromNode::Join {
                left: Box::new(node),
                join_type: join.join_type,
                right: Box::new(FromNode::Table(join.table.clone())),
                on: join.on.clone(),
            };
        }
    }

    Some(node)
}
