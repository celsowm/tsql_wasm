use crate::ast::*;
use crate::error::DbError;

use crate::parser::expression::{parse_expr, parse_expr_with_subqueries};
use crate::parser::statements::subquery_utils::{apply_subquery_map, extract_subqueries};
use crate::parser::utils::{
    find_keyword_top_level, parse_table_ref, split_csv_top_level, tokenize_preserving_parens,
};

struct SelectClauseBounds {
    where_idx: Option<usize>,
    group_idx: Option<usize>,
    having_idx: Option<usize>,
    order_idx: Option<usize>,
}

impl SelectClauseBounds {
    fn detect(tail: &str) -> Self {
        Self {
            where_idx: find_keyword_top_level(tail, "WHERE"),
            group_idx: find_keyword_top_level(tail, "GROUP BY"),
            having_idx: find_keyword_top_level(tail, "HAVING"),
            order_idx: find_keyword_top_level(tail, "ORDER BY"),
        }
    }

    fn first_boundary(&self) -> Option<usize> {
        [
            self.where_idx,
            self.group_idx,
            self.having_idx,
            self.order_idx,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn next_after(&self, start: usize) -> usize {
        [self.group_idx, self.having_idx, self.order_idx]
            .into_iter()
            .flatten()
            .filter(|idx| *idx > start)
            .min()
            .unwrap_or(0)
    }
}

pub(crate) fn parse_select(sql: &str) -> Result<Statement, DbError> {
    let after_select = sql["SELECT".len()..].trim();

    let (distinct, after_distinct) = if after_select.to_uppercase().starts_with("DISTINCT ") {
        (true, after_select["DISTINCT".len()..].trim())
    } else {
        (false, after_select)
    };

    let (top, select_rest) = parse_optional_top(after_distinct)?;

    let from_idx = find_keyword_top_level(select_rest, "FROM");

    if from_idx.is_none() {
        if let Some(stmt) = try_parse_select_assign_no_from(select_rest)? {
            return Ok(stmt);
        }
        let projection = parse_projection(select_rest.trim())?;
        return Ok(Statement::Select(SelectStmt {
            from: None,
            joins: vec![],
            projection,
            distinct,
            top,
            selection: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
        }));
    }

    let from_idx = from_idx.unwrap();
    let projection_raw = select_rest[..from_idx].trim();
    let tail = select_rest[from_idx + "FROM".len()..].trim();

    if let Some(stmt) = try_parse_select_assign(projection_raw, tail)? {
        return Ok(stmt);
    }

    let bounds = SelectClauseBounds::detect(tail);
    let source_end = bounds.first_boundary().unwrap_or(tail.len());

    let (from, joins) = parse_from_source(tail[..source_end].trim())?;
    let selection = parse_where_clause(tail, &bounds)?;
    let group_by = parse_group_by_clause(tail, &bounds)?;
    let having = parse_having_clause(tail, &bounds)?;
    let order_by = parse_order_by_clause(tail, &bounds)?;
    let projection = parse_projection(projection_raw)?;

    Ok(Statement::Select(SelectStmt {
        from: Some(from),
        joins,
        projection,
        distinct,
        top,
        selection,
        group_by,
        having,
        order_by,
    }))
}

fn try_parse_select_assign(projection_raw: &str, tail: &str) -> Result<Option<Statement>, DbError> {
    let mut targets = Vec::new();
    for item in split_csv_top_level(projection_raw) {
        let eq = item.find('=');
        let Some(eq_idx) = eq else {
            return Ok(None);
        };
        let left = item[..eq_idx].trim();
        if !left.starts_with('@') {
            return Ok(None);
        }
        let right = item[eq_idx + 1..].trim();
        let (processed, subquery_map) = extract_subqueries(right);
        let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
        apply_subquery_map(&mut expr, &subquery_map);
        targets.push(SelectAssignTarget {
            variable: left.to_string(),
            expr,
        });
    }

    let bounds = SelectClauseBounds::detect(tail);
    let source_end = bounds.first_boundary().unwrap_or(tail.len());
    let (from, joins) = parse_from_source(tail[..source_end].trim())?;
    let selection = parse_where_clause(tail, &bounds)?;

    Ok(Some(Statement::SelectAssign(SelectAssignStmt {
        targets,
        from: Some(from),
        joins,
        selection,
    })))
}

fn try_parse_select_assign_no_from(select_rest: &str) -> Result<Option<Statement>, DbError> {
    let mut targets = Vec::new();
    for item in split_csv_top_level(select_rest) {
        let Some(eq_idx) = item.find('=') else {
            return Ok(None);
        };
        let left = item[..eq_idx].trim();
        if !left.starts_with('@') {
            return Ok(None);
        }
        let right = item[eq_idx + 1..].trim();
        let expr = parse_expr(right)?;
        targets.push(SelectAssignTarget {
            variable: left.to_string(),
            expr,
        });
    }
    Ok(Some(Statement::SelectAssign(SelectAssignStmt {
        targets,
        from: None,
        joins: vec![],
        selection: None,
    })))
}

fn parse_where_clause(tail: &str, bounds: &SelectClauseBounds) -> Result<Option<Expr>, DbError> {
    let Some(widx) = bounds.where_idx else {
        return Ok(None);
    };
    let end = bounds.next_after(widx);
    let end = if end == 0 { tail.len() } else { end };
    let expr_str = tail[widx + "WHERE".len()..end].trim();
    let (processed, subquery_map) = extract_subqueries(expr_str);
    let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
    apply_subquery_map(&mut expr, &subquery_map);
    Ok(Some(expr))
}

fn parse_group_by_clause(tail: &str, bounds: &SelectClauseBounds) -> Result<Vec<Expr>, DbError> {
    let Some(gidx) = bounds.group_idx else {
        return Ok(vec![]);
    };
    let end = bounds.next_after(gidx);
    let end = if end == 0 { tail.len() } else { end };
    split_csv_top_level(tail[gidx + "GROUP BY".len()..end].trim())
        .into_iter()
        .map(|s| parse_expr(s.trim()))
        .collect()
}

fn parse_having_clause(tail: &str, bounds: &SelectClauseBounds) -> Result<Option<Expr>, DbError> {
    let Some(hidx) = bounds.having_idx else {
        return Ok(None);
    };
    let end = bounds.order_idx.unwrap_or(tail.len());
    let expr_str = tail[hidx + "HAVING".len()..end].trim();
    let (processed, subquery_map) = extract_subqueries(expr_str);
    let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
    apply_subquery_map(&mut expr, &subquery_map);
    Ok(Some(expr))
}

fn parse_order_by_clause(
    tail: &str,
    bounds: &SelectClauseBounds,
) -> Result<Vec<OrderByExpr>, DbError> {
    let Some(oidx) = bounds.order_idx else {
        return Ok(vec![]);
    };
    parse_order_by(tail[oidx + "ORDER BY".len()..].trim())
}

fn parse_from_source(input: &str) -> Result<(TableRef, Vec<JoinClause>), DbError> {
    let mut rest = input.trim();
    let first_join = find_next_join_top_level(rest);
    let base = if let Some((idx, _, _)) = first_join {
        parse_table_ref(rest[..idx].trim())?
    } else {
        return Ok((parse_table_ref(rest)?, vec![]));
    };

    let mut joins = Vec::new();
    while let Some((idx, join_type, join_len)) = find_next_join_top_level(rest) {
        let after_join = rest[idx + join_len..].trim();
        let on_idx = find_keyword_top_level(after_join, "ON")
            .ok_or_else(|| DbError::Parse("JOIN missing ON".into()))?;

        let table_ref = parse_table_ref(after_join[..on_idx].trim())?;
        let after_on = after_join[on_idx + "ON".len()..].trim();
        let next_join = find_next_join_top_level(after_on)
            .map(|(i, _, _)| i)
            .unwrap_or(after_on.len());
        let on_expr_str = after_on[..next_join].trim();
        let (processed_on, on_subquery_map) = extract_subqueries(on_expr_str);
        let mut on_expr = parse_expr_with_subqueries(&processed_on, &on_subquery_map)?;
        apply_subquery_map(&mut on_expr, &on_subquery_map);

        joins.push(JoinClause {
            join_type,
            table: table_ref,
            on: on_expr,
        });

        if next_join >= after_on.len() {
            break;
        }
        rest = after_on[next_join..].trim();
    }

    Ok((base, joins))
}

fn find_next_join_top_level(input: &str) -> Option<(usize, JoinType, usize)> {
    let patterns = [
        ("FULL OUTER JOIN", JoinType::Full),
        ("FULL JOIN", JoinType::Full),
        ("LEFT JOIN", JoinType::Left),
        ("RIGHT JOIN", JoinType::Right),
        ("INNER JOIN", JoinType::Inner),
        ("JOIN", JoinType::Inner),
    ];

    let upper = input.to_uppercase();
    let bytes = upper.as_bytes();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut i = 0usize;

    while i < bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            '\'' => {
                in_string = !in_string;
                i += 1;
                continue;
            }
            '(' if !in_string => depth += 1,
            ')' if !in_string => depth = depth.saturating_sub(1),
            _ => {}
        }

        if !in_string && depth == 0 {
            for (pat, ty) in patterns {
                let p = pat.as_bytes();
                if i + p.len() <= bytes.len() && &bytes[i..i + p.len()] == p {
                    let prev_ok = i == 0 || (bytes[i - 1] as char).is_whitespace();
                    let next_ok =
                        i + p.len() == bytes.len() || (bytes[i + p.len()] as char).is_whitespace();
                    if prev_ok && next_ok {
                        return Some((i, ty, p.len()));
                    }
                }
            }
        }
        i += 1;
    }

    None
}

fn parse_projection(input: &str) -> Result<Vec<SelectItem>, DbError> {
    if input.trim() == "*" {
        return Ok(vec![SelectItem {
            expr: Expr::Wildcard,
            alias: None,
        }]);
    }

    split_csv_top_level(input)
        .into_iter()
        .map(|raw| parse_select_item(raw.trim()))
        .collect()
}

fn parse_select_item(input: &str) -> Result<SelectItem, DbError> {
    if input == "*" {
        return Ok(SelectItem {
            expr: Expr::Wildcard,
            alias: None,
        });
    }

    if let Some(idx) = find_keyword_top_level(input, "AS") {
        let expr_raw = input[..idx].trim();
        let (processed, subquery_map) = extract_subqueries(expr_raw);
        let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
        apply_subquery_map(&mut expr, &subquery_map);
        let alias = input[idx + "AS".len()..]
            .trim()
            .trim_matches('[')
            .trim_matches(']')
            .to_string();
        return Ok(SelectItem {
            expr,
            alias: Some(alias),
        });
    }

    let (processed, subquery_map) = extract_subqueries(input);
    let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
    apply_subquery_map(&mut expr, &subquery_map);
    Ok(SelectItem { expr, alias: None })
}

fn parse_order_by(input: &str) -> Result<Vec<OrderByExpr>, DbError> {
    let mut out = Vec::new();
    for item in split_csv_top_level(input) {
        let parts = tokenize_preserving_parens(item.trim());
        if parts.is_empty() {
            continue;
        }
        let desc = parts.len() > 1 && parts[parts.len() - 1].eq_ignore_ascii_case("DESC");

        let expr_text = if parts.len() > 1
            && (parts[parts.len() - 1].eq_ignore_ascii_case("DESC")
                || parts[parts.len() - 1].eq_ignore_ascii_case("ASC"))
        {
            parts[..parts.len() - 1].join(" ")
        } else {
            parts.join(" ")
        };

        let (processed, subquery_map) = extract_subqueries(expr_text.trim());
        let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
        apply_subquery_map(&mut expr, &subquery_map);
        out.push(OrderByExpr { expr, desc });
    }
    Ok(out)
}

fn parse_optional_top(input: &str) -> Result<(Option<TopSpec>, &str), DbError> {
    let trimmed = input.trim_start();
    if !trimmed.to_uppercase().starts_with("TOP") {
        return Ok((None, trimmed));
    }

    let mut rest = trimmed[3..].trim_start();
    if rest.starts_with('(') {
        let close = rest
            .find(')')
            .ok_or_else(|| DbError::Parse("TOP missing ')'".into()))?;
        let expr_text = &rest[1..close];
        let expr = parse_expr(expr_text.trim())?;
        rest = rest[close + 1..].trim_start();
        Ok((Some(TopSpec { value: expr }), rest))
    } else {
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let expr = parse_expr(rest[..end].trim())?;
        rest = rest[end..].trim_start();
        Ok((Some(TopSpec { value: expr }), rest))
    }
}
