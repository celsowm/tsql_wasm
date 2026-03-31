use crate::ast::*;
use crate::error::DbError;

use crate::parser::expression::{parse_expr, parse_expr_with_subqueries};
use crate::parser::statements::subquery_utils::{apply_subquery_map, extract_subqueries};
use crate::parser::utils::{
    find_keyword_top_level, parse_object_name, parse_table_ref, split_csv_top_level, tokenize_preserving_parens,
};

struct SelectClauseBounds {
    where_idx: Option<usize>,
    group_idx: Option<usize>,
    having_idx: Option<usize>,
    order_idx: Option<usize>,
    offset_idx: Option<usize>,
}

impl SelectClauseBounds {
    fn detect(tail: &str) -> Self {
        Self {
            where_idx: find_keyword_top_level(tail, "WHERE"),
            group_idx: find_keyword_top_level(tail, "GROUP BY"),
            having_idx: find_keyword_top_level(tail, "HAVING"),
            order_idx: find_keyword_top_level(tail, "ORDER BY"),
            offset_idx: find_keyword_top_level(tail, "OFFSET"),
        }
    }

    fn first_boundary(&self) -> Option<usize> {
        [
            self.where_idx,
            self.group_idx,
            self.having_idx,
            self.order_idx,
            self.offset_idx,
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

    let into_idx = find_keyword_top_level(select_rest, "INTO");
    let from_idx = find_keyword_top_level(select_rest, "FROM");

    let (into_table, select_rest_no_into) = if let Some(idx) = into_idx {
        if from_idx.map_or(true, |f| idx < f) {
            let after_into = &select_rest[idx + "INTO".len()..].trim();
            let end_of_into = find_keyword_top_level(after_into, "FROM").unwrap_or(after_into.len());
            let into_name = after_into[..end_of_into].trim();
            let name = parse_object_name(into_name);
            let reconstructed = format!("{} {}", &select_rest[..idx], &after_into[end_of_into..]);
            (Some(name), reconstructed)
        } else {
            (None, select_rest.to_string())
        }
    } else {
        (None, select_rest.to_string())
    };

    let from_idx = find_keyword_top_level(&select_rest_no_into, "FROM");

    let Some(from_idx) = from_idx else {
        if let Some(stmt) = try_parse_select_assign_no_from(&select_rest_no_into)? {
            return Ok(stmt);
        }
        let projection = parse_projection(select_rest_no_into.trim())?;
        return Ok(Statement::Select(SelectStmt {
            from: None,
            joins: vec![],
            applies: vec![],
            projection,
            into_table,
            distinct,
            top,
            selection: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            offset: None,
            fetch: None,
        }));
    };

    let projection_raw = select_rest_no_into[..from_idx].trim();
    let tail = select_rest_no_into[from_idx + "FROM".len()..].trim();

    if let Some(stmt) = try_parse_select_assign(projection_raw, tail)? {
        return Ok(stmt);
    }

    let bounds = SelectClauseBounds::detect(tail);
    let source_end = bounds.first_boundary().unwrap_or(tail.len());

    let (from, joins, applies) = parse_from_source_internal(tail[..source_end].trim())?;
    let selection = parse_where_clause(tail, &bounds)?;
    let group_by = parse_group_by_clause(tail, &bounds)?;
    let having = parse_having_clause(tail, &bounds)?;
    let order_by = parse_order_by_clause(tail, &bounds)?;
    let (offset, fetch) = parse_offset_fetch_clause(tail, &bounds)?;
    let projection = parse_projection(projection_raw)?;

    Ok(Statement::Select(SelectStmt {
        from: Some(from),
        joins,
        applies,
        projection,
        into_table,
        distinct,
        top,
        selection,
        group_by,
        having,
        order_by,
        offset,
        fetch,
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
    let (from, joins, _applies) = parse_from_source_internal(tail[..source_end].trim())?;
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
    let order_part = &tail[oidx + "ORDER BY".len()..];
    // Stop at OFFSET if present
    let end = bounds
        .offset_idx
        .map(|oi| oi - oidx - "ORDER BY".len())
        .unwrap_or(order_part.len());
    parse_order_by(order_part[..end].trim())
}

pub(crate) fn parse_from_source_internal(input: &str) -> Result<(TableRef, Vec<JoinClause>, Vec<ApplyClause>), DbError> {
    let mut rest = input.trim();
    let first_join = find_next_join_top_level(rest);
    let base = if let Some((idx, _, _)) = first_join {
        let base_raw = rest[..idx].trim();
        parse_table_ref(base_raw)?
    } else {
        return Ok((parse_table_ref(rest)?, vec![], vec![]));
    };

    let mut joins = Vec::new();
    let mut applies = Vec::new();
    while let Some((idx, join_type, join_len)) = find_next_join_top_level(rest) {
        let after_join = rest[idx + join_len..].trim();

        // CROSS APPLY / OUTER APPLY: expect a parenthesized subquery followed by alias
        if join_type == JoinType::Cross || join_type == JoinType::Left {
            // Check if this is an APPLY (keyword was CROSS APPLY or OUTER APPLY)
            let upper_rest = rest[idx..].to_uppercase();
            let is_apply = upper_rest.starts_with("CROSS APPLY") || upper_rest.starts_with("OUTER APPLY");
            if is_apply {
                let apply_type = if upper_rest.starts_with("CROSS APPLY") {
                    ApplyType::Cross
                } else {
                    ApplyType::Outer
                };
                // after_join starts after "CROSS APPLY" or "OUTER APPLY"
                let trimmed = after_join.trim();
                if trimmed.starts_with('(') {
                    // find matching close paren
                    let close = find_matching_paren(trimmed)
                        .ok_or_else(|| DbError::Parse("APPLY missing closing ')'".into()))?;
                    let subquery_sql = trimmed[1..close].trim();
                    let after_paren = trimmed[close + 1..].trim();
                    // parse alias: optional AS
                    let next = find_next_join_top_level(after_paren)
                        .map(|(i, _, _)| i)
                        .unwrap_or(after_paren.len());
                    let alias_str = after_paren[..next].trim();
                    let alias = if alias_str.to_uppercase().starts_with("AS ") {
                        alias_str[3..].trim()
                    } else {
                        alias_str
                    };
                    let alias = alias.trim_matches('[').trim_matches(']').to_string();
                    // Parse the subquery
                    let subquery_stmt = crate::parser::parse_sql(subquery_sql)?;
                    let subquery = match subquery_stmt {
                        Statement::Select(s) => s,
                        _ => return Err(DbError::Parse("APPLY requires a SELECT subquery".into())),
                    };
                    applies.push(ApplyClause {
                        apply_type,
                        subquery,
                        alias,
                    });
                    if next >= after_paren.len() {
                        break;
                    }
                    rest = after_paren[next..].trim();
                    continue;
                }
            }
        }

        if join_type == JoinType::Cross {
            // CROSS JOIN: no ON clause
            let next_join = find_next_join_top_level(after_join)
                .map(|(i, _, _)| i)
                .unwrap_or(after_join.len());
            let table_ref = parse_table_ref(after_join[..next_join].trim())?;
            joins.push(JoinClause {
                join_type: JoinType::Cross,
                table: table_ref,
                on: None,
            });
            if next_join >= after_join.len() {
                break;
            }
            rest = after_join[next_join..].trim();
            continue;
        }

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
            on: Some(on_expr),
        });

        if next_join >= after_on.len() {
            break;
        }
        rest = after_on[next_join..].trim();
    }

    Ok((base, joins, applies))
}

fn find_matching_paren(input: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut in_string = false;
    for (i, ch) in input.char_indices() {
        match ch {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

pub(crate) fn find_next_join_top_level(input: &str) -> Option<(usize, JoinType, usize)> {
    let patterns: &[(&str, JoinType)] = &[
        ("FULL OUTER JOIN", JoinType::Full),
        ("FULL JOIN", JoinType::Full),
        ("CROSS APPLY", JoinType::Cross),
        ("OUTER APPLY", JoinType::Left),
        ("CROSS JOIN", JoinType::Cross),
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
                        return Some((i, *ty, p.len()));
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
        let asc = !desc;
        out.push(OrderByExpr { expr, asc });
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

fn parse_offset_fetch_clause(
    tail: &str,
    bounds: &SelectClauseBounds,
) -> Result<(Option<Expr>, Option<Expr>), DbError> {
    let Some(oidx) = bounds.offset_idx else {
        return Ok((None, None));
    };

    let after_offset = &tail[oidx + "OFFSET".len()..].trim();
    let fetch_idx = find_keyword_top_level(after_offset, "FETCH");

    // Parse offset value: extract number before ROW/ROWS
    let offset_expr = if let Some(fi) = fetch_idx {
        let raw = after_offset[..fi].trim();
        parse_offset_value(raw)?
    } else {
        parse_offset_value(after_offset)?
    };

    let fetch_expr = if let Some(fi) = fetch_idx {
        let after_fetch = &after_offset[fi + "FETCH".len()..].trim();
        // NEXT <n> ROWS ONLY - skip NEXT
        let trimmed = after_fetch
            .trim_start_matches(|c: char| c.is_alphabetic())
            .trim();
        parse_offset_value(trimmed)?
    } else {
        None
    };

    Ok((offset_expr, fetch_expr))
}

fn parse_offset_value(input: &str) -> Result<Option<Expr>, DbError> {
    // Extract just the number, skipping ROW/ROWS/ONLY
    let raw = input.trim();
    let mut end = raw.len();
    for (i, ch) in raw.char_indices() {
        if ch.is_whitespace() {
            end = i;
            break;
        }
    }
    let num_str = &raw[..end];
    if num_str.is_empty() {
        return Ok(None);
    }
    Ok(Some(parse_expr(num_str)?))
}
