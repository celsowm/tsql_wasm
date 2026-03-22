use crate::ast::*;
use crate::error::DbError;

use crate::parser::expression::parse_expr;
use crate::parser::statements::subquery_utils::{apply_subquery_map, extract_subqueries};
use crate::parser::utils::{find_keyword_top_level, parse_object_name, split_csv_top_level};

pub(crate) fn parse_insert(sql: &str) -> Result<Statement, DbError> {
    let after_into = sql["INSERT INTO".len()..].trim();
    let upper = after_into.to_uppercase();

    if upper.ends_with("DEFAULT VALUES") {
        let table_name = after_into[..after_into.len() - "DEFAULT VALUES".len()].trim();
        return Ok(Statement::Insert(InsertStmt {
            table: parse_object_name(table_name),
            columns: None,
            values: vec![],
            default_values: true,
        }));
    }

    let values_idx = find_keyword_top_level(after_into, "VALUES")
        .ok_or_else(|| DbError::Parse("INSERT missing VALUES".into()))?;

    let head = after_into[..values_idx].trim();
    let values_part = after_into[values_idx + "VALUES".len()..].trim();

    let (table_name, columns) = if let Some(open) = head.find('(') {
        let close = head
            .rfind(')')
            .ok_or_else(|| DbError::Parse("missing ')' in INSERT columns".into()))?;
        let table_name = head[..open].trim();
        let cols = head[open + 1..close]
            .split(',')
            .map(|c| c.trim().to_string())
            .collect::<Vec<_>>();
        (table_name.to_string(), Some(cols))
    } else {
        (head.to_string(), None)
    };

    let table = parse_object_name(&table_name);
    let values = parse_values_groups(values_part)?;

    Ok(Statement::Insert(InsertStmt {
        table,
        columns,
        values,
        default_values: false,
    }))
}

pub(crate) fn parse_update(sql: &str) -> Result<Statement, DbError> {
    let after_update = sql["UPDATE".len()..].trim();
    let set_idx = find_keyword_top_level(after_update, "SET")
        .ok_or_else(|| DbError::Parse("UPDATE missing SET".into()))?;

    let table = parse_object_name(after_update[..set_idx].trim());
    let tail = after_update[set_idx + "SET".len()..].trim();
    let where_idx = find_keyword_top_level(tail, "WHERE");

    let assignments_raw = if let Some(idx) = where_idx {
        &tail[..idx]
    } else {
        tail
    };
    let selection = where_idx
        .map(|idx| parse_expr(tail[idx + "WHERE".len()..].trim()))
        .transpose()?;

    let assignments = split_csv_top_level(assignments_raw)
        .into_iter()
        .map(|part| parse_assignment(part.trim()))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Statement::Update(UpdateStmt {
        table,
        assignments,
        selection,
    }))
}

pub(crate) fn parse_delete(sql: &str) -> Result<Statement, DbError> {
    let after_delete = sql["DELETE FROM".len()..].trim();
    let where_idx = find_keyword_top_level(after_delete, "WHERE");

    let table = if let Some(idx) = where_idx {
        parse_object_name(after_delete[..idx].trim())
    } else {
        parse_object_name(after_delete)
    };

    let selection = where_idx
        .map(|idx| parse_expr(after_delete[idx + "WHERE".len()..].trim()))
        .transpose()?;

    Ok(Statement::Delete(DeleteStmt { table, selection }))
}

fn parse_assignment(input: &str) -> Result<Assignment, DbError> {
    let eq_idx = input
        .find('=')
        .ok_or_else(|| DbError::Parse("SET assignment missing '='".into()))?;
    let expr_raw = input[eq_idx + 1..].trim();
    let (processed, subquery_map) = extract_subqueries(expr_raw);
    let mut expr =
        crate::parser::expression::parse_expr_with_subqueries(&processed, &subquery_map)?;
    apply_subquery_map(&mut expr, &subquery_map);
    Ok(Assignment {
        column: input[..eq_idx].trim().to_string(),
        expr,
    })
}

fn parse_values_groups(input: &str) -> Result<Vec<Vec<Expr>>, DbError> {
    let mut out = Vec::new();
    let chars = input.chars().collect::<Vec<_>>();
    let mut i = 0usize;

    while i < chars.len() {
        while i < chars.len() && (chars[i].is_whitespace() || chars[i] == ',') {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        if chars[i] != '(' {
            return Err(DbError::Parse("expected '(' starting VALUES tuple".into()));
        }

        let start = i + 1;
        let mut depth = 1usize;
        let mut in_string = false;
        i += 1;
        while i < chars.len() && depth > 0 {
            match chars[i] {
                '\'' => in_string = !in_string,
                '(' if !in_string => depth += 1,
                ')' if !in_string => depth -= 1,
                _ => {}
            }
            i += 1;
        }

        if depth != 0 {
            return Err(DbError::Parse("unclosed VALUES tuple".into()));
        }

        let inner = &input[start..i - 1];
        let exprs = split_csv_top_level(inner)
            .into_iter()
            .map(|s| parse_expr(s.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        out.push(exprs);
    }

    Ok(out)
}
