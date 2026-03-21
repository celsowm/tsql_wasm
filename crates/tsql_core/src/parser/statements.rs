use crate::ast::*;
use crate::error::DbError;

use super::expression::parse_expr;
use super::utils::{
    find_keyword_top_level, parse_object_name, parse_table_ref, split_csv_top_level,
    tokenize_preserving_parens,
};

pub(crate) fn parse_create_table(sql: &str) -> Result<Statement, DbError> {
    let open = sql.find('(').ok_or_else(|| DbError::Parse("missing '('".into()))?;
    let close = sql.rfind(')').ok_or_else(|| DbError::Parse("missing ')'".into()))?;

    let head = sql[..open].trim();
    let body = sql[open + 1..close].trim();

    let table_name = head["CREATE TABLE".len()..].trim();
    let name = parse_object_name(table_name);

    let mut columns = Vec::new();
    for raw_col in split_csv_top_level(body) {
        columns.push(parse_column_spec(raw_col.trim())?);
    }

    Ok(Statement::CreateTable(CreateTableStmt { name, columns }))
}

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

pub(crate) fn parse_select(sql: &str) -> Result<Statement, DbError> {
    let after_select = sql["SELECT".len()..].trim();
    let (top, select_rest) = parse_optional_top(after_select)?;

    let from_idx = find_keyword_top_level(select_rest, "FROM")
        .ok_or_else(|| DbError::Parse("SELECT missing FROM".into()))?;

    let projection_raw = select_rest[..from_idx].trim();
    let tail = select_rest[from_idx + "FROM".len()..].trim();

    let where_idx = find_keyword_top_level(tail, "WHERE");
    let group_idx = find_keyword_top_level(tail, "GROUP BY");
    let order_idx = find_keyword_top_level(tail, "ORDER BY");

    let source_end = [where_idx, group_idx, order_idx]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(tail.len());

    let source_raw = tail[..source_end].trim();
    let (from, joins) = parse_from_source(source_raw)?;

    let selection = if let Some(widx) = where_idx {
        let end = [group_idx, order_idx]
            .into_iter()
            .flatten()
            .filter(|idx| *idx > widx)
            .min()
            .unwrap_or(tail.len());
        Some(parse_expr(tail[widx + "WHERE".len()..end].trim())?)
    } else {
        None
    };

    let group_by = if let Some(gidx) = group_idx {
        let end = order_idx.unwrap_or(tail.len());
        split_csv_top_level(tail[gidx + "GROUP BY".len()..end].trim())
            .into_iter()
            .map(|s| parse_expr(s.trim()))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![]
    };

    let order_by = if let Some(oidx) = order_idx {
        parse_order_by(tail[oidx + "ORDER BY".len()..].trim())?
    } else {
        vec![]
    };

    let projection = parse_projection(projection_raw)?;

    Ok(Statement::Select(SelectStmt {
        from,
        joins,
        projection,
        top,
        selection,
        group_by,
        order_by,
    }))
}

pub(crate) fn parse_update(sql: &str) -> Result<Statement, DbError> {
    let after_update = sql["UPDATE".len()..].trim();
    let set_idx = find_keyword_top_level(after_update, "SET")
        .ok_or_else(|| DbError::Parse("UPDATE missing SET".into()))?;

    let table = parse_object_name(after_update[..set_idx].trim());
    let tail = after_update[set_idx + "SET".len()..].trim();
    let where_idx = find_keyword_top_level(tail, "WHERE");

    let assignments_raw = if let Some(idx) = where_idx { &tail[..idx] } else { tail };
    let selection = if let Some(idx) = where_idx {
        Some(parse_expr(tail[idx + "WHERE".len()..].trim())?)
    } else {
        None
    };

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

    let selection = if let Some(idx) = where_idx {
        Some(parse_expr(after_delete[idx + "WHERE".len()..].trim())?)
    } else {
        None
    };

    Ok(Statement::Delete(DeleteStmt { table, selection }))
}

fn parse_column_spec(input: &str) -> Result<ColumnSpec, DbError> {
    let mut tokens = tokenize_preserving_parens(input);
    if tokens.len() < 2 {
        return Err(DbError::Parse(format!("invalid column definition '{}'", input)));
    }

    let name = tokens.remove(0);
    let data_type_token = tokens.remove(0);
    let data_type = parse_data_type(&data_type_token)?;

    let mut nullable = true;
    let mut primary_key = false;
    let mut identity = None;
    let mut default = None;

    let mut i = 0;
    while i < tokens.len() {
        let tok = tokens[i].to_uppercase();
        match tok.as_str() {
            "NOT" => {
                if i + 1 < tokens.len() && tokens[i + 1].eq_ignore_ascii_case("NULL") {
                    nullable = false;
                    i += 2;
                } else {
                    return Err(DbError::Parse("expected NULL after NOT".into()));
                }
            }
            "NULL" => {
                nullable = true;
                i += 1;
            }
            "PRIMARY" => {
                if i + 1 < tokens.len() && tokens[i + 1].eq_ignore_ascii_case("KEY") {
                    primary_key = true;
                    nullable = false;
                    i += 2;
                } else {
                    return Err(DbError::Parse("expected KEY after PRIMARY".into()));
                }
            }
            "DEFAULT" => {
                if i + 1 >= tokens.len() {
                    return Err(DbError::Parse("missing expression after DEFAULT".into()));
                }
                default = Some(parse_expr(&tokens[i + 1])?);
                i += 2;
            }
            _ if tok.starts_with("IDENTITY(") => {
                let inner = &tokens[i]["IDENTITY(".len()..tokens[i].len() - 1];
                let parts = inner.split(',').map(|s| s.trim()).collect::<Vec<_>>();
                if parts.len() != 2 {
                    return Err(DbError::Parse("IDENTITY expects 2 arguments".into()));
                }
                let seed = parts[0]
                    .parse::<i64>()
                    .map_err(|_| DbError::Parse("invalid IDENTITY seed".into()))?;
                let inc = parts[1]
                    .parse::<i64>()
                    .map_err(|_| DbError::Parse("invalid IDENTITY increment".into()))?;
                identity = Some((seed, inc));
                i += 1;
            }
            _ => return Err(DbError::Parse(format!("unexpected token '{}'", tokens[i]))),
        }
    }

    Ok(ColumnSpec {
        name,
        data_type,
        nullable,
        primary_key,
        identity,
        default,
    })
}

fn parse_data_type(input: &str) -> Result<DataTypeSpec, DbError> {
    let upper = input.to_uppercase();
    if upper == "BIT" {
        Ok(DataTypeSpec::Bit)
    } else if upper == "INT" {
        Ok(DataTypeSpec::Int)
    } else if upper == "BIGINT" {
        Ok(DataTypeSpec::BigInt)
    } else if upper == "DATETIME" {
        Ok(DataTypeSpec::DateTime)
    } else if upper.starts_with("VARCHAR(") && upper.ends_with(')') {
        let n = upper["VARCHAR(".len()..upper.len() - 1]
            .parse::<u16>()
            .map_err(|_| DbError::Parse("invalid VARCHAR length".into()))?;
        Ok(DataTypeSpec::VarChar(n))
    } else if upper.starts_with("NVARCHAR(") && upper.ends_with(')') {
        let n = upper["NVARCHAR(".len()..upper.len() - 1]
            .parse::<u16>()
            .map_err(|_| DbError::Parse("invalid NVARCHAR length".into()))?;
        Ok(DataTypeSpec::NVarChar(n))
    } else {
        Err(DbError::Parse(format!("unsupported data type '{}'", input)))
    }
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
        let on_expr = parse_expr(after_on[..next_join].trim())?;

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
        ("LEFT JOIN", JoinType::Left),
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
                    let next_ok = i + p.len() == bytes.len() || (bytes[i + p.len()] as char).is_whitespace();
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
        let expr = parse_expr(input[..idx].trim())?;
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

    Ok(SelectItem {
        expr: parse_expr(input)?,
        alias: None,
    })
}

fn parse_assignment(input: &str) -> Result<Assignment, DbError> {
    let eq_idx = input
        .find('=')
        .ok_or_else(|| DbError::Parse("SET assignment missing '='".into()))?;
    Ok(Assignment {
        column: input[..eq_idx].trim().to_string(),
        expr: parse_expr(input[eq_idx + 1..].trim())?,
    })
}

fn parse_order_by(input: &str) -> Result<Vec<OrderByExpr>, DbError> {
    let mut out = Vec::new();
    for item in split_csv_top_level(input) {
        let parts = tokenize_preserving_parens(item.trim());
        if parts.is_empty() {
            continue;
        }
        let desc = if parts.len() > 1 && parts[parts.len() - 1].eq_ignore_ascii_case("DESC") {
            true
        } else {
            false
        };

        let expr_text = if parts.len() > 1
            && (parts[parts.len() - 1].eq_ignore_ascii_case("DESC")
                || parts[parts.len() - 1].eq_ignore_ascii_case("ASC"))
        {
            parts[..parts.len() - 1].join(" ")
        } else {
            parts.join(" ")
        };

        out.push(OrderByExpr {
            expr: parse_expr(expr_text.trim())?,
            desc,
        });
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
        let close = rest.find(')').ok_or_else(|| DbError::Parse("TOP missing ')'".into()))?;
        let expr_text = &rest[1..close];
        let expr = parse_expr(expr_text.trim())?;
        rest = rest[close + 1..].trim_start();
        Ok((Some(TopSpec { value: expr }), rest))
    } else {
        let mut end = rest.len();
        for (idx, ch) in rest.char_indices() {
            if ch.is_whitespace() {
                end = idx;
                break;
            }
        }
        let expr = parse_expr(rest[..end].trim())?;
        rest = rest[end..].trim_start();
        Ok((Some(TopSpec { value: expr }), rest))
    }
}
