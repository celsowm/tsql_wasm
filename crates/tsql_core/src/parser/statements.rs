use crate::ast::*;
use crate::error::DbError;

use super::expression::parse_expr;
use super::utils::{
    find_keyword_top_level, parse_object_name, parse_table_ref, split_csv_top_level,
    tokenize_preserving_parens,
};

// ─── DDL ────────────────────────────────────────────────────────────────

pub(crate) fn parse_create_table(sql: &str) -> Result<Statement, DbError> {
    let open = sql
        .find('(')
        .ok_or_else(|| DbError::Parse("missing '('".into()))?;
    let close = sql
        .rfind(')')
        .ok_or_else(|| DbError::Parse("missing ')'".into()))?;

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

pub(crate) fn parse_drop_table(sql: &str) -> Result<Statement, DbError> {
    let table_name = sql["DROP TABLE".len()..].trim();
    let name = parse_object_name(table_name);
    Ok(Statement::DropTable(DropTableStmt { name }))
}

pub(crate) fn parse_create_schema(sql: &str) -> Result<Statement, DbError> {
    let schema_name = sql["CREATE SCHEMA".len()..].trim();
    if schema_name.is_empty() {
        return Err(DbError::Parse("CREATE SCHEMA missing name".into()));
    }
    Ok(Statement::CreateSchema(CreateSchemaStmt {
        name: schema_name.to_string(),
    }))
}

pub(crate) fn parse_drop_schema(sql: &str) -> Result<Statement, DbError> {
    let schema_name = sql["DROP SCHEMA".len()..].trim();
    if schema_name.is_empty() {
        return Err(DbError::Parse("DROP SCHEMA missing name".into()));
    }
    Ok(Statement::DropSchema(DropSchemaStmt {
        name: schema_name.to_string(),
    }))
}

// ─── DML ────────────────────────────────────────────────────────────────

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

// ─── SELECT ─────────────────────────────────────────────────────────────

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
            .unwrap_or(0) // 0 means "end of string" — caller handles this
    }
}

pub(crate) fn parse_select(sql: &str) -> Result<Statement, DbError> {
    let after_select = sql["SELECT".len()..].trim();

    // Check for DISTINCT
    let (distinct, after_distinct) = if after_select.to_uppercase().starts_with("DISTINCT ") {
        (true, after_select["DISTINCT".len()..].trim())
    } else {
        (false, after_select)
    };

    let (top, select_rest) = parse_optional_top(after_distinct)?;

    let from_idx = find_keyword_top_level(select_rest, "FROM");

    if from_idx.is_none() {
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

fn parse_where_clause(tail: &str, bounds: &SelectClauseBounds) -> Result<Option<Expr>, DbError> {
    let Some(widx) = bounds.where_idx else {
        return Ok(None);
    };
    let end = bounds.next_after(widx);
    let end = if end == 0 { tail.len() } else { end };
    Ok(Some(parse_expr(tail[widx + "WHERE".len()..end].trim())?))
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
    Ok(Some(parse_expr(tail[hidx + "HAVING".len()..end].trim())?))
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

// ─── Column spec ────────────────────────────────────────────────────────

fn parse_column_spec(input: &str) -> Result<ColumnSpec, DbError> {
    let mut tokens = tokenize_preserving_parens(input);
    if tokens.len() < 2 {
        return Err(DbError::Parse(format!(
            "invalid column definition '{}'",
            input
        )));
    }

    let name = tokens.remove(0);
    let data_type_token = tokens.remove(0);
    let data_type = parse_data_type(&data_type_token)?;

    let mut nullable = true;
    let mut primary_key = false;
    let mut unique = false;
    let mut identity = None;
    let mut default = None;

    let mut i = 0;
    while i < tokens.len() {
        match tokens[i].to_uppercase().as_str() {
            "NOT" => {
                if tokens
                    .get(i + 1)
                    .is_some_and(|t| t.eq_ignore_ascii_case("NULL"))
                {
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
                if tokens
                    .get(i + 1)
                    .is_some_and(|t| t.eq_ignore_ascii_case("KEY"))
                {
                    primary_key = true;
                    unique = true;
                    nullable = false;
                    i += 2;
                } else {
                    return Err(DbError::Parse("expected KEY after PRIMARY".into()));
                }
            }
            "UNIQUE" => {
                unique = true;
                i += 1;
            }
            "DEFAULT" => {
                let expr_tok = tokens
                    .get(i + 1)
                    .ok_or_else(|| DbError::Parse("missing expression after DEFAULT".into()))?;
                default = Some(parse_expr(expr_tok)?);
                i += 2;
            }
            tok if tok.starts_with("IDENTITY(") => {
                let inner = &tokens[i]["IDENTITY(".len()..tokens[i].len() - 1];
                let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
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
        unique,
        identity,
        default,
    })
}

// ─── Data type parsing ──────────────────────────────────────────────────

fn parse_parameterized_type(prefix: &str, upper: &str) -> Result<u16, DbError> {
    upper[prefix.len()..upper.len() - 1]
        .parse::<u16>()
        .map_err(|_| DbError::Parse(format!("invalid {} length", prefix.trim_end_matches('('))))
}

fn parse_decimal_params(upper: &str) -> Result<(u8, u8), DbError> {
    let open = upper.find('(').unwrap();
    let inner = &upper[open + 1..upper.len() - 1];
    let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    if parts.len() != 2 {
        return Err(DbError::Parse(
            "DECIMAL/NUMERIC requires precision and scale".into(),
        ));
    }
    let p = parts[0]
        .parse::<u8>()
        .map_err(|_| DbError::Parse("invalid DECIMAL precision".into()))?;
    let s = parts[1]
        .parse::<u8>()
        .map_err(|_| DbError::Parse("invalid DECIMAL scale".into()))?;
    Ok((p, s))
}

fn parse_data_type(input: &str) -> Result<DataTypeSpec, DbError> {
    let upper = input.to_uppercase();
    match upper.as_str() {
        "BIT" => Ok(DataTypeSpec::Bit),
        "TINYINT" => Ok(DataTypeSpec::TinyInt),
        "SMALLINT" => Ok(DataTypeSpec::SmallInt),
        "INT" => Ok(DataTypeSpec::Int),
        "BIGINT" => Ok(DataTypeSpec::BigInt),
        "DATE" => Ok(DataTypeSpec::Date),
        "TIME" => Ok(DataTypeSpec::Time),
        "DATETIME" => Ok(DataTypeSpec::DateTime),
        "DATETIME2" => Ok(DataTypeSpec::DateTime2),
        "UNIQUEIDENTIFIER" => Ok(DataTypeSpec::UniqueIdentifier),
        "DECIMAL" | "NUMERIC" => Ok(DataTypeSpec::Decimal(18, 0)),
        _ => parse_parameterized_data_type(&upper),
    }
}

type DataTypeParser = fn(u16) -> DataTypeSpec;

fn parse_parameterized_data_type(upper: &str) -> Result<DataTypeSpec, DbError> {
    if (upper.starts_with("DECIMAL(") || upper.starts_with("NUMERIC(")) && upper.ends_with(')') {
        let (p, s) = parse_decimal_params(upper)?;
        return Ok(DataTypeSpec::Decimal(p, s));
    }

    let prefixes: &[(&str, DataTypeParser)] = &[
        ("VARCHAR(", DataTypeSpec::VarChar),
        ("NVARCHAR(", DataTypeSpec::NVarChar),
        ("CHAR(", DataTypeSpec::Char),
        ("NCHAR(", DataTypeSpec::NChar),
    ];

    for (prefix, constructor) in prefixes {
        if upper.starts_with(prefix) && upper.ends_with(')') {
            let n = parse_parameterized_type(prefix, upper)?;
            return Ok(constructor(n));
        }
    }

    Err(DbError::Parse(format!("unsupported data type '{}'", upper)))
}

// ─── Values ─────────────────────────────────────────────────────────────

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

// ─── FROM / JOIN ────────────────────────────────────────────────────────

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

// ─── Projection ─────────────────────────────────────────────────────────

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
        let desc = parts.len() > 1 && parts[parts.len() - 1].eq_ignore_ascii_case("DESC");

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
