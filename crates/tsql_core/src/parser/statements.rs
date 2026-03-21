use crate::ast::*;
use crate::error::DbError;

use super::expression::{parse_expr, parse_expr_with_subqueries};
use super::utils::{
    find_keyword_top_level, parse_object_name, parse_table_ref, split_csv_top_level,
    tokenize_preserving_parens,
};

use std::collections::HashMap;

// ─── Subquery extraction ────────────────────────────────────────────────

pub(crate) fn extract_subqueries(input: &str) -> (String, HashMap<String, SelectStmt>) {
    let mut map = HashMap::new();
    let mut counter = 0usize;
    let result = input.to_string();
    let upper = result.to_uppercase();
    let chars: Vec<char> = result.chars().collect();
    let upper_chars: Vec<char> = upper.chars().collect();

    // First pass: handle EXISTS (SELECT ...) and NOT EXISTS (SELECT ...)
    let mut i = 0;
    while i < chars.len() {
        // Check for EXISTS (
        if i + 6 <= chars.len() && upper_chars[i..i + 6] == ['E', 'X', 'I', 'S', 'T', 'S'] {
            let prev_ok = i == 0 || !chars[i - 1].is_ascii_alphanumeric();
            let next_ok = i + 6 >= chars.len() || !chars[i + 6].is_ascii_alphanumeric();
            if prev_ok && next_ok {
                let after_exists = result[i + 6..].trim_start();
                if after_exists.starts_with('(') {
                    let start_in_result = result.len() - after_exists.len();
                    if let Some((sql, _end)) = extract_paren_content_from(&chars, start_in_result) {
                        let upper_sql = sql.to_uppercase().trim().to_string();
                        if upper_sql.starts_with("SELECT") {
                            let placeholder = format!("__SUBQ_{}__", counter);
                            counter += 1;
                            if let Ok(Statement::Select(sel)) = parse_select(&sql) {
                                map.insert(placeholder.clone(), sel);
                                // Rebuild with EXISTS placeholder
                                let before: String = chars[..i].iter().collect();
                                let after_exists_str: String =
                                    chars[start_in_result..].iter().collect();
                                if let Some(paren_end) = find_matching_paren(&after_exists_str) {
                                    let new_expr = format!(
                                        "{}EXISTS {}{}",
                                        before,
                                        placeholder,
                                        &after_exists_str[paren_end + 1..]
                                    );
                                    return finalize_subquery_extraction(&new_expr, map, counter);
                                }
                            }
                        }
                    }
                }
            }
        }
        i += 1;
    }

    // Second pass: replace remaining (SELECT ...) patterns
    finalize_subquery_extraction(&result, map, counter)
}

fn find_matching_paren(input: &str) -> Option<usize> {
    let chars: Vec<char> = input.chars().collect();
    if chars.is_empty() || chars[0] != '(' {
        return None;
    }
    let mut depth = 1;
    let mut in_string = false;
    for i in 1..chars.len() {
        match chars[i] {
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

fn finalize_subquery_extraction(
    input: &str,
    mut map: HashMap<String, SelectStmt>,
    mut counter: usize,
) -> (String, HashMap<String, SelectStmt>) {
    let mut result = input.to_string();

    loop {
        let chars: Vec<char> = result.chars().collect();
        let mut replaced = false;
        let mut i = 0;

        while i < chars.len() {
            if chars[i] == '(' {
                if let Some((sql, end)) = extract_paren_content_from(&chars, i) {
                    let upper_sql = sql.to_uppercase().trim().to_string();
                    if upper_sql.starts_with("SELECT") {
                        let placeholder = format!("__SUBQ_{}__", counter);
                        counter += 1;
                        if let Ok(Statement::Select(sel)) = parse_select(&sql) {
                            map.insert(placeholder.clone(), sel);
                            let before: String = chars[..i].iter().collect();
                            let after: String = chars[end..].iter().collect();
                            result = format!("{}({}){}", before, placeholder, after);
                            replaced = true;
                            break;
                        }
                    }
                }
            }
            i += 1;
        }

        if !replaced {
            break;
        }
    }

    (result, map)
}

fn extract_paren_content_from(chars: &[char], start: usize) -> Option<(String, usize)> {
    if start >= chars.len() || chars[start] != '(' {
        return None;
    }
    let mut depth = 1usize;
    let mut in_string = false;
    let mut i = start + 1;

    while i < chars.len() {
        match chars[i] {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    let inner: String = chars[start + 1..i].iter().collect();
                    return Some((inner, i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

pub(crate) fn apply_subquery_map(expr: &mut Expr, _map: &HashMap<String, SelectStmt>) {
    // The subquery_map is already applied during parsing via parse_expr_with_subqueries.
    // This function is a placeholder for any post-processing if needed.
    match expr {
        Expr::InList { list, .. } => {
            for item in list.iter_mut() {
                apply_subquery_map(item, _map);
            }
        }
        Expr::Binary { left, right, .. } => {
            apply_subquery_map(left, _map);
            apply_subquery_map(right, _map);
        }
        Expr::Unary { expr: inner, .. } => {
            apply_subquery_map(inner, _map);
        }
        _ => {}
    }
}

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

pub(crate) fn parse_truncate_table(sql: &str) -> Result<Statement, DbError> {
    let table_name = sql["TRUNCATE TABLE".len()..].trim();
    let name = parse_object_name(table_name);
    Ok(Statement::TruncateTable(crate::ast::TruncateTableStmt {
        name,
    }))
}

pub(crate) fn parse_alter_table(sql: &str) -> Result<Statement, DbError> {
    let after_table = sql["ALTER TABLE".len()..].trim();

    if let Some(add_idx) = find_keyword_top_level(after_table, "ADD") {
        let table_name = after_table[..add_idx].trim();
        let col_def = after_table[add_idx + "ADD".len()..].trim();
        let column = parse_column_spec(col_def)?;
        return Ok(Statement::AlterTable(crate::ast::AlterTableStmt {
            table: parse_object_name(table_name),
            action: crate::ast::AlterTableAction::AddColumn(column),
        }));
    }

    if let Some(drop_idx) = find_keyword_top_level(after_table, "DROP COLUMN") {
        let table_name = after_table[..drop_idx].trim();
        let col_name = after_table[drop_idx + "DROP COLUMN".len()..].trim();
        return Ok(Statement::AlterTable(crate::ast::AlterTableStmt {
            table: parse_object_name(table_name),
            action: crate::ast::AlterTableAction::DropColumn(col_name.to_string()),
        }));
    }

    Err(DbError::Parse(
        "ALTER TABLE only supports ADD column and DROP COLUMN".into(),
    ))
}

pub(crate) fn parse_with_cte(sql: &str) -> Result<Statement, DbError> {
    let after_with = sql["WITH".len()..].trim();
    let mut ctes = Vec::new();
    let mut rest = after_with.to_string();

    loop {
        // Find CTE name
        let name_end = rest
            .find(|c: char| c.is_whitespace() || c == '(')
            .ok_or_else(|| DbError::Parse("expected CTE name after WITH".into()))?;
        let cte_name = rest[..name_end].trim().to_string();
        rest = rest[name_end..].trim().to_string();

        // Expect AS
        let upper_rest = rest.to_uppercase();
        if !upper_rest.starts_with("AS") {
            return Err(DbError::Parse("expected AS after CTE name".into()));
        }
        rest = rest[2..].trim().to_string();

        // Expect (
        if !rest.starts_with('(') {
            return Err(DbError::Parse("expected '(' after AS".into()));
        }
        rest = rest[1..].trim().to_string();

        // Find matching closing )
        let (query_text, after_paren) = extract_paren_content(&rest)?;
        let query = match parse_select(&query_text)? {
            Statement::Select(s) => s,
            _ => return Err(DbError::Parse("CTE query must be a SELECT".into())),
        };

        ctes.push(crate::ast::CteDef {
            name: cte_name,
            query,
        });

        rest = after_paren.trim().to_string();

        // Check for comma (more CTEs) or end of CTEs
        if rest.starts_with(',') {
            rest = rest[1..].trim().to_string();
            continue;
        }
        break;
    }

    // Parse the body statement
    let body = super::parse_sql(&rest)?;

    Ok(Statement::WithCte(crate::ast::WithCteStmt {
        ctes,
        body: Box::new(body),
    }))
}

fn extract_paren_content(input: &str) -> Result<(String, String), DbError> {
    let mut depth = 1usize;
    let mut in_string = false;
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '\'' => in_string = !in_string,
            '(' if !in_string => depth += 1,
            ')' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    let inner: String = chars[..i].iter().collect();
                    let rest: String = chars[i + 1..].iter().collect();
                    return Ok((inner, rest));
                }
            }
            _ => {}
        }
        i += 1;
    }

    Err(DbError::Parse("unclosed parenthesis in CTE".into()))
}

// ─── Procedural statements ─────────────────────────────────────────────

pub(crate) fn parse_declare(sql: &str) -> Result<Statement, DbError> {
    let after_declare = sql["DECLARE".len()..].trim();
    // DECLARE @name TYPE [= default]
    let at_pos = after_declare
        .find('@')
        .ok_or_else(|| DbError::Parse("DECLARE requires @variable name".into()))?;
    let rest = &after_declare[at_pos..];
    let name_end = rest
        .find(|c: char| c.is_whitespace() || c == '=')
        .unwrap_or(rest.len());
    let var_name = rest[..name_end].to_string();
    let after_name = rest[name_end..].trim();

    // Parse data type
    let (data_type_spec, after_type) = parse_type_from_declare(after_name)?;

    // Check for default value
    let default = if after_type.trim_start().starts_with('=') {
        let expr_str = after_type.trim_start()[1..].trim();
        Some(super::parse_expr(expr_str)?)
    } else {
        None
    };

    Ok(Statement::Declare(crate::ast::DeclareStmt {
        name: var_name,
        data_type: data_type_spec,
        default,
    }))
}

fn parse_type_from_declare(input: &str) -> Result<(crate::ast::DataTypeSpec, &str), DbError> {
    let trimmed = input.trim();
    let upper = trimmed.to_uppercase();

    // Check for parameterized types
    let types: &[(&str, fn(u16) -> crate::ast::DataTypeSpec)] = &[
        ("VARCHAR(", crate::ast::DataTypeSpec::VarChar),
        ("NVARCHAR(", crate::ast::DataTypeSpec::NVarChar),
        ("CHAR(", crate::ast::DataTypeSpec::Char),
        ("NCHAR(", crate::ast::DataTypeSpec::NChar),
    ];

    for (prefix, constructor) in types {
        if upper.starts_with(prefix) {
            let close = upper.find(')').ok_or_else(|| {
                DbError::Parse(format!("missing ')' for {}", prefix.trim_end_matches('(')))
            })?;
            let len_str = &upper[prefix.len()..close];
            let len: u16 = len_str
                .parse()
                .map_err(|_| DbError::Parse("invalid type length".into()))?;
            let rest = &trimmed[close + 1..];
            return Ok((constructor(len), rest));
        }
    }

    // Check for DECIMAL(p,s)
    if upper.starts_with("DECIMAL(") || upper.starts_with("NUMERIC(") {
        let close = upper
            .find(')')
            .ok_or_else(|| DbError::Parse("missing ')' for DECIMAL".into()))?;
        let inner = &upper[8..close];
        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
        let p: u8 = parts[0]
            .parse()
            .map_err(|_| DbError::Parse("invalid precision".into()))?;
        let s: u8 = if parts.len() > 1 {
            parts[1]
                .parse()
                .map_err(|_| DbError::Parse("invalid scale".into()))?
        } else {
            0
        };
        let rest = &trimmed[close + 1..];
        return Ok((crate::ast::DataTypeSpec::Decimal(p, s), rest));
    }

    // Simple types
    let simple_types: &[(&str, crate::ast::DataTypeSpec)] = &[
        ("BIT", crate::ast::DataTypeSpec::Bit),
        ("TINYINT", crate::ast::DataTypeSpec::TinyInt),
        ("SMALLINT", crate::ast::DataTypeSpec::SmallInt),
        ("INT", crate::ast::DataTypeSpec::Int),
        ("BIGINT", crate::ast::DataTypeSpec::BigInt),
        ("DATE", crate::ast::DataTypeSpec::Date),
        ("TIME", crate::ast::DataTypeSpec::Time),
        ("DATETIME", crate::ast::DataTypeSpec::DateTime),
        ("DATETIME2", crate::ast::DataTypeSpec::DateTime2),
        (
            "UNIQUEIDENTIFIER",
            crate::ast::DataTypeSpec::UniqueIdentifier,
        ),
        ("VARCHAR", crate::ast::DataTypeSpec::VarChar(8000)),
        ("NVARCHAR", crate::ast::DataTypeSpec::NVarChar(4000)),
        ("DECIMAL", crate::ast::DataTypeSpec::Decimal(18, 0)),
    ];

    for (name, spec) in simple_types {
        if upper.starts_with(name) {
            let after = &trimmed[name.len()..];
            let next_char = after.chars().next();
            if next_char.is_none()
                || next_char.unwrap().is_whitespace()
                || next_char.unwrap() == '='
                || next_char.unwrap() == ';'
            {
                return Ok((spec.clone(), after));
            }
        }
    }

    Err(DbError::Parse(format!(
        "unsupported data type in DECLARE: '{}'",
        trimmed
    )))
}

pub(crate) fn parse_set(sql: &str) -> Result<Statement, DbError> {
    let after_set = sql["SET".len()..].trim();
    let eq_pos = after_set
        .find('=')
        .ok_or_else(|| DbError::Parse("SET requires '=' assignment".into()))?;
    let var_name = after_set[..eq_pos].trim().to_string();
    let expr_str = after_set[eq_pos + 1..].trim();
    let (processed, subquery_map) = extract_subqueries(expr_str);
    let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
    apply_subquery_map(&mut expr, &subquery_map);
    Ok(Statement::Set(crate::ast::SetStmt {
        name: var_name,
        expr,
    }))
}

pub(crate) fn parse_if(sql: &str) -> Result<Statement, DbError> {
    let after_if = sql["IF".len()..].trim();

    // Find THEN-like boundary (we look for the first statement keyword)
    // In T-SQL, IF condition BEGIN...END ELSE BEGIN...END or IF condition statement
    // We look for BEGIN to find the body
    let begin_idx = find_keyword_top_level(after_if, "BEGIN");
    let else_idx = find_keyword_top_level(after_if, "ELSE");

    let (condition_str, body_str, else_str) = if let Some(bi) = begin_idx {
        let cond = after_if[..bi].trim();
        let else_pos = else_idx.filter(|&ei| ei > bi);
        let body = if let Some(ei) = else_pos {
            &after_if[bi..ei]
        } else {
            &after_if[bi..]
        };
        let else_body = else_pos.map(|ei| &after_if[ei + "ELSE".len()..]);
        (cond, body, else_body)
    } else if let Some(ei) = else_idx {
        let cond = after_if[..ei].trim();
        // Body is everything between condition and ELSE (a single statement)
        // For simplicity, treat as single statement
        let body_start = cond.len();
        let _remaining = &after_if[body_start..];
        // Actually this is tricky. Let me just support IF ... BEGIN...END ELSE BEGIN...END
        (
            cond,
            &after_if[body_start..ei],
            Some(&after_if[ei + "ELSE".len()..]),
        )
    } else {
        // Single statement: IF condition statement
        // For now, treat the whole thing as condition and use empty body
        // This is a simplification - ideally we'd parse the first expression as condition
        return Err(DbError::Parse(
            "IF requires BEGIN...END blocks (use: IF condition BEGIN ... END)".into(),
        ));
    };

    let condition = super::parse_expr(condition_str)?;
    let then_body = if body_str.trim().to_uppercase().starts_with("BEGIN") {
        parse_begin_end_body(body_str)?
    } else {
        super::parse_batch(body_str)?
    };

    let else_body = else_str
        .map(|s| {
            let s = s.trim();
            if s.to_uppercase().starts_with("BEGIN") {
                parse_begin_end_body(s)
            } else {
                super::parse_batch(s)
            }
        })
        .transpose()?;

    Ok(Statement::If(crate::ast::IfStmt {
        condition,
        then_body,
        else_body,
    }))
}

pub(crate) fn parse_while(sql: &str) -> Result<Statement, DbError> {
    let after_while = sql["WHILE".len()..].trim();
    let begin_idx = find_keyword_top_level(after_while, "BEGIN")
        .ok_or_else(|| DbError::Parse("WHILE requires BEGIN...END body".into()))?;
    let condition_str = after_while[..begin_idx].trim();
    let body_str = &after_while[begin_idx..];

    let condition = super::parse_expr(condition_str)?;
    let body = parse_begin_end_body(body_str)?;

    Ok(Statement::While(crate::ast::WhileStmt { condition, body }))
}

pub(crate) fn parse_begin_end(sql: &str) -> Result<Statement, DbError> {
    let body = parse_begin_end_body(sql)?;
    Ok(Statement::BeginEnd(body))
}

fn parse_begin_end_body(sql: &str) -> Result<Vec<Statement>, DbError> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    if !upper.starts_with("BEGIN") {
        return Err(DbError::Parse("expected BEGIN".into()));
    }
    let rest = trimmed["BEGIN".len()..].trim();
    // Find matching END
    let end_idx = find_matching_end(rest)?;
    let body_str = rest[..end_idx].trim();
    super::parse_batch(body_str)
}

fn find_matching_end(input: &str) -> Result<usize, DbError> {
    let upper = input.to_uppercase();
    let mut depth = 0usize;
    let mut in_string = false;
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '\'' => in_string = !in_string,
            _ if in_string => {}
            _ => {
                if i + 5 <= chars.len() && &upper[i..i + 5] == "BEGIN" {
                    depth += 1;
                    i += 5;
                    continue;
                }
                if i + 3 <= chars.len() && &upper[i..i + 3] == "END" {
                    if depth == 0 {
                        return Ok(i);
                    }
                    depth -= 1;
                    i += 3;
                    continue;
                }
            }
        }
        i += 1;
    }

    Err(DbError::Parse("missing END".into()))
}

pub(crate) fn parse_exec(sql: &str) -> Result<Statement, DbError> {
    let upper = sql.to_uppercase();
    let after_exec = if upper.starts_with("EXECUTE ") {
        sql["EXECUTE".len()..].trim()
    } else {
        sql["EXEC".len()..].trim()
    };
    let expr = super::parse_expr(after_exec)?;
    Ok(Statement::Exec(crate::ast::ExecStmt { sql_expr: expr }))
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
    let expr_str = tail[widx + "WHERE".len()..end].trim();
    let (processed, subquery_map) = extract_subqueries(expr_str);
    eprintln!(
        "DEBUG WHERE: input='{}' processed='{}' map={:?}",
        expr_str,
        processed,
        subquery_map.keys().collect::<Vec<_>>()
    );
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

fn parse_assignment(input: &str) -> Result<Assignment, DbError> {
    let eq_idx = input
        .find('=')
        .ok_or_else(|| DbError::Parse("SET assignment missing '='".into()))?;
    let expr_raw = input[eq_idx + 1..].trim();
    let (processed, subquery_map) = extract_subqueries(expr_raw);
    let mut expr = parse_expr_with_subqueries(&processed, &subquery_map)?;
    apply_subquery_map(&mut expr, &subquery_map);
    Ok(Assignment {
        column: input[..eq_idx].trim().to_string(),
        expr,
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
