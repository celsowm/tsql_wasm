use crate::ast::*;
use crate::error::DbError;

use crate::parser::expression::parse_expr;
use crate::parser::utils::{find_keyword_top_level, parse_object_name, split_csv_top_level};

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
    let mut table_constraints = Vec::new();
    for raw_col in split_csv_top_level(body) {
        let item = raw_col.trim();
        if item.to_uppercase().starts_with("CONSTRAINT ") {
            table_constraints.push(parse_table_constraint(item)?);
        } else if item.to_uppercase().starts_with("PRIMARY KEY") {
            let after_pk = item["PRIMARY KEY".len()..].trim();
            let cols_raw = strip_wrapping_parens(after_pk);
            let columns_list = split_csv_top_level(cols_raw)
                .into_iter()
                .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
                .collect();
            table_constraints.push(TableConstraintSpec::PrimaryKey {
                name: String::new(),
                columns: columns_list,
            });
        } else if item.to_uppercase().starts_with("UNIQUE") {
            let after_uq = item["UNIQUE".len()..].trim();
            let cols_raw = strip_wrapping_parens(after_uq);
            let columns_list = split_csv_top_level(cols_raw)
                .into_iter()
                .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
                .collect();
            table_constraints.push(TableConstraintSpec::Unique {
                name: String::new(),
                columns: columns_list,
            });
        } else {
            columns.push(parse_column_spec(item)?);
        }
    }

    Ok(Statement::CreateTable(CreateTableStmt {
        name,
        columns,
        table_constraints,
    }))
}

pub(crate) fn parse_drop_table(sql: &str) -> Result<Statement, DbError> {
    let table_name = sql["DROP TABLE".len()..].trim();
    let name = parse_object_name(table_name);
    Ok(Statement::DropTable(DropTableStmt { name }))
}

pub(crate) fn parse_create_index(sql: &str) -> Result<Statement, DbError> {
    let after_prefix = sql["CREATE INDEX".len()..].trim();
    let on_idx = find_keyword_top_level(after_prefix, "ON")
        .ok_or_else(|| DbError::Parse("CREATE INDEX missing ON".into()))?;
    let index_name = parse_object_name(after_prefix[..on_idx].trim());
    let rest = after_prefix[on_idx + "ON".len()..].trim();

    let open = rest
        .find('(')
        .ok_or_else(|| DbError::Parse("CREATE INDEX missing '('".into()))?;
    let close = rest
        .rfind(')')
        .ok_or_else(|| DbError::Parse("CREATE INDEX missing ')'".into()))?;
    let table = parse_object_name(rest[..open].trim());
    let columns = split_csv_top_level(rest[open + 1..close].trim())
        .into_iter()
        .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(DbError::Parse(
            "CREATE INDEX requires at least one column".into(),
        ));
    }

    Ok(Statement::CreateIndex(CreateIndexStmt {
        name: index_name,
        table,
        columns,
    }))
}

pub(crate) fn parse_drop_index(sql: &str) -> Result<Statement, DbError> {
    let after_prefix = sql["DROP INDEX".len()..].trim();
    let on_idx = find_keyword_top_level(after_prefix, "ON")
        .ok_or_else(|| DbError::Parse("DROP INDEX missing ON".into()))?;
    let index_name = parse_object_name(after_prefix[..on_idx].trim());
    let table = parse_object_name(after_prefix[on_idx + "ON".len()..].trim());

    Ok(Statement::DropIndex(DropIndexStmt {
        name: index_name,
        table,
    }))
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

pub(crate) fn parse_create_view(sql: &str) -> Result<Statement, DbError> {
    let after_create = sql["CREATE VIEW".len()..].trim();
    let as_idx = find_keyword_top_level(after_create, "AS")
        .ok_or_else(|| DbError::Parse("CREATE VIEW missing AS".into()))?;

    let view_name = parse_object_name(after_create[..as_idx].trim());
    let query_sql = after_create[as_idx + "AS".len()..].trim();

    let query = match super::select::parse_select(query_sql)? {
        Statement::Select(s) => s,
        _ => return Err(DbError::Parse("VIEW body must be a SELECT".into())),
    };

    Ok(Statement::CreateView(CreateViewStmt {
        name: view_name,
        query,
    }))
}

pub(crate) fn parse_drop_view(sql: &str) -> Result<Statement, DbError> {
    let view_name = sql["DROP VIEW".len()..].trim();
    if view_name.is_empty() {
        return Err(DbError::Parse("DROP VIEW missing name".into()));
    }
    Ok(Statement::DropView(DropViewStmt {
        name: parse_object_name(view_name),
    }))
}

pub(crate) fn parse_create_type(sql: &str) -> Result<Statement, DbError> {
    let after = sql["CREATE TYPE".len()..].trim();
    let upper = after.to_uppercase();
    let as_idx = find_keyword_top_level(&upper, "AS")
        .ok_or_else(|| DbError::Parse("CREATE TYPE missing AS".into()))?;
    let name = parse_object_name(after[..as_idx].trim());
    let after_as = after[as_idx + "AS".len()..].trim();
    if !after_as.to_uppercase().starts_with("TABLE") {
        return Err(DbError::Parse("CREATE TYPE only supports AS TABLE".into()));
    }
    let open = after_as
        .find('(')
        .ok_or_else(|| DbError::Parse("CREATE TYPE ... AS TABLE missing '('".into()))?;
    let close = after_as
        .rfind(')')
        .ok_or_else(|| DbError::Parse("CREATE TYPE ... AS TABLE missing ')'".into()))?;
    let body = after_as[open + 1..close].trim();
    let mut columns = Vec::new();
    let mut table_constraints = Vec::new();
    for raw_col in split_csv_top_level(body) {
        let item = raw_col.trim();
        if item.to_uppercase().starts_with("CONSTRAINT ") {
            table_constraints.push(parse_table_constraint(item)?);
        } else if item.to_uppercase().starts_with("PRIMARY KEY") {
            let after_pk = item["PRIMARY KEY".len()..].trim();
            let cols_raw = strip_wrapping_parens(after_pk);
            let columns_list = split_csv_top_level(cols_raw)
                .into_iter()
                .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
                .collect();
            table_constraints.push(TableConstraintSpec::PrimaryKey {
                name: String::new(),
                columns: columns_list,
            });
        } else if item.to_uppercase().starts_with("UNIQUE") {
            let after_uq = item["UNIQUE".len()..].trim();
            let cols_raw = strip_wrapping_parens(after_uq);
            let columns_list = split_csv_top_level(cols_raw)
                .into_iter()
                .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
                .collect();
            table_constraints.push(TableConstraintSpec::Unique {
                name: String::new(),
                columns: columns_list,
            });
        } else {
            columns.push(parse_column_spec(item)?);
        }
    }
    Ok(Statement::CreateType(CreateTypeStmt {
        name,
        columns,
        table_constraints,
    }))
}

pub(crate) fn parse_drop_type(sql: &str) -> Result<Statement, DbError> {
    let name = parse_object_name(sql["DROP TYPE".len()..].trim());
    Ok(Statement::DropType(DropTypeStmt { name }))
}

pub(crate) fn parse_truncate_table(sql: &str) -> Result<Statement, DbError> {
    let table_name = sql["TRUNCATE TABLE".len()..].trim();
    let name = parse_object_name(table_name);
    Ok(Statement::TruncateTable(TruncateTableStmt { name }))
}

pub(crate) fn parse_alter_table(sql: &str) -> Result<Statement, DbError> {
    let after_table = sql["ALTER TABLE".len()..].trim();

    if let Some(add_idx) = find_keyword_top_level(after_table, "ADD") {
        let table_name = after_table[..add_idx].trim();
        let col_def = after_table[add_idx + "ADD".len()..].trim();

        if col_def.to_uppercase().starts_with("CONSTRAINT") {
            let constraint = parse_table_constraint(col_def)?;
            return Ok(Statement::AlterTable(AlterTableStmt {
                table: parse_object_name(table_name),
                action: AlterTableAction::AddConstraint(constraint),
            }));
        }

        let column = parse_column_spec(col_def)?;
        return Ok(Statement::AlterTable(AlterTableStmt {
            table: parse_object_name(table_name),
            action: AlterTableAction::AddColumn(column),
        }));
    }

    if let Some(drop_idx) = find_keyword_top_level(after_table, "DROP COLUMN") {
        let table_name = after_table[..drop_idx].trim();
        let col_name = after_table[drop_idx + "DROP COLUMN".len()..].trim();
        return Ok(Statement::AlterTable(AlterTableStmt {
            table: parse_object_name(table_name),
            action: AlterTableAction::DropColumn(col_name.to_string()),
        }));
    }

    if let Some(drop_idx) = find_keyword_top_level(after_table, "DROP CONSTRAINT") {
        let table_name = after_table[..drop_idx].trim();
        let constraint_name = after_table[drop_idx + "DROP CONSTRAINT".len()..].trim();
        return Ok(Statement::AlterTable(AlterTableStmt {
            table: parse_object_name(table_name),
            action: AlterTableAction::DropConstraint(constraint_name.to_string()),
        }));
    }

    Err(DbError::Parse(
        "ALTER TABLE only supports ADD column, ADD CONSTRAINT, DROP COLUMN, and DROP CONSTRAINT"
            .into(),
    ))
}

pub(crate) fn parse_with_cte(sql: &str) -> Result<Statement, DbError> {
    let after_with = sql["WITH".len()..].trim();
    let upper_after_with = after_with.to_uppercase();

    let (recursive, mut rest) = if upper_after_with.starts_with("RECURSIVE") {
        (true, after_with["RECURSIVE".len()..].trim().to_string())
    } else {
        (false, after_with.to_string())
    };

    let mut ctes = Vec::new();

    loop {
        let name_end = rest
            .find(|c: char| c.is_whitespace() || c == '(')
            .ok_or_else(|| DbError::Parse("expected CTE name after WITH".into()))?;
        let cte_name = rest[..name_end].trim().to_string();
        rest = rest[name_end..].trim().to_string();

        let upper_rest = rest.to_uppercase();
        if !upper_rest.starts_with("AS") {
            return Err(DbError::Parse("expected AS after CTE name".into()));
        }
        rest = rest[2..].trim().to_string();

        if !rest.starts_with('(') {
            return Err(DbError::Parse("expected '(' after AS".into()));
        }
        rest = rest[1..].trim().to_string();

        let (query_text, after_paren) = extract_paren_content(&rest)?;
        let query = super::super::parse_sql(&query_text)?;

        ctes.push(CteDef {
            name: cte_name,
            query,
        });

        rest = after_paren.trim().to_string();

        if rest.starts_with(',') {
            rest = rest[1..].trim().to_string();
            continue;
        }
        break;
    }

    let body = super::super::parse_sql(&rest)?;

    Ok(Statement::WithCte(WithCteStmt {
        recursive,
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

pub(crate) fn parse_column_spec(input: &str) -> Result<ColumnSpec, DbError> {
    use crate::parser::utils::tokenize_preserving_parens;

    let tokens = tokenize_preserving_parens(input);
    if tokens.len() < 2 {
        return Err(DbError::Parse(format!(
            "invalid column definition '{}'",
            input
        )));
    }

    let name = tokens[0].clone();

    if tokens.get(1).is_some_and(|t| t.eq_ignore_ascii_case("AS")) {
        let expr_tok = tokens
            .get(2)
            .ok_or_else(|| DbError::Parse("computed column missing expression".into()))?;
        let expr = parse_expr(strip_wrapping_parens(expr_tok))?;
        return Ok(ColumnSpec {
            name,
            data_type: DataTypeSpec::Int,
            nullable: true,
            primary_key: false,
            unique: false,
            identity: None,
            default: None,
            default_constraint_name: None,
            check: None,
            check_constraint_name: None,
            computed_expr: Some(expr),
            foreign_key: None,
        });
    }

    let mut tail_tokens = tokens[1..].to_vec();
    let data_type_token = tail_tokens.remove(0);
    let data_type = parse_data_type(&data_type_token)?;

    let mut nullable = true;
    let mut primary_key = false;
    let mut unique = false;
    let mut identity = None;
    let mut default = None;
    let mut default_constraint_name = None;
    let mut check = None;
    let mut check_constraint_name = None;
    let computed_expr = None;
    let mut foreign_key = None;

    let mut i = 0;
    while i < tail_tokens.len() {
        match tail_tokens[i].to_uppercase().as_str() {
            "CONSTRAINT" => {
                let cname = tail_tokens
                    .get(i + 1)
                    .ok_or_else(|| DbError::Parse("missing constraint name".into()))?
                    .to_string();
                let ctype = tail_tokens
                    .get(i + 2)
                    .ok_or_else(|| DbError::Parse("missing constraint type".into()))?
                    .to_uppercase();
                if ctype == "DEFAULT" {
                    let expr_tok = tail_tokens.get(i + 3).ok_or_else(|| {
                        DbError::Parse("missing expression after CONSTRAINT DEFAULT".into())
                    })?;
                    default = Some(parse_expr(expr_tok)?);
                    default_constraint_name = Some(cname);
                    i += 4;
                } else if ctype == "CHECK" {
                    let expr_tok = tail_tokens.get(i + 3).ok_or_else(|| {
                        DbError::Parse("missing expression after CONSTRAINT CHECK".into())
                    })?;
                    let normalized = strip_wrapping_parens(expr_tok);
                    check = Some(parse_expr(normalized)?);
                    check_constraint_name = Some(cname);
                    i += 4;
                } else {
                    return Err(DbError::Parse(format!(
                        "unsupported constraint type '{}'",
                        ctype
                    )));
                }
            }
            "NOT" => {
                if tail_tokens
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
                if tail_tokens
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
                let expr_tok = tail_tokens
                    .get(i + 1)
                    .ok_or_else(|| DbError::Parse("missing expression after DEFAULT".into()))?;
                default = Some(parse_expr(expr_tok)?);
                i += 2;
            }
            "CHECK" => {
                let expr_tok = tail_tokens
                    .get(i + 1)
                    .ok_or_else(|| DbError::Parse("missing expression after CHECK".into()))?;
                let normalized = strip_wrapping_parens(expr_tok);
                check = Some(parse_expr(normalized)?);
                i += 2;
            }
            tok if tok.starts_with("IDENTITY(") => {
                let inner = &tail_tokens[i]["IDENTITY(".len()..tail_tokens[i].len() - 1];
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
            "REFERENCES" => {
                let ref_tok = tail_tokens
                    .get(i + 1)
                    .ok_or_else(|| DbError::Parse("missing table after REFERENCES".into()))?;
                let ref_str = ref_tok.as_str();
                if let Some(open) = ref_str.find('(') {
                    let close = ref_str
                        .rfind(')')
                        .ok_or_else(|| DbError::Parse("missing ')' in REFERENCES".into()))?;
                    let ref_table = parse_object_name(&ref_str[..open]);
                    let ref_cols = split_csv_top_level(&ref_str[open + 1..close])
                        .into_iter()
                        .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
                        .collect();
                    foreign_key = Some(crate::ast::ForeignKeyRef {
                        referenced_table: ref_table,
                        referenced_columns: ref_cols,
                        on_delete: None,
                        on_update: None,
                    });
                } else {
                    let ref_table = parse_object_name(ref_str);
                    foreign_key = Some(crate::ast::ForeignKeyRef {
                        referenced_table: ref_table,
                        referenced_columns: vec![],
                        on_delete: None,
                        on_update: None,
                    });
                }
                i += 2;
            }
            _ => {
                return Err(DbError::Parse(format!(
                    "unexpected token '{}'",
                    tail_tokens[i]
                )))
            }
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
        default_constraint_name,
        check,
        check_constraint_name,
        computed_expr,
        foreign_key,
    })
}

fn strip_wrapping_parens(input: &str) -> &str {
    let trimmed = input.trim();
    if trimmed.starts_with('(') && trimmed.ends_with(')') && trimmed.len() > 1 {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    }
}

pub(crate) fn parse_table_constraint(input: &str) -> Result<TableConstraintSpec, DbError> {
    use crate::parser::utils::tokenize_preserving_parens;
    let tokens = tokenize_preserving_parens(input);
    if tokens.len() < 4 {
        return Err(DbError::Parse("invalid table constraint".into()));
    }
    if !tokens[0].eq_ignore_ascii_case("CONSTRAINT") {
        return Err(DbError::Parse("expected CONSTRAINT".into()));
    }
    let name = tokens[1].clone();
    if tokens[2].eq_ignore_ascii_case("DEFAULT") {
        if tokens.len() < 6 || !tokens[4].eq_ignore_ascii_case("FOR") {
            return Err(DbError::Parse(
                "table DEFAULT constraint must be: CONSTRAINT name DEFAULT expr FOR column".into(),
            ));
        }
        let expr = parse_expr(&tokens[3])?;
        let column = tokens[5].trim_matches('[').trim_matches(']').to_string();
        return Ok(TableConstraintSpec::Default { name, column, expr });
    }
    if tokens[2].eq_ignore_ascii_case("CHECK") {
        let expr_raw = strip_wrapping_parens(&tokens[3]);
        let expr = parse_expr(expr_raw)?;
        return Ok(TableConstraintSpec::Check { name, expr });
    }
    if tokens[2].eq_ignore_ascii_case("PRIMARY") {
        if tokens.len() < 5 || !tokens[3].eq_ignore_ascii_case("KEY") {
            return Err(DbError::Parse("expected KEY after PRIMARY".into()));
        }
        let cols_raw = strip_wrapping_parens(&tokens[4]);
        let columns = split_csv_top_level(cols_raw)
            .into_iter()
            .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
            .collect();
        return Ok(TableConstraintSpec::PrimaryKey { name, columns });
    }
    if tokens[2].eq_ignore_ascii_case("UNIQUE") {
        if tokens.len() < 4 {
            return Err(DbError::Parse("UNIQUE constraint missing columns".into()));
        }
        let cols_raw = strip_wrapping_parens(&tokens[3]);
        let columns = split_csv_top_level(cols_raw)
            .into_iter()
            .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
            .collect();
        return Ok(TableConstraintSpec::Unique { name, columns });
    }
    if tokens[2].eq_ignore_ascii_case("FOREIGN") {
        // CONSTRAINT name FOREIGN KEY (cols) REFERENCES table(ref_cols) [ON DELETE action] [ON UPDATE action]
        if tokens.len() < 7 || !tokens[3].eq_ignore_ascii_case("KEY") {
            return Err(DbError::Parse("expected KEY after FOREIGN".into()));
        }
        let cols_raw = strip_wrapping_parens(&tokens[4]);
        let columns = split_csv_top_level(cols_raw)
            .into_iter()
            .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
            .collect();

        if !tokens[5].eq_ignore_ascii_case("REFERENCES") {
            return Err(DbError::Parse("expected REFERENCES".into()));
        }

        let ref_part = &tokens[6];
        let open = ref_part
            .find('(')
            .ok_or_else(|| DbError::Parse("missing '(' in REFERENCES".into()))?;
        let close = ref_part
            .rfind(')')
            .ok_or_else(|| DbError::Parse("missing ')' in REFERENCES".into()))?;

        let ref_table_name = ref_part[..open].trim();
        let referenced_table = parse_object_name(ref_table_name);

        let ref_cols_raw = &ref_part[open + 1..close];
        let referenced_columns = split_csv_top_level(ref_cols_raw)
            .into_iter()
            .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
            .collect();

        let mut on_delete: Option<crate::ast::ReferentialAction> = None;
        let mut on_update: Option<crate::ast::ReferentialAction> = None;

        let mut i = 7;
        while i < tokens.len() {
            if tokens[i].eq_ignore_ascii_case("ON") && i + 1 < tokens.len() {
                if tokens[i + 1].eq_ignore_ascii_case("DELETE") {
                    if i + 2 < tokens.len() {
                        let action_token = if i + 3 < tokens.len()
                            && (tokens[i + 2].eq_ignore_ascii_case("NO")
                                || tokens[i + 2].eq_ignore_ascii_case("SET"))
                        {
                            format!("{} {}", tokens[i + 2], tokens[i + 3])
                        } else {
                            tokens[i + 2].clone()
                        };
                        on_delete = Some(parse_referential_action(&action_token)?);
                        i += if action_token.contains(' ') { 5 } else { 3 };
                        continue;
                    }
                } else if tokens[i + 1].eq_ignore_ascii_case("UPDATE") {
                    if i + 2 < tokens.len() {
                        let action_token = if i + 3 < tokens.len()
                            && (tokens[i + 2].eq_ignore_ascii_case("NO")
                                || tokens[i + 2].eq_ignore_ascii_case("SET"))
                        {
                            format!("{} {}", tokens[i + 2], tokens[i + 3])
                        } else {
                            tokens[i + 2].clone()
                        };
                        on_update = Some(parse_referential_action(&action_token)?);
                        i += if action_token.contains(' ') { 5 } else { 3 };
                        continue;
                    }
                }
            }
            i += 1;
        }

        return Ok(TableConstraintSpec::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        });
    }
    Err(DbError::Parse("unsupported table constraint".into()))
}

pub(crate) fn parse_create_trigger(sql: &str) -> Result<Statement, DbError> {
    let after_prefix = sql["CREATE TRIGGER".len()..].trim();
    let on_idx = find_keyword_top_level(after_prefix, "ON")
        .ok_or_else(|| DbError::Parse("CREATE TRIGGER missing ON".into()))?;
    let trigger_name = parse_object_name(after_prefix[..on_idx].trim());
    let rest = &after_prefix[on_idx + "ON".len()..].trim();

    let (is_instead_of, table_name_end_pos, event_pos) =
        if let Some(pos) = find_keyword_top_level(rest, "INSTEAD OF") {
            (true, pos, pos + "INSTEAD OF".len())
        } else if let Some(pos) = find_keyword_top_level(rest, "AFTER") {
            (false, pos, pos + "AFTER".len())
        } else if let Some(pos) = find_keyword_top_level(rest, "FOR") {
            (false, pos, pos + "FOR".len())
        } else {
            return Err(DbError::Parse(
                "CREATE TRIGGER expects AFTER, FOR, or INSTEAD OF".into(),
            ));
        };

    let table_name = parse_object_name(rest[..table_name_end_pos].trim());
    let after_event = &rest[event_pos..].trim();

    let as_idx = find_keyword_top_level(after_event, "AS")
        .ok_or_else(|| DbError::Parse("CREATE TRIGGER missing AS".into()))?;
    let event_text = after_event[..as_idx].trim().to_uppercase();
    let body_text = after_event[as_idx + "AS".len()..].trim();

    let mut events = Vec::new();
    for part in event_text.split(',') {
        match part.trim() {
            "INSERT" => events.push(TriggerEvent::Insert),
            "UPDATE" => events.push(TriggerEvent::Update),
            "DELETE" => events.push(TriggerEvent::Delete),
            _ => return Err(DbError::Parse(format!("invalid trigger event: {}", part))),
        }
    }

    let body_stmts = if body_text.to_uppercase().starts_with("BEGIN") {
        match super::procedural::parse_begin_end(body_text)? {
            Statement::BeginEnd(stmts) => stmts,
            other => vec![other],
        }
    } else {
        crate::parser::parse_batch(body_text)?
    };

    Ok(Statement::CreateTrigger(CreateTriggerStmt {
        name: trigger_name,
        table: table_name,
        events,
        is_instead_of,
        body: body_stmts,
    }))
}

pub(crate) fn parse_drop_trigger(sql: &str) -> Result<Statement, DbError> {
    let name = parse_object_name(sql["DROP TRIGGER".len()..].trim());
    Ok(Statement::DropTrigger(DropTriggerStmt { name }))
}

pub(crate) fn parse_data_type(input: &str) -> Result<DataTypeSpec, DbError> {
    let upper = input.to_uppercase();
    match upper.as_str() {
        "BIT" => Ok(DataTypeSpec::Bit),
        "TINYINT" => Ok(DataTypeSpec::TinyInt),
        "SMALLINT" => Ok(DataTypeSpec::SmallInt),
        "INT" => Ok(DataTypeSpec::Int),
        "BIGINT" => Ok(DataTypeSpec::BigInt),
        "FLOAT" => Ok(DataTypeSpec::Float),
        "REAL" => Ok(DataTypeSpec::Float),
        "MONEY" => Ok(DataTypeSpec::Money),
        "SMALLMONEY" => Ok(DataTypeSpec::SmallMoney),
        "DATE" => Ok(DataTypeSpec::Date),
        "TIME" => Ok(DataTypeSpec::Time),
        "DATETIME" => Ok(DataTypeSpec::DateTime),
        "DATETIME2" => Ok(DataTypeSpec::DateTime2),
        "UNIQUEIDENTIFIER" => Ok(DataTypeSpec::UniqueIdentifier),
        "SQL_VARIANT" => Ok(DataTypeSpec::SqlVariant),
        "DECIMAL" | "NUMERIC" => Ok(DataTypeSpec::Decimal(18, 0)),
        "BINARY" => Ok(DataTypeSpec::Binary(1)),
        "VARBINARY" => Ok(DataTypeSpec::VarBinary(8000)),
        _ => parse_parameterized_data_type(&upper),
    }
}

fn parse_parameterized_type(prefix: &str, upper: &str) -> Result<u16, DbError> {
    upper[prefix.len()..upper.len() - 1]
        .parse::<u16>()
        .map_err(|_| DbError::Parse(format!("invalid {} length", prefix.trim_end_matches('('))))
}

fn parse_decimal_params(upper: &str) -> Result<(u8, u8), DbError> {
    let open = upper.find('(').ok_or_else(|| DbError::Parse("DECIMAL/NUMERIC missing opening parenthesis".into()))?;
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

type DataTypeParser = fn(u16) -> DataTypeSpec;

fn parse_parameterized_data_type(upper: &str) -> Result<DataTypeSpec, DbError> {
    if (upper.starts_with("DECIMAL(") || upper.starts_with("NUMERIC(")) && upper.ends_with(')') {
        let (p, s) = parse_decimal_params(upper)?;
        return Ok(DataTypeSpec::Decimal(p, s));
    }

    if (upper.starts_with("FLOAT(")) && upper.ends_with(')') {
        return Ok(DataTypeSpec::Float);
    }

    if (upper.starts_with("BINARY(")) && upper.ends_with(')') {
        let n = parse_parameterized_type("BINARY(", upper)?;
        return Ok(DataTypeSpec::Binary(n));
    }

    if (upper.starts_with("VARBINARY(")) && upper.ends_with(')') {
        let n = parse_parameterized_type("VARBINARY(", upper)?;
        return Ok(DataTypeSpec::VarBinary(n));
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

fn parse_referential_action(token: &str) -> Result<crate::ast::ReferentialAction, DbError> {
    match token.to_uppercase().as_str() {
        "CASCADE" => Ok(crate::ast::ReferentialAction::Cascade),
        "SET_NULL" | "SET NULL" => Ok(crate::ast::ReferentialAction::SetNull),
        "SET_DEFAULT" | "SET DEFAULT" => Ok(crate::ast::ReferentialAction::SetDefault),
        "NO_ACTION" | "NO ACTION" => Ok(crate::ast::ReferentialAction::NoAction),
        _ => Err(DbError::Parse(format!(
            "invalid referential action '{}'",
            token
        ))),
    }
}
