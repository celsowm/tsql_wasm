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

    Err(DbError::Parse(
        "ALTER TABLE only supports ADD column and DROP COLUMN".into(),
    ))
}

pub(crate) fn parse_with_cte(sql: &str) -> Result<Statement, DbError> {
    let after_with = sql["WITH".len()..].trim();
    let mut ctes = Vec::new();
    let mut rest = after_with.to_string();

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
        let query = match crate::parser::statements::select::parse_select(&query_text)? {
            Statement::Select(s) => s,
            _ => return Err(DbError::Parse("CTE query must be a SELECT".into())),
        };

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
    if tokens[2].eq_ignore_ascii_case("FOREIGN") {
        // CONSTRAINT name FOREIGN KEY (cols) REFERENCES table(ref_cols)
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
        let open = ref_part.find('(').ok_or_else(|| DbError::Parse("missing '(' in REFERENCES".into()))?;
        let close = ref_part.rfind(')').ok_or_else(|| DbError::Parse("missing ')' in REFERENCES".into()))?;

        let ref_table_name = ref_part[..open].trim();
        let referenced_table = parse_object_name(ref_table_name);

        let ref_cols_raw = &ref_part[open + 1..close];
        let referenced_columns = split_csv_top_level(ref_cols_raw)
            .into_iter()
            .map(|s| s.trim().trim_matches('[').trim_matches(']').to_string())
            .collect();

        return Ok(TableConstraintSpec::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
        });
    }
    Err(DbError::Parse("unsupported table constraint".into()))
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
