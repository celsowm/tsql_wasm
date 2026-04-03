use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpan {
    pub start_line: usize,
    pub start_col: usize,
    pub end_line: usize,
    pub end_col: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementSlice {
    pub index: usize,
    pub sql: String,
    pub normalized_sql: String,
    pub span: SourceSpan,
}

pub fn split_sql_statements(sql: &str) -> Vec<StatementSlice> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_string = false;
    let mut paren_depth = 0usize;
    let mut block_depth = 0usize;
    let mut line = 1usize;
    let mut col = 1usize;
    let mut start_line = 1usize;
    let mut start_col = 1usize;
    let chars: Vec<char> = sql.chars().collect();
    let mut idx = 0usize;

    while idx < chars.len() {
        let ch = chars[idx];
        if buf.is_empty() && ch.is_whitespace() {
            advance_pos(ch, &mut line, &mut col);
            idx += 1;
            continue;
        }
        if buf.is_empty() {
            start_line = line;
            start_col = col;
        }

        if !in_string {
            if ch == '\'' {
                in_string = true;
            } else if ch == '(' {
                paren_depth += 1;
            } else if ch == ')' {
                paren_depth = paren_depth.saturating_sub(1);
            } else if idx + 4 < chars.len()
                && chars[idx..idx + 5]
                    .iter()
                    .collect::<String>()
                    .to_uppercase()
                    == "BEGIN"
            {
                block_depth += 1;
            } else if idx + 2 < chars.len()
                && chars[idx..idx + 3]
                    .iter()
                    .collect::<String>()
                    .to_uppercase()
                    == "END"
            {
                block_depth = block_depth.saturating_sub(1);
            } else if ch == ';' && paren_depth == 0 && block_depth == 0 {
                push_slice(
                    &mut out,
                    &mut buf,
                    start_line,
                    start_col,
                    line,
                    col,
                );
                advance_pos(ch, &mut line, &mut col);
                idx += 1;
                continue;
            } else if idx + 1 < chars.len()
                && chars[idx..idx + 2]
                    .iter()
                    .collect::<String>()
                    .to_uppercase()
                    == "GO"
                && (idx + 2 == chars.len() || chars[idx + 2].is_whitespace() || chars[idx + 2] == ';')
            {
                push_slice(
                    &mut out,
                    &mut buf,
                    start_line,
                    start_col,
                    line,
                    col,
                );
                idx += 2;
                col += 2;
                continue;
            }
        } else if ch == '\'' {
            if idx + 1 < chars.len() && chars[idx + 1] == '\'' {
                buf.push('\'');
                buf.push('\'');
                idx += 2;
                col += 2;
                continue;
            } else {
                in_string = false;
            }
        }

        buf.push(ch);
        advance_pos(ch, &mut line, &mut col);
        idx += 1;
    }

    if !buf.trim().is_empty() {
        push_slice(
            &mut out,
            &mut buf,
            start_line,
            start_col,
            line,
            col,
        );
    }

    out
}

fn advance_pos(ch: char, line: &mut usize, col: &mut usize) {
    if ch == '\n' {
        *line += 1;
        *col = 1;
    } else {
        *col += 1;
    }
}

fn push_slice(
    out: &mut Vec<StatementSlice>,
    buf: &mut String,
    start_line: usize,
    start_col: usize,
    end_line: usize,
    end_col: usize,
) {
    let sql = buf.trim().to_string();
    if !sql.is_empty() {
        out.push(StatementSlice {
            index: out.len(),
            normalized_sql: sql.to_uppercase(),
            sql,
            span: SourceSpan {
                start_line,
                start_col,
                end_line,
                end_col,
            },
        });
    }
    buf.clear();
}
