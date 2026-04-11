use crate::error::DbError;
use crate::types::{DataType, Value};

pub(crate) fn enforce_string_length(
    data_type: &DataType,
    value: &Value,
    col_name: &str,
) -> Result<(), DbError> {
    let max_len = match data_type {
        DataType::Char { len } | DataType::NChar { len } => Some(*len as usize),
        DataType::VarChar { max_len } | DataType::NVarChar { max_len } => Some(*max_len as usize),
        DataType::Binary { len } | DataType::VarBinary { max_len: len } => Some(*len as usize),
        _ => None,
    };

    if let Some(max) = max_len {
        let actual_len = match value {
            Value::Char(s) | Value::VarChar(s) | Value::NChar(s) | Value::NVarChar(s) => {
                Some(s.len())
            }
            Value::Binary(v) | Value::VarBinary(v) => Some(v.len()),
            _ => None,
        };
        if let Some(actual_len) = actual_len {
            if actual_len > max {
                return Err(DbError::Execution(format!(
                    "String or binary data would be truncated for column '{}'",
                    col_name
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn apply_ansi_padding(
    value: &mut Value,
    data_type: &DataType,
    ansi_padding_on: bool,
) {
    if ansi_padding_on {
        return;
    }

    match (data_type, value) {
        (DataType::VarChar { .. }, Value::VarChar(s))
        | (DataType::NVarChar { .. }, Value::NVarChar(s)) => {
            let trimmed = s.trim_end_matches(' ').to_string();
            *s = trimmed;
        }
        (DataType::VarBinary { .. }, Value::VarBinary(v)) => {
            while v.last().copied() == Some(0) {
                v.pop();
            }
        }
        _ => {}
    }
}
