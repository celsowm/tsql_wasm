use serde_json::Value as JsonValue;

use crate::error::DbError;
use crate::types::Value;

pub fn parse_json(s: &str) -> Result<JsonValue, DbError> {
    serde_json::from_str(s)
        .map_err(|e| DbError::Execution(format!("Invalid JSON: {}", e)))
}

pub fn json_to_string(json: &JsonValue) -> String {
    serde_json::to_string(json).unwrap_or_else(|_| "null".to_string())
}

pub fn json_value(json_str: &str, path: &str) -> Result<Value, DbError> {
    let json = parse_json(json_str)?;
    let normalized_path = normalize_json_path(path);

    let result = json_value_at_path(&json, &normalized_path);
    match result {
        Some(v) => Ok(Value::NVarChar(v.to_string().trim_matches('"').to_string())),
        None => Ok(Value::Null),
    }
}

pub fn json_query(json_str: &str, path: &str) -> Result<Value, DbError> {
    let json = parse_json(json_str)?;
    let normalized_path = normalize_json_path(path);

    let result = json_value_at_path(&json, &normalized_path);
    match result {
        Some(v) => Ok(Value::NVarChar(json_to_string(v))),
        None => Ok(Value::Null),
    }
}

pub fn json_modify(json_str: &str, path: &str, new_value: &str) -> Result<Value, DbError> {
    let mut json = parse_json(json_str)?;
    let normalized_path = normalize_json_path(path);

    if let Ok(new_json) = parse_json(new_value) {
        set_json_value(&mut json, &normalized_path, new_json);
    } else {
        set_json_value(&mut json, &normalized_path, JsonValue::String(new_value.to_string()));
    }

    Ok(Value::NVarChar(json_to_string(&json)))
}

pub fn is_json(json_str: &str) -> Result<Value, DbError> {
    let is_valid = serde_json::from_str::<JsonValue>(json_str).is_ok();
    Ok(Value::Bit(is_valid))
}

pub fn json_array_length(json_str: &str) -> Result<Value, DbError> {
    let json = parse_json(json_str)?;
    match json {
        JsonValue::Array(arr) => Ok(Value::Int(arr.len() as i32)),
        _ => Ok(Value::Null),
    }
}

pub fn json_keys(json_str: &str, path: Option<&str>) -> Result<Value, DbError> {
    let json = parse_json(json_str)?;
    let target = if let Some(p) = path {
        let normalized = normalize_json_path(p);
        match json_value_at_path(&json, &normalized) {
            Some(v) => v.clone(),
            None => return Ok(Value::Null),
        }
    } else {
        json
    };

    match target {
        JsonValue::Object(map) => {
            let keys: Vec<String> = map.keys().map(|k| format!("\"{}\"", k)).collect();
            Ok(Value::NVarChar(format!("[{}]", keys.join(","))))
        }
        _ => Ok(Value::Null),
    }
}

pub fn json_extract(json_str: &str, path: &str) -> Result<Value, DbError> {
    let json = parse_json(json_str)?;
    let normalized_path = normalize_json_path(path);

    match json_value_at_path(&json, &normalized_path) {
        Some(v) => Ok(Value::NVarChar(json_to_string(v))),
        None => Ok(Value::Null),
    }
}

fn normalize_json_path(path: &str) -> String {
    path.trim_start_matches('$')
        .trim_start_matches('.')
        .to_string()
}

fn json_value_at_path<'a>(json: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path.is_empty() {
        return Some(json);
    }

    let parts: Vec<&str> = path.split('.').collect();
    let mut current = json;

    for part in parts {
        let (key, index) = parse_path_part(part);

        if let Some(idx) = index {
            match current {
                JsonValue::Array(arr) => {
                    if idx < arr.len() {
                        current = &arr[idx];
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        } else {
            match current {
                JsonValue::Object(map) => {
                    if let Some(val) = map.get(&key) {
                        current = val;
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
    }

    Some(current)
}

fn parse_path_part(part: &str) -> (String, Option<usize>) {
    if part.ends_with(']') {
        if let Some(bracket_pos) = part.find('[') {
            let key = part[..bracket_pos].to_string();
            let index_str = &part[bracket_pos + 1..part.len() - 1];
            if let Ok(idx) = index_str.parse::<usize>() {
                return (key, Some(idx));
            }
        }
    }
    (part.to_string(), None)
}

fn set_json_value(json: &mut JsonValue, path: &str, new_value: JsonValue) {
    if path.is_empty() {
        *json = new_value;
        return;
    }

    let parts: Vec<&str> = path.split('.').collect();
    let last_idx = parts.len() - 1;
    let mut current = json;

    for (i, part) in parts.iter().enumerate() {
        let (key, index) = parse_path_part(part);

        if i == last_idx {
            // Last part - set the value
            if let Some(idx) = index {
                if let JsonValue::Array(arr) = current {
                    if idx >= arr.len() {
                        arr.resize(idx + 1, JsonValue::Null);
                    }
                    arr[idx] = new_value.clone();
                }
            } else if let JsonValue::Object(map) = current {
                map.insert(key, new_value.clone());
            }
        } else {
            // Navigate to next level
            if let Some(idx) = index {
                if let JsonValue::Array(arr) = current {
                    if idx >= arr.len() {
                        arr.resize(idx + 1, JsonValue::Null);
                    }
                    current = &mut arr[idx];
                }
            } else if let JsonValue::Object(map) = current {
                current = if map.contains_key(&key) {
                    map.get_mut(&key).unwrap()
                } else {
                    let next_part = &parts[i + 1];
                    let (_, next_index) = parse_path_part(next_part);
                    let default = if next_index.is_some() {
                        JsonValue::Array(vec![])
                    } else {
                        JsonValue::Object(serde_json::Map::new())
                    };
                    map.entry(key).or_insert(default)
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_value_simple() {
        let json = r#"{"name": "test", "value": 42}"#;
        let result = json_value(json, "$.name").unwrap();
        assert_eq!(result, Value::NVarChar("test".to_string()));
    }

    #[test]
    fn test_json_query_array() {
        let json = r#"[1, 2, 3]"#;
        let result = json_query(json, "$[0]").unwrap();
        assert_eq!(result, Value::NVarChar("1".to_string()));
    }

    #[test]
    fn test_is_json() {
        assert_eq!(is_json(r#"{"valid": true}"#).unwrap(), Value::Bit(true));
        assert_eq!(is_json("not json").unwrap(), Value::Bit(false));
    }

    #[test]
    fn test_json_modify() {
        let json = r#"{"name": "old"}"#;
        let result = json_modify(json, "name", "new").unwrap();
        assert!(matches!(result, Value::NVarChar(_)));
    }
}
