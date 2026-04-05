use super::super::value_helpers::pad_right;
use super::coercion::coerce_value_to_type;
use crate::error::DbError;
use crate::types::{DataType, Value};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

pub fn convert_with_style(value: Value, ty: &DataType, style: i32) -> Result<Value, DbError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Date(v) => convert_date_to_string(v, ty, style),
        Value::Time(v) => convert_time_to_string(v, ty, style),
        Value::DateTime(v) | Value::DateTime2(v) => convert_datetime_to_string(v, ty, style),
        Value::VarChar(ref s)
        | Value::NVarChar(ref s)
        | Value::Char(ref s)
        | Value::NChar(ref s) => convert_string_to_datetime(s, ty, style),
        _ => coerce_value_to_type(value, ty),
    }
}

fn convert_date_to_string(d: NaiveDate, ty: &DataType, style: i32) -> Result<Value, DbError> {
    let formatted = format_date(&d, style);
    match ty {
        DataType::Char { len } => Ok(Value::Char(pad_right(&formatted, *len as usize))),
        DataType::VarChar { .. } => Ok(Value::VarChar(formatted)),
        DataType::NChar { len } => Ok(Value::NChar(pad_right(&formatted, *len as usize))),
        DataType::NVarChar { .. } => Ok(Value::NVarChar(formatted)),
        DataType::Date => Ok(Value::Date(d)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Date(d)))),
        _ => coerce_value_to_type(Value::VarChar(formatted), ty),
    }
}

fn convert_time_to_string(t: NaiveTime, ty: &DataType, style: i32) -> Result<Value, DbError> {
    let formatted = format_time(&t, style);
    match ty {
        DataType::Char { len } => Ok(Value::Char(pad_right(&formatted, *len as usize))),
        DataType::VarChar { .. } => Ok(Value::VarChar(formatted)),
        DataType::NChar { len } => Ok(Value::NChar(pad_right(&formatted, *len as usize))),
        DataType::NVarChar { .. } => Ok(Value::NVarChar(formatted)),
        DataType::Time => Ok(Value::Time(t)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Time(t)))),
        _ => coerce_value_to_type(Value::VarChar(formatted), ty),
    }
}

fn convert_datetime_to_string(
    dt: NaiveDateTime,
    ty: &DataType,
    style: i32,
) -> Result<Value, DbError> {
    let formatted = format_datetime(&dt, style);

    match ty {
        DataType::Char { len } => Ok(Value::Char(pad_right(&formatted, *len as usize))),
        DataType::VarChar { .. } => Ok(Value::VarChar(formatted)),
        DataType::NChar { len } => Ok(Value::NChar(pad_right(&formatted, *len as usize))),
        DataType::NVarChar { .. } => Ok(Value::NVarChar(formatted)),
        DataType::DateTime | DataType::DateTime2 => Ok(Value::DateTime(dt)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::DateTime(dt)))),
        _ => coerce_value_to_type(Value::VarChar(formatted), ty),
    }
}

pub fn format_date(d: &NaiveDate, style: i32) -> String {
    match style {
        101 => d.format("%m/%d/%Y").to_string(),
        103 => d.format("%d/%m/%Y").to_string(),
        120 => d.format("%Y-%m-%d").to_string(),
        121 => d.format("%Y-%m-%d").to_string(),
        _ => d.format("%Y-%m-%d").to_string(),
    }
}

pub fn format_time(t: &NaiveTime, style: i32) -> String {
    match style {
        108 | 114 => t.format("%H:%M:%S").to_string(),
        _ => t.format("%H:%M:%S%.f").to_string(),
    }
}

pub fn format_datetime(dt: &NaiveDateTime, style: i32) -> String {
    match style {
        0 => dt.format("%b %d %Y %I:%M%p").to_string(),
        1 => dt.format("%m/%d/%y").to_string(),
        2 => dt.format("%y.%m.%d").to_string(),
        3 => dt.format("%d/%m/%y").to_string(),
        4 => dt.format("%d.%m.%y").to_string(),
        5 => dt.format("%d-%m-%y").to_string(),
        6 => dt.format("%d %b %y").to_string(),
        7 => dt.format("%b %d, %y").to_string(),
        8 | 108 => dt.format("%H:%M:%S").to_string(),
        9 => dt.format("%b %d %Y %I:%M:%S:%f%p").to_string(),
        10 => dt.format("%m-%d-%y").to_string(),
        11 => dt.format("%y/%m/%d").to_string(),
        12 => dt.format("%y%m%d").to_string(),
        13 => dt.format("%d %b %Y %H:%M:%S:%f").to_string(),
        14 => dt.format("%H:%M:%S:%f").to_string(),
        20 | 120 => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        21 | 25 | 121 => dt.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        101 => dt.format("%m/%d/%Y").to_string(),
        102 => dt.format("%Y.%m.%d").to_string(),
        103 => dt.format("%d/%m/%Y").to_string(),
        104 => dt.format("%d.%m.%Y").to_string(),
        105 => dt.format("%d-%m-%Y").to_string(),
        106 => dt.format("%d %b %Y").to_string(),
        107 => dt.format("%b %d, %Y").to_string(),
        109 => dt.format("%b %d %Y %I:%M:%S:%f%p").to_string(),
        110 => dt.format("%m-%d-%Y").to_string(),
        111 => dt.format("%Y/%m/%d").to_string(),
        112 => dt.format("%Y%m%d").to_string(),
        113 => dt.format("%d %b %Y %H:%M:%S:%f").to_string(),
        126 => dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
        127 => dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
        130 => dt.format("%d %b %Y %H:%M:%S:%f").to_string(),
        131 => dt.format("%d/%m/%Y %H:%M:%S:%f").to_string(),
        _ => dt.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
    }
}

fn convert_string_to_datetime(s: &str, ty: &DataType, _style: i32) -> Result<Value, DbError> {
    match ty {
        DataType::Date => {
            let parsed = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .or_else(|_| NaiveDate::parse_from_str(s, "%m/%d/%Y"))
                .or_else(|_| NaiveDate::parse_from_str(s, "%d/%m/%Y"));
            match parsed {
                Ok(d) => Ok(Value::Date(d)),
                Err(_) => Err(DbError::Execution(format!("invalid date: {}", s))),
            }
        }
        DataType::Time => {
            let parsed = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"));
            match parsed {
                Ok(t) => Ok(Value::Time(t)),
                Err(_) => Err(DbError::Execution(format!("invalid time: {}", s))),
            }
        }
        DataType::DateTime | DataType::DateTime2 => {
            let parsed = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| NaiveDateTime::parse_from_str(s, "%m/%d/%Y %H:%M:%S"));
            match parsed {
                Ok(dt) => Ok(Value::DateTime(dt)),
                Err(_) => Err(DbError::Execution(format!("invalid datetime: {}", s))),
            }
        }
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(s.to_string())))),
        _ => coerce_value_to_type(Value::VarChar(s.to_string()), ty),
    }
}

pub fn normalize_datetime_string(s: &str) -> String {
    let date_time: Vec<&str> = s.splitn(2, |c: char| c.is_ascii_whitespace()).collect();
    let date_part = date_time[0];
    let time_part = date_time.get(1).unwrap_or(&"");
    let date_parts: Vec<&str> = date_part.split(|c: char| c == '-' || c == '/').collect();
    if date_parts.len() >= 3 {
        let y = date_parts[0].trim();
        let m = date_parts[1].trim();
        let d = date_parts[2].trim();
        if time_part.is_empty() {
            return format!("{}-{}-{}", y, m, d);
        }
        return format!("{}-{}-{} {}", y, m, d, time_part.trim());
    }
    s.to_string()
}

fn parse_dt_parts(dt: &str) -> (i32, i32, i32, i32, i32, i32) {
    let parts: Vec<&str> = dt
        .split(|c: char| c == '-' || c == '/' || c == ':')
        .collect();
    let y = parts
        .get(0)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    let mo = parts
        .get(1)
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(1);
    let d_and_t = parts.get(2).unwrap_or(&"1");
    let (d, rest) = if let Some(pos) = d_and_t.find(|c: char| c.is_ascii_whitespace()) {
        (&d_and_t[..pos], d_and_t[pos..].trim())
    } else {
        (*d_and_t, "")
    };
    let d = d.parse().unwrap_or(1);
    let (h, mi, s) = if !rest.is_empty() {
        let tparts: Vec<&str> = rest.split(':').collect();
        (
            tparts
                .get(0)
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0),
            tparts
                .get(1)
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0),
            tparts
                .get(2)
                .and_then(|s| s.trim().parse::<f64>().ok().map(|f| f as i32))
                .unwrap_or(0),
        )
    } else {
        (0, 0, 0)
    };
    (y, mo, d, h, mi, s)
}

fn month_abbr(m: i32) -> &'static str {
    match m {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "???",
    }
}

fn pad2(n: i32) -> String {
    format!("{:0>2}", n)
}

fn to_12hour(h: i32) -> (i32, &'static str) {
    let ampm = if h >= 12 { "PM" } else { "AM" };
    let h12 = match h {
        0 => 12,
        n if n > 12 => n - 12,
        _ => h,
    };
    (h12, ampm)
}
