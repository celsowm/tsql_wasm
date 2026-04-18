use super::super::value_helpers::pad_right;
use super::coercion::coerce_value_to_type;
use crate::error::DbError;
use crate::types::{DataType, Value};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

pub fn convert_with_style(
    value: Value,
    ty: &DataType,
    style: i32,
    dateformat: &str,
) -> Result<Value, DbError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Date(v) => convert_date_to_string(v, ty, style),
        Value::Time(v) => convert_time_to_string(v, ty, style),
        Value::DateTime(v) | Value::DateTime2(v) | Value::SmallDateTime(v) => {
            convert_datetime_to_string(v, ty, style)
        }
        Value::DateTimeOffset(ref s) => convert_string_to_datetime(s, ty, style, dateformat),
        Value::VarChar(ref s)
        | Value::NVarChar(ref s)
        | Value::Char(ref s)
        | Value::NChar(ref s) => convert_string_to_datetime(s, ty, style, dateformat),
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
        DataType::DateTime | DataType::DateTime2 | DataType::SmallDateTime => {
            Ok(Value::DateTime(dt))
        }
        DataType::DateTimeOffset => Ok(Value::DateTimeOffset(formatted)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::DateTime(dt)))),
        _ => coerce_value_to_type(Value::VarChar(formatted), ty),
    }
}

pub fn format_date(d: &NaiveDate, style: i32) -> String {
    match style {
        1 => d.format("%m/%d/%Y").to_string(),
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
        1 => dt.format("%m/%d/%Y").to_string(),
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

fn convert_string_to_datetime(
    s: &str,
    ty: &DataType,
    _style: i32,
    dateformat: &str,
) -> Result<Value, DbError> {
    match ty {
        DataType::Date => {
            let parsed = parse_date_string(s, dateformat);
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
        DataType::DateTime | DataType::DateTime2 | DataType::SmallDateTime => {
            let parsed = parse_datetime_string(s, dateformat);
            match parsed {
                Ok(dt) => Ok(Value::DateTime(dt)),
                Err(_) => Err(DbError::Execution(format!("invalid datetime: {}", s))),
            }
        }
        DataType::DateTimeOffset => Ok(Value::DateTimeOffset(s.to_string())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(s.to_string())))),
        _ => coerce_value_to_type(Value::VarChar(s.to_string()), ty),
    }
}

fn parse_date_string(v: &str, dateformat: &str) -> Result<NaiveDate, ()> {
    NaiveDate::parse_from_str(v, "%Y-%m-%d")
        .or_else(|_| NaiveDate::parse_from_str(v, "%m/%d/%Y"))
        .or_else(|_| NaiveDate::parse_from_str(v, "%d/%m/%Y"))
        .or_else(|_| NaiveDate::parse_from_str(v, "%m-%d-%Y"))
        .or_else(|_| NaiveDate::parse_from_str(v, "%d-%m-%Y"))
        .or_else(|_| NaiveDate::parse_from_str(v, "%Y/%m/%d"))
        .or_else(|_| {
            let fmt = match dateformat.to_ascii_lowercase().as_str() {
                "dmy" => "%d/%m/%Y",
                "ymd" => "%Y/%m/%d",
                "ydm" => "%Y/%d/%m",
                "myd" => "%m/%Y/%d",
                "dym" => "%d/%Y/%m",
                _ => "%m/%d/%Y",
            };
            NaiveDate::parse_from_str(v, fmt)
        })
        .map_err(|_| ())
}

pub(crate) fn parse_datetime_string(v: &str, dateformat: &str) -> Result<NaiveDateTime, ()> {
    NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(v, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(v, "%m/%d/%Y %H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(v, "%d/%m/%Y %H:%M:%S"))
        .or_else(|_| parse_date_string(v, dateformat).map(|d| d.and_hms_opt(0, 0, 0).unwrap()))
        .map_err(|_| ())
}
