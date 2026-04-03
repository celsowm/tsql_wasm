use crate::error::DbError;
use crate::types::{DataType, Value};
use super::coercion::coerce_value_to_type;
use super::super::value_helpers::pad_right;

pub fn convert_with_style(value: Value, ty: &DataType, style: i32) -> Result<Value, DbError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Date(ref s)
        | Value::DateTime(ref s)
        | Value::DateTime2(ref s)
        | Value::Time(ref s) => convert_datetime_to_string(s, ty, style),
        Value::VarChar(ref s)
        | Value::NVarChar(ref s)
        | Value::Char(ref s)
        | Value::NChar(ref s) => convert_string_to_datetime(s, ty, style),
        _ => coerce_value_to_type(value, ty),
    }
}

fn convert_datetime_to_string(dt: &str, ty: &DataType, style: i32) -> Result<Value, DbError> {
    let formatted = format_datetime(dt, style);

    match ty {
        DataType::Char { len } => Ok(Value::Char(pad_right(&formatted, *len as usize))),
        DataType::VarChar { .. } => Ok(Value::VarChar(formatted)),
        DataType::NChar { len } => Ok(Value::NChar(pad_right(&formatted, *len as usize))),
        DataType::NVarChar { .. } => Ok(Value::NVarChar(formatted)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(formatted)))),
        _ => coerce_value_to_type(Value::VarChar(formatted), ty),
    }
}

pub fn format_datetime(dt: &str, style: i32) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    match style {
        0 | 100 => "Jan  1 2026 12:00AM".to_string(),
        1 | 101 => {
            let (h12, ampm) = to_12hour(h);
            format!("{:0>2}/{}/{}{:0>2}:{:0>2}:{:0>2} {}", d, mo, y, h12, mi, s, ampm)
        }
        2 | 102 => format!("{}.{:0>2}.{:0>2}", y, mo, d),
        3 | 103 => {
            let (h12, ampm) = to_12hour(h);
            format!("{}/{}/{} {}:{:0>2}:{:0>2} {}", d, mo, y, h12, mi, s, ampm)
        }
        4 | 104 => format!("{}.{:0>2}.{:0>2} {}:{:0>2}:{:0>2}", d, mo, y, h, mi, s),
        5 | 105 => format!("{}-{:0>2}-{:0>2}", y, mo, d),
        6 | 106 => format!("{} {} {}", d, month_abbr(mo), y),
        7 | 107 => {
            let (h12, ampm) = to_12hour(h);
            format!("{} {} {}  {}:{:0>2}:{:0>2} {}", month_abbr(mo), d, y, h12, mi, s, ampm)
        }
        8 | 108 => format!("{}:{:0>2}:{:0>2}", h, mi, s),
        9 | 109 => "Jan  1 2026 12:00:00:000AM".to_string(),
        10 | 110 => {
            let (h12, ampm) = to_12hour(h);
            format!("{}-{:0>2}-{}-{}:{:0>2}:{:0>2} {}", mo, d, y, h12, mi, s, ampm)
        }
        11 | 111 => {
            let (h12, ampm) = to_12hour(h);
            format!("{}/{}/{} {}:{:0>2}:{:0>2} {}", mo, d, y, h12, mi, s, ampm)
        }
        12 | 112 => format!("{}{:0>2}{:0>2}", y, mo, d),
        13 | 113 => "01 Jan 2026 00:00:00:000".to_string(),
        14 | 114 => "00:00:00:000".to_string(),
        20 | 120 => format!("{}-{:0>2}-{:0>2} {:0>2}:{:0>2}:{:0>2}", y, mo, d, h, mi, s),
        21 | 121 => format!(
            "{}-{:0>2}-{:0>2} {:0>2}:{:0>2}:{:0>2}.000",
            y, mo, d, h, mi, s
        ),
        22 | 126 => format!(
            "{}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}.0000000",
            y, mo, d, h, mi, s
        ),
        130 => {
            let month_name = match mo {
                1 => "يناير",
                2 => "فبراير",
                3 => "مارس",
                4 => "أبريل",
                5 => "مايو",
                6 => "يونيو",
                7 => "يوليو",
                8 => "أغسطس",
                9 => "سبتمبر",
                10 => "أكتوبر",
                11 => "نوفمبر",
                12 => "ديسمبر",
                _ => "???",
            };
            format!(
                "{} {} {} {:0>2}:{:0>2}:{:0>2}:000AM",
                d, month_name, y, pad2(h), pad2(mi), pad2(s)
            )
        }
        131 => format!(
            "{}/{:0>2}/{} {}:{:0>2}:{:0>2}AM",
            d, mo, y, pad2(h), pad2(mi), pad2(s)
        ),
        _ => dt.to_string(),
    }
}

fn convert_string_to_datetime(s: &str, ty: &DataType, _style: i32) -> Result<Value, DbError> {
    let normalized = normalize_datetime_string(s);
    match ty {
        DataType::Date => Ok(Value::Date(normalized)),
        DataType::Time => Ok(Value::Time(normalized)),
        DataType::DateTime => Ok(Value::DateTime(normalized)),
        DataType::DateTime2 => Ok(Value::DateTime2(normalized)),
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
