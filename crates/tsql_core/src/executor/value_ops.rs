use std::cmp::Ordering;

use crate::error::DbError;
use crate::executor::value_helpers::to_decimal_parts;
use crate::types::{DataType, Value};

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
    let _is_date_like = dt.contains(' ') || dt.contains('-') || dt.contains('/');

    let formatted = match style {
        0 | 100 => "Jan  1 2026 12:00AM".to_string(),
        1 | 101 => format_datetime_style1(dt),
        2 | 102 => format_datetime_style2(dt),
        3 | 103 => format_datetime_style3(dt),
        4 | 104 => format_datetime_style4(dt),
        5 | 105 => format_datetime_style5(dt),
        6 | 106 => format_datetime_style6(dt),
        7 | 107 => format_datetime_style7(dt),
        8 | 108 => format_datetime_style8(dt),
        9 | 109 => "Jan  1 2026 12:00:00:000AM".to_string(),
        10 | 110 => format_datetime_style10(dt),
        11 | 111 => format_datetime_style11(dt),
        12 | 112 => format_datetime_style12(dt),
        13 | 113 => "01 Jan 2026 00:00:00:000".to_string(),
        14 | 114 => "00:00:00:000".to_string(),
        20 | 120 => format_datetime_style20(dt),
        21 | 121 => format_datetime_style21(dt),
        22 | 126 => format_datetime_style126(dt),
        130 => format_datetime_style130(dt),
        131 => format_datetime_style131(dt),
        _ => dt.to_string(),
    };

    match ty {
        DataType::Char { len } => Ok(Value::Char(pad_right(&formatted, *len as usize))),
        DataType::VarChar { .. } => Ok(Value::VarChar(formatted)),
        DataType::NChar { len } => Ok(Value::NChar(pad_right(&formatted, *len as usize))),
        DataType::NVarChar { .. } => Ok(Value::NVarChar(formatted)),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(formatted)))),
        _ => coerce_value_to_type(Value::VarChar(formatted), ty),
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

fn normalize_datetime_string(s: &str) -> String {
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

fn format_datetime_style1(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    let ampm = if h >= 12 { "PM" } else { "AM" };
    let h12 = if h == 0 {
        12
    } else if h > 12 {
        h - 12
    } else {
        h
    };
    format!(
        "{:0>2}/{}/{}{:0>2}:{:0>2}:{:0>2} {}",
        d, mo, y, h12, mi, s, ampm
    )
}

fn format_datetime_style2(dt: &str) -> String {
    let (y, mo, d, _, _, _) = parse_dt_parts(dt);
    format!("{}.{:0>2}.{:0>2}", y, mo, d)
}

fn format_datetime_style3(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    let ampm = if h >= 12 { "PM" } else { "AM" };
    let h12 = if h == 0 {
        12
    } else if h > 12 {
        h - 12
    } else {
        h
    };
    format!("{}/{}/{} {}:{:0>2}:{:0>2} {}", d, mo, y, h12, mi, s, ampm)
}

fn format_datetime_style4(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    format!("{}.{:0>2}.{:0>2} {}:{:0>2}:{:0>2}", d, mo, y, h, mi, s)
}

fn format_datetime_style5(dt: &str) -> String {
    let (y, mo, d, _, _, _) = parse_dt_parts(dt);
    format!("{}-{:0>2}-{:0>2}", y, mo, d)
}

fn format_datetime_style6(dt: &str) -> String {
    let (y, mo, d, _, _, _) = parse_dt_parts(dt);
    format!("{} {} {}", d, month_abbr(mo), y)
}

fn format_datetime_style7(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    let ampm = if h >= 12 { "PM" } else { "AM" };
    let h12 = if h == 0 {
        12
    } else if h > 12 {
        h - 12
    } else {
        h
    };
    format!(
        "{} {} {}  {}:{:0>2}:{:0>2} {}",
        month_abbr(mo),
        d,
        y,
        h12,
        mi,
        s,
        ampm
    )
}

fn format_datetime_style8(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    let _ = (y, mo, d);
    format!("{}:{:0>2}:{:0>2}", h, mi, s)
}

fn format_datetime_style10(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    let ampm = if h >= 12 { "PM" } else { "AM" };
    let h12 = if h == 0 {
        12
    } else if h > 12 {
        h - 12
    } else {
        h
    };
    format!(
        "{}-{:0>2}-{}-{}:{:0>2}:{:0>2} {}",
        mo, d, y, h12, mi, s, ampm
    )
}

fn format_datetime_style11(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    let ampm = if h >= 12 { "PM" } else { "AM" };
    let h12 = if h == 0 {
        12
    } else if h > 12 {
        h - 12
    } else {
        h
    };
    format!("{}/{}/{} {}:{:0>2}:{:0>2} {}", mo, d, y, h12, mi, s, ampm)
}

fn format_datetime_style12(dt: &str) -> String {
    let (y, mo, d, _, _, _) = parse_dt_parts(dt);
    format!("{}{:0>2}{:0>2}", y, mo, d)
}

fn format_datetime_style20(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    format!("{}-{:0>2}-{:0>2} {:0>2}:{:0>2}:{:0>2}", y, mo, d, h, mi, s)
}

fn format_datetime_style21(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    format!(
        "{}-{:0>2}-{:0>2} {:0>2}:{:0>2}:{:0>2}.000",
        y, mo, d, h, mi, s
    )
}

fn format_datetime_style126(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    format!(
        "{}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}.0000000",
        y, mo, d, h, mi, s
    )
}

fn format_datetime_style130(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
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
        d,
        month_name,
        y,
        pad2(h),
        pad2(mi),
        pad2(s)
    )
}

fn format_datetime_style131(dt: &str) -> String {
    let (y, mo, d, h, mi, s) = parse_dt_parts(dt);
    format!(
        "{}/{:0>2}/{} {}:{:0>2}:{:0>2}AM",
        d,
        mo,
        y,
        pad2(h),
        pad2(mi),
        pad2(s)
    )
}

pub fn coerce_value_to_type(value: Value, ty: &DataType) -> Result<Value, DbError> {
    if matches!(ty, DataType::SqlVariant) {
        return Ok(match value {
            Value::Null => Value::Null,
            Value::SqlVariant(inner) => Value::SqlVariant(inner),
            other => Value::SqlVariant(Box::new(other)),
        });
    }

    let value = match value {
        Value::SqlVariant(inner) => *inner,
        other => other,
    };

    match value {
        Value::Null => Ok(Value::Null),
        Value::Bit(v) => coerce_bit(v, ty),
        Value::TinyInt(v) => coerce_int(v as i64, ty),
        Value::SmallInt(v) => coerce_int(v as i64, ty),
        Value::Int(v) => coerce_int(v as i64, ty),
        Value::BigInt(v) => coerce_int(v, ty),
        Value::Decimal(raw, scale) => coerce_decimal(raw, scale, ty),
        Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => {
            coerce_string(&v, ty)
        }
        Value::Date(v) => coerce_date_time_string(&v, ty),
        Value::Time(v) => coerce_date_time_string(&v, ty),
        Value::DateTime(v) => coerce_date_time_string(&v, ty),
        Value::DateTime2(v) => coerce_date_time_string(&v, ty),
        Value::UniqueIdentifier(v) => coerce_uuid(&v, ty),
        Value::SqlVariant(inner) => coerce_value_to_type(*inner, ty),
    }
}

fn coerce_bit(v: bool, ty: &DataType) -> Result<Value, DbError> {
    let int_val: i64 = if v { 1 } else { 0 };
    match ty {
        DataType::Bit => Ok(Value::Bit(v)),
        DataType::TinyInt => Ok(Value::TinyInt(int_val as u8)),
        DataType::SmallInt => Ok(Value::SmallInt(int_val as i16)),
        DataType::Int => Ok(Value::Int(int_val as i32)),
        DataType::BigInt => Ok(Value::BigInt(int_val)),
        DataType::Decimal { scale, .. } => Ok(Value::Decimal(int_val as i128, *scale)),
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(int_val.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(int_val.to_string()))
        }
        DataType::DateTime | DataType::DateTime2 | DataType::Date | DataType::Time => Err(
            DbError::Execution(format!("cannot convert bit to {:?}", ty)),
        ),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert bit to UNIQUEIDENTIFIER".into(),
        )),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Bit(v)))),
    }
}

fn coerce_int(v: i64, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Bit => Ok(Value::Bit(v != 0)),
        DataType::TinyInt => {
            if !(0..=255).contains(&v) {
                Err(DbError::Execution(format!(
                    "Arithmetic overflow error converting value {} to TINYINT",
                    v
                )))
            } else {
                Ok(Value::TinyInt(v as u8))
            }
        }
        DataType::SmallInt => {
            if v < i16::MIN as i64 || v > i16::MAX as i64 {
                Err(DbError::Execution(format!(
                    "Arithmetic overflow error converting value {} to SMALLINT",
                    v
                )))
            } else {
                Ok(Value::SmallInt(v as i16))
            }
        }
        DataType::Int => {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                Err(DbError::Execution(format!(
                    "Arithmetic overflow error converting value {} to INT",
                    v
                )))
            } else {
                Ok(Value::Int(v as i32))
            }
        }
        DataType::BigInt => Ok(Value::BigInt(v)),
        DataType::Decimal { scale, .. } => {
            let raw = v as i128 * 10i128.pow(*scale as u32);
            Ok(Value::Decimal(raw, *scale))
        }
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::DateTime | DataType::DateTime2 | DataType::Date | DataType::Time => Err(
            DbError::Execution(format!("cannot convert integer to {:?}", ty)),
        ),
        DataType::UniqueIdentifier => Err(DbError::Execution(
            "cannot convert integer to UNIQUEIDENTIFIER".into(),
        )),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::BigInt(v)))),
    }
}

fn coerce_decimal(raw: i128, scale: u8, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Decimal { scale: ts, .. } => {
            if *ts == scale {
                Ok(Value::Decimal(raw, scale))
            } else {
                let converted = rescale_decimal(raw, scale, *ts);
                Ok(Value::Decimal(converted, *ts))
            }
        }
        DataType::Bit => Ok(Value::Bit(raw != 0)),
        DataType::TinyInt => {
            let v = if scale > 0 {
                raw / 10i128.pow(scale as u32)
            } else {
                raw
            };
            if !(0..=255).contains(&v) {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to TINYINT".into(),
                ))
            } else {
                Ok(Value::TinyInt(v as u8))
            }
        }
        DataType::SmallInt => {
            let v = if scale > 0 {
                raw / 10i128.pow(scale as u32)
            } else {
                raw
            };
            if v < i16::MIN as i128 || v > i16::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to SMALLINT".into(),
                ))
            } else {
                Ok(Value::SmallInt(v as i16))
            }
        }
        DataType::Int => {
            let v = if scale > 0 {
                raw / 10i128.pow(scale as u32)
            } else {
                raw
            };
            if v < i32::MIN as i128 || v > i32::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to INT".into(),
                ))
            } else {
                Ok(Value::Int(v as i32))
            }
        }
        DataType::BigInt => {
            let v = if scale > 0 {
                raw / 10i128.pow(scale as u32)
            } else {
                raw
            };
            if v < i64::MIN as i128 || v > i64::MAX as i128 {
                Err(DbError::Execution(
                    "Arithmetic overflow error converting DECIMAL to BIGINT".into(),
                ))
            } else {
                Ok(Value::BigInt(v as i64))
            }
        }
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(crate::types::format_decimal(raw, scale)))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(crate::types::format_decimal(raw, scale)))
        }
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Decimal(raw, scale)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert DECIMAL to {:?}",
            ty
        ))),
    }
}

fn coerce_string(v: &str, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Bit => Ok(Value::Bit(v != "0" && !v.is_empty())),
        DataType::TinyInt => v
            .parse::<u8>()
            .map(Value::TinyInt)
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to TINYINT", v))),
        DataType::SmallInt => v
            .parse::<i16>()
            .map(Value::SmallInt)
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to SMALLINT", v))),
        DataType::Int => v
            .parse::<i32>()
            .map(Value::Int)
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to INT", v))),
        DataType::BigInt => v
            .parse::<i64>()
            .map(Value::BigInt)
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to BIGINT", v))),
        DataType::Decimal { scale, .. } => parse_decimal_string(v, *scale),
        DataType::Char { len } => {
            let padded = pad_right(v, *len as usize);
            Ok(Value::Char(padded))
        }
        DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { len } => {
            let padded = pad_right(v, *len as usize);
            Ok(Value::NChar(padded))
        }
        DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::Date => Ok(Value::Date(v.to_string())),
        DataType::Time => Ok(Value::Time(v.to_string())),
        DataType::DateTime => Ok(Value::DateTime(v.to_string())),
        DataType::DateTime2 => Ok(Value::DateTime2(v.to_string())),
        DataType::UniqueIdentifier => Ok(Value::UniqueIdentifier(v.to_string())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(v.to_string())))),
    }
}

fn coerce_date_time_string(v: &str, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::Date => Ok(Value::Date(v.to_string())),
        DataType::Time => Ok(Value::Time(v.to_string())),
        DataType::DateTime => Ok(Value::DateTime(v.to_string())),
        DataType::DateTime2 => Ok(Value::DateTime2(v.to_string())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::VarChar(v.to_string())))),
        _ => Err(DbError::Execution(format!(
            "cannot convert datetime-like value to {:?}",
            ty
        ))),
    }
}

fn coerce_uuid(v: &str, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::UniqueIdentifier => Ok(Value::UniqueIdentifier(v.to_string())),
        DataType::Char { .. } | DataType::VarChar { .. } => Ok(Value::VarChar(v.to_string())),
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(v.to_string())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::UniqueIdentifier(
            v.to_string(),
        )))),
        _ => Err(DbError::Execution(format!(
            "cannot convert UNIQUEIDENTIFIER to {:?}",
            ty
        ))),
    }
}

pub(crate) fn parse_decimal_string(s: &str, scale: u8) -> Result<Value, DbError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(DbError::Execution(
            "cannot convert empty string to DECIMAL".into(),
        ));
    }
    let negative = trimmed.starts_with('-');
    let abs_str = if negative || trimmed.starts_with('+') {
        &trimmed[1..]
    } else {
        trimmed
    };

    let parts: Vec<&str> = abs_str.splitn(2, '.').collect();
    let whole_str = parts[0];
    let frac_str = parts.get(1).copied().unwrap_or("");

    let whole: i128 = whole_str
        .parse()
        .map_err(|_| DbError::Execution(format!("cannot convert '{}' to DECIMAL", s)))?;

    let mut frac: i128 = 0;
    if scale > 0 && !frac_str.is_empty() {
        let truncated = if frac_str.len() > scale as usize {
            &frac_str[..scale as usize]
        } else {
            frac_str
        };
        frac = truncated
            .parse()
            .map_err(|_| DbError::Execution(format!("cannot convert '{}' to DECIMAL", s)))?;
        if frac_str.len() < scale as usize {
            frac *= 10i128.pow((scale as usize - frac_str.len()) as u32);
        }
    }

    let raw = whole * 10i128.pow(scale as u32) + frac;
    let raw = if negative { -raw } else { raw };
    Ok(Value::Decimal(raw, scale))
}

fn rescale_decimal(raw: i128, from_scale: u8, to_scale: u8) -> i128 {
    if from_scale == to_scale {
        return raw;
    }
    if to_scale > from_scale {
        raw * 10i128.pow((to_scale - from_scale) as u32)
    } else {
        raw / 10i128.pow((from_scale - to_scale) as u32)
    }
}

fn pad_right(s: &str, len: usize) -> String {
    if s.len() >= len {
        s[..len].to_string()
    } else {
        format!("{:width$}", s, width = len)
    }
}

pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    if let Value::SqlVariant(inner) = a {
        return compare_values(inner, b);
    }
    if let Value::SqlVariant(inner) = b {
        return compare_values(a, inner);
    }

    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,

        // Integer-to-integer comparisons
        (Value::TinyInt(x), Value::TinyInt(y)) => x.cmp(y),
        (Value::SmallInt(x), Value::SmallInt(y)) => x.cmp(y),
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::BigInt(x), Value::BigInt(y)) => x.cmp(y),

        // Cross-integer comparisons via i64
        (a, b) if is_integer_value(a) && is_integer_value(b) => {
            let ai = a.to_integer_i64().unwrap_or(0);
            let bi = b.to_integer_i64().unwrap_or(0);
            ai.cmp(&bi)
        }

        // Bit with integers (via to_integer_i64)
        (Value::Bit(_), b) if is_integer_value(b) => {
            let ai = a.to_integer_i64().unwrap_or(0);
            let bi = b.to_integer_i64().unwrap_or(0);
            ai.cmp(&bi)
        }
        (a, Value::Bit(_)) if is_integer_value(a) => {
            let ai = a.to_integer_i64().unwrap_or(0);
            let bi = b.to_integer_i64().unwrap_or(0);
            ai.cmp(&bi)
        }
        (Value::Bit(x), Value::Bit(y)) => x.cmp(y),

        // Decimal comparisons
        (Value::Decimal(ar, as_), Value::Decimal(br, bs)) => {
            let (an, bn) = normalize_decimals(*ar, *as_, *br, *bs);
            an.cmp(&bn)
        }
        (Value::Decimal(_, _), b) if is_numeric_value(b) => {
            let (a_dec, b_dec) = to_comparable_decimals(a, b);
            a_dec.cmp(&b_dec)
        }
        (a, Value::Decimal(_, _)) if is_numeric_value(a) => {
            let (a_dec, b_dec) = to_comparable_decimals(a, b);
            a_dec.cmp(&b_dec)
        }

        // String comparisons
        (Value::Char(x), Value::Char(y)) => x.cmp(y),
        (Value::VarChar(x), Value::VarChar(y)) => x.cmp(y),
        (Value::NChar(x), Value::NChar(y)) => x.cmp(y),
        (Value::NVarChar(x), Value::NVarChar(y)) => x.cmp(y),
        (a, b) if is_string_value(a) && is_string_value(b) => {
            let astr = extract_string(a);
            let bstr = extract_string(b);
            astr.cmp(bstr)
        }

        // Mixed numeric-string comparisons: try numeric coercion first.
        (a, b) if is_numeric_value(a) && is_string_value(b) => {
            let b_num = parse_string_as_numeric(extract_string(b));
            if let Some((br, bs)) = b_num {
                let (ar, as_) = to_decimal_parts(a);
                let (an, bn) = normalize_decimals(ar, as_, br, bs);
                an.cmp(&bn)
            } else {
                a.to_string_value().cmp(&b.to_string_value())
            }
        }
        (a, b) if is_string_value(a) && is_numeric_value(b) => {
            let a_num = parse_string_as_numeric(extract_string(a));
            if let Some((ar, as_)) = a_num {
                let (br, bs) = to_decimal_parts(b);
                let (an, bn) = normalize_decimals(ar, as_, br, bs);
                an.cmp(&bn)
            } else {
                a.to_string_value().cmp(&b.to_string_value())
            }
        }

        // DateTime-like comparisons
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
        (Value::DateTime2(x), Value::DateTime2(y)) => x.cmp(y),
        (a, b) if is_datetime_value(a) && is_datetime_value(b) => {
            let astr = extract_string(a);
            let bstr = extract_string(b);
            astr.cmp(bstr)
        }
        (a, b) if is_datetime_value(a) && is_string_value(b) => {
            a.to_string_value().cmp(&b.to_string_value())
        }
        (a, b) if is_string_value(a) && is_datetime_value(b) => {
            a.to_string_value().cmp(&b.to_string_value())
        }

        // UUID
        (Value::UniqueIdentifier(x), Value::UniqueIdentifier(y)) => x.cmp(y),

        // Fallback
        _ => value_key(a).cmp(&value_key(b)),
    }
}

fn is_integer_value(v: &Value) -> bool {
    if let Value::SqlVariant(inner) = v {
        return is_integer_value(inner);
    }
    matches!(
        v,
        Value::Bit(_) | Value::TinyInt(_) | Value::SmallInt(_) | Value::Int(_) | Value::BigInt(_)
    )
}

fn is_numeric_value(v: &Value) -> bool {
    is_integer_value(v) || matches!(v, Value::Decimal(_, _))
}

fn is_string_value(v: &Value) -> bool {
    if let Value::SqlVariant(inner) = v {
        return is_string_value(inner);
    }
    matches!(
        v,
        Value::Char(_) | Value::VarChar(_) | Value::NChar(_) | Value::NVarChar(_)
    )
}

fn is_datetime_value(v: &Value) -> bool {
    if let Value::SqlVariant(inner) = v {
        return is_datetime_value(inner);
    }
    matches!(
        v,
        Value::Date(_) | Value::Time(_) | Value::DateTime(_) | Value::DateTime2(_)
    )
}

fn extract_string(v: &Value) -> &str {
    match v {
        Value::Char(s) | Value::VarChar(s) | Value::NChar(s) | Value::NVarChar(s) => s,
        Value::Date(s) | Value::Time(s) | Value::DateTime(s) | Value::DateTime2(s) => s,
        _ => "",
    }
}

fn normalize_decimals(ar: i128, as_: u8, br: i128, bs: u8) -> (i128, i128) {
    let max_scale = as_.max(bs);
    let an = rescale_decimal(ar, as_, max_scale);
    let bn = rescale_decimal(br, bs, max_scale);
    (an, bn)
}

fn to_comparable_decimals(a: &Value, b: &Value) -> (i128, i128) {
    let (ar, as_) = match a {
        Value::Decimal(r, s) => (*r, *s),
        _ => (a.to_integer_i64().unwrap_or(0) as i128, 0),
    };
    let (br, bs) = match b {
        Value::Decimal(r, s) => (*r, *s),
        _ => (b.to_integer_i64().unwrap_or(0) as i128, 0),
    };
    normalize_decimals(ar, as_, br, bs)
}

fn parse_string_as_numeric(input: &str) -> Option<(i128, u8)> {
    let s = input.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(i) = s.parse::<i128>() {
        return Some((i, 0));
    }
    let negative = s.starts_with('-');
    let core = if negative || s.starts_with('+') {
        &s[1..]
    } else {
        s
    };
    let parts: Vec<&str> = core.splitn(2, '.').collect();
    if parts.len() != 2 {
        return None;
    }
    let whole = parts[0].parse::<i128>().ok()?;
    let frac_raw = parts[1];
    if frac_raw.is_empty() || !frac_raw.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let scale = frac_raw.len() as u8;
    let frac = frac_raw.parse::<i128>().ok()?;
    let mut raw = whole * 10i128.pow(scale as u32) + frac;
    if negative {
        raw = -raw;
    }
    Some((raw, scale))
}

pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bit(v) => *v,
        Value::TinyInt(v) => *v != 0,
        Value::SmallInt(v) => *v != 0,
        Value::Int(v) => *v != 0,
        Value::BigInt(v) => *v != 0,
        Value::Decimal(raw, _) => *raw != 0,
        Value::Char(v) | Value::VarChar(v) | Value::NChar(v) | Value::NVarChar(v) => !v.is_empty(),
        Value::Date(_)
        | Value::Time(_)
        | Value::DateTime(_)
        | Value::DateTime2(_)
        | Value::UniqueIdentifier(_) => true,
        Value::SqlVariant(inner) => truthy(inner),
    }
}

pub fn value_key(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bit(v) => format!("BIT:{}", v),
        Value::TinyInt(v) => format!("TINYINT:{}", v),
        Value::SmallInt(v) => format!("SMALLINT:{}", v),
        Value::Int(v) => format!("INT:{}", v),
        Value::BigInt(v) => format!("BIGINT:{}", v),
        Value::Decimal(raw, scale) => format!("DECIMAL:{}:{}", raw, scale),
        Value::Char(v) => format!("CHAR:{}", v),
        Value::VarChar(v) => format!("VARCHAR:{}", v),
        Value::NChar(v) => format!("NCHAR:{}", v),
        Value::NVarChar(v) => format!("NVARCHAR:{}", v),
        Value::Date(v) => format!("DATE:{}", v),
        Value::Time(v) => format!("TIME:{}", v),
        Value::DateTime(v) => format!("DATETIME:{}", v),
        Value::DateTime2(v) => format!("DATETIME2:{}", v),
        Value::UniqueIdentifier(v) => format!("UNIQUEIDENTIFIER:{}", v),
        Value::SqlVariant(inner) => format!("SQL_VARIANT:{}", value_key(inner)),
    }
}
