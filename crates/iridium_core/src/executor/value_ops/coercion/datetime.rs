use crate::error::DbError;
use crate::types::{DataType, Value};

pub(crate) fn coerce_date_value(v: chrono::NaiveDate, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(v.format("%Y-%m-%d").to_string()))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(v.format("%Y-%m-%d").to_string()))
        }
        DataType::Date => Ok(Value::Date(v)),
        DataType::DateTime | DataType::DateTime2 => {
            let dt = v.and_hms_opt(0, 0, 0).unwrap();
            Ok(Value::DateTime(dt))
        }
        DataType::SmallDateTime => {
            let dt = v.and_hms_opt(0, 0, 0).unwrap();
            Ok(Value::SmallDateTime(dt))
        }
        DataType::DateTimeOffset => {
            let dt = v.and_hms_opt(0, 0, 0).unwrap();
            Ok(Value::DateTimeOffset(
                dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
            ))
        }
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Date(v)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert DATE value to {:?}",
            ty
        ))),
    }
}

pub(crate) fn coerce_time_value(v: chrono::NaiveTime, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(v.format("%H:%M:%S%.f").to_string()))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => {
            Ok(Value::NVarChar(v.format("%H:%M:%S%.f").to_string()))
        }
        DataType::Time => Ok(Value::Time(v)),
        DataType::DateTime | DataType::DateTime2 => {
            let dt = chrono::NaiveDate::from_ymd_opt(1900, 1, 1)
                .unwrap()
                .and_time(v);
            Ok(Value::DateTime(dt))
        }
        DataType::SmallDateTime => {
            let dt = chrono::NaiveDate::from_ymd_opt(1900, 1, 1)
                .unwrap()
                .and_time(v);
            Ok(Value::SmallDateTime(dt))
        }
        DataType::DateTimeOffset => Ok(Value::DateTimeOffset(format!(
            "1900-01-01T{}",
            v.format("%H:%M:%S%.f")
        ))),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::Time(v)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert TIME value to {:?}",
            ty
        ))),
    }
}

pub(crate) fn coerce_datetime_value(v: chrono::NaiveDateTime, ty: &DataType) -> Result<Value, DbError> {
    match ty {
        DataType::Char { .. } | DataType::VarChar { .. } => {
            Ok(Value::VarChar(v.format("%Y-%m-%d %H:%M:%S%.f").to_string()))
        }
        DataType::NChar { .. } | DataType::NVarChar { .. } => Ok(Value::NVarChar(
            v.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        )),
        DataType::DateTime | DataType::DateTime2 => Ok(Value::DateTime(v)),
        DataType::SmallDateTime => Ok(Value::SmallDateTime(v)),
        DataType::DateTimeOffset => Ok(Value::DateTimeOffset(
            v.format("%Y-%m-%dT%H:%M:%S%.f").to_string(),
        )),
        DataType::Date => Ok(Value::Date(v.date())),
        DataType::Time => Ok(Value::Time(v.time())),
        DataType::SqlVariant => Ok(Value::SqlVariant(Box::new(Value::DateTime(v)))),
        _ => Err(DbError::Execution(format!(
            "cannot convert DATETIME value to {:?}",
            ty
        ))),
    }
}

pub(crate) fn parse_date_string(v: &str, dateformat: &str) -> Result<chrono::NaiveDate, ()> {
    if let Ok(date) = chrono::NaiveDate::parse_from_str(v, "%Y-%m-%d") {
        return Ok(date);
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(v, "%Y/%m/%d") {
        return Ok(date);
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(v, "%Y.%m.%d") {
        return Ok(date);
    }

    let fmt = match dateformat.to_ascii_lowercase().as_str() {
        "dmy" => ["%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y"],
        "ymd" => ["%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d"],
        "ydm" => ["%Y/%d/%m", "%Y-%d-%m", "%Y.%d.%m"],
        "myd" => ["%m/%Y/%d", "%m-%Y-%d", "%m.%Y.%d"],
        "dym" => ["%d/%Y/%m", "%d-%Y-%m", "%d.%Y.%m"],
        _ => ["%m/%d/%Y", "%m-%d-%Y", "%m.%d.%Y"],
    };

    for candidate in fmt {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(v, candidate) {
            return Ok(date);
        }
    }

    chrono::NaiveDate::parse_from_str(v, "%d/%m/%Y").map_err(|_| ())
}
