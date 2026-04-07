use crate::error::DbError;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

/// Typed enum for date parts, replacing string-based dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl DatePart {
    /// Parse a date part string (case-insensitive) into a typed enum.
    pub fn from_str(s: &str) -> Result<Self, DbError> {
        match s.to_lowercase().as_str() {
            "year" | "yy" | "yyyy" => Ok(DatePart::Year),
            "month" | "mm" | "m" => Ok(DatePart::Month),
            "day" | "dd" | "d" => Ok(DatePart::Day),
            "hour" | "hh" => Ok(DatePart::Hour),
            "minute" | "mi" | "n" => Ok(DatePart::Minute),
            "second" | "ss" | "s" => Ok(DatePart::Second),
            _ => Err(DbError::Execution(format!("unknown datepart '{}'", s))),
        }
    }
}

pub(crate) fn parse_datetime_parts(s: &str) -> Result<(i32, i32, i32, i32, i32, i32), DbError> {
    let s = s.trim();
    let t_parts: Vec<&str> = s.splitn(2, |c: char| c == 'T' || c == ' ').collect();
    let date_part = t_parts[0];
    let time_part = t_parts.get(1).copied().unwrap_or("00:00:00");

    let date_segments: Vec<&str> = date_part.split('-').collect();
    if date_segments.len() < 3 {
        return Err(DbError::Execution(format!(
            "invalid datetime format: '{}'",
            s
        )));
    }
    let y: i32 = date_segments[0]
        .parse()
        .map_err(|_| DbError::Execution(format!("invalid year in '{}'", s)))?;
    let m: i32 = date_segments[1]
        .parse()
        .map_err(|_| DbError::Execution(format!("invalid month in '{}'", s)))?;
    let d: i32 = date_segments[2]
        .split(|c: char| c == ' ' || c == 'T')
        .next()
        .unwrap_or(date_segments[2])
        .parse()
        .map_err(|_| DbError::Execution(format!("invalid day in '{}'", s)))?;

    let time_segments: Vec<&str> = time_part.split(':').collect();
    let h: i32 = time_segments
        .first()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mi: i32 = time_segments
        .get(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let s_secs: f64 = time_segments
        .get(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    let s: i32 = s_secs as i32;

    Ok((y, m, d, h, mi, s))
}

pub(crate) fn date_to_days(y: i32, m: i32, d: i32) -> i64 {
    let (y_adj, m_adj) = if m <= 2 { (y - 1, m + 12) } else { (y, m) };
    let era = y_adj as i64 / 400;
    let yoe = y_adj as i64 - era * 400;
    let doy = (153 * (m_adj as i64 - 3) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[allow(dead_code)]
pub(crate) fn days_to_date(days: i64) -> (i32, i32, i32) {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as i32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as i32;
    let y = if m <= 2 { y + 1 } else { y } as i32;
    (y, m, d)
}

pub(crate) fn day_of_week_from_date(y: i32, m: i32, d: i32) -> i32 {
    let days = date_to_days(y, m, d);
    (((days + 719471) % 7 + 7) % 7) as i32
}

pub(crate) fn apply_dateadd(
    part: &str,
    num: i64,
    date_str: &str,
) -> Result<NaiveDateTime, DbError> {
    let date_part = DatePart::from_str(part)?;
    let dt = NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| {
            NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).expect("midnight is valid"))
        })
        .map_err(|_| DbError::Execution(format!("invalid datetime format: '{}'", date_str)))?;

    let result = match date_part {
        DatePart::Year => {
            let new_year = dt.year() + num as i32;
            dt.with_year(new_year)
                .ok_or_else(|| DbError::Execution("invalid year after DATEADD".into()))?
        }
        DatePart::Month => {
            let total_months = dt.year() as i64 * 12 + (dt.month() as i64 - 1) + num;
            let new_year = (total_months / 12) as i32;
            let new_month = (total_months % 12 + 1) as u32;
            dt.with_year(new_year)
                .and_then(|d| d.with_month(new_month))
                .ok_or_else(|| DbError::Execution("invalid month after DATEADD".into()))?
        }
        DatePart::Day => dt + chrono::Duration::days(num),
        DatePart::Hour => dt + chrono::Duration::hours(num),
        DatePart::Minute => dt + chrono::Duration::minutes(num),
        DatePart::Second => dt + chrono::Duration::seconds(num),
    };

    Ok(result)
}
