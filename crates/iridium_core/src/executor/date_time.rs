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

pub(crate) fn parse_datetime_parts(
    s: &str,
    dateformat: &str,
) -> Result<(i32, i32, i32, i32, i32, i32), DbError> {
    let s = s.trim();
    let (date_part, time_part) = split_date_time(s);
    let (y, m, d) = parse_date_part(date_part, dateformat)?;
    let (h, mi, sec) = parse_time_part(time_part)?;
    Ok((y, m, d, h, mi, sec))
}

pub(crate) fn date_to_days(y: i32, m: i32, d: i32) -> i64 {
    let (y_adj, m_adj) = if m <= 2 { (y - 1, m + 12) } else { (y, m) };
    let era = y_adj as i64 / 400;
    let yoe = y_adj as i64 - era * 400;
    let doy = (153 * (m_adj as i64 - 3) + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub(crate) fn day_of_week_from_date(y: i32, m: i32, d: i32) -> i32 {
    let days = date_to_days(y, m, d);
    (((days + 719471) % 7 + 7) % 7) as i32
}

pub(crate) fn apply_dateadd(
    part: &str,
    num: i64,
    date_str: &str,
    dateformat: &str,
) -> Result<NaiveDateTime, DbError> {
    let date_part = DatePart::from_str(part)?;
    let (y, m, d, h, mi, s) = parse_datetime_parts(date_str, dateformat)?;
    let date = NaiveDate::from_ymd_opt(y, m as u32, d as u32)
        .ok_or_else(|| DbError::Execution(format!("invalid datetime format: '{}'", date_str)))?;
    let dt = date
        .and_hms_opt(h as u32, mi as u32, s as u32)
        .ok_or_else(|| DbError::Execution(format!("invalid datetime format: '{}'", date_str)))?;

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

fn split_date_time(s: &str) -> (&str, &str) {
    if let Some(pos) = s.find('T') {
        (&s[..pos], s[pos + 1..].trim())
    } else if let Some(pos) = s.find(' ') {
        (&s[..pos], s[pos + 1..].trim())
    } else {
        (s, "")
    }
}

fn parse_date_part(date_part: &str, dateformat: &str) -> Result<(i32, i32, i32), DbError> {
    let cleaned = date_part.trim();
    let segments: Vec<&str> = cleaned
        .split(['-', '/', '.'])
        .filter(|s| !s.is_empty())
        .collect();
    if segments.len() < 3 {
        return Err(DbError::Execution(format!(
            "invalid datetime format: '{}'",
            date_part
        )));
    }

    if segments[0].len() == 4 {
        let y = parse_i32(segments[0], "year", date_part)?;
        let m = parse_i32(segments[1], "month", date_part)?;
        let d = parse_i32(segments[2], "day", date_part)?;
        return Ok((y, m, d));
    }

    let fmt = dateformat.to_ascii_lowercase();
    let (y, m, d) = match fmt.as_str() {
        "dmy" => (
            parse_i32(segments[2], "year", date_part)?,
            parse_i32(segments[1], "month", date_part)?,
            parse_i32(segments[0], "day", date_part)?,
        ),
        "ydm" => (
            parse_i32(segments[0], "year", date_part)?,
            parse_i32(segments[2], "month", date_part)?,
            parse_i32(segments[1], "day", date_part)?,
        ),
        "myd" => (
            parse_i32(segments[2], "year", date_part)?,
            parse_i32(segments[0], "month", date_part)?,
            parse_i32(segments[1], "day", date_part)?,
        ),
        "dym" => (
            parse_i32(segments[1], "year", date_part)?,
            parse_i32(segments[2], "month", date_part)?,
            parse_i32(segments[0], "day", date_part)?,
        ),
        _ => (
            parse_i32(segments[2], "year", date_part)?,
            parse_i32(segments[0], "month", date_part)?,
            parse_i32(segments[1], "day", date_part)?,
        ),
    };
    Ok((y, m, d))
}

fn parse_time_part(time_part: &str) -> Result<(i32, i32, i32), DbError> {
    if time_part.is_empty() {
        return Ok((0, 0, 0));
    }
    let time_segments: Vec<&str> = time_part.split(':').collect();
    let h = time_segments
        .first()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mi = time_segments
        .get(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let sec = time_segments
        .get(2)
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0) as i32;
    Ok((h, mi, sec))
}

fn parse_i32(segment: &str, label: &str, source: &str) -> Result<i32, DbError> {
    segment
        .parse::<i32>()
        .map_err(|_| DbError::Execution(format!("invalid {} in '{}'", label, source)))
}
