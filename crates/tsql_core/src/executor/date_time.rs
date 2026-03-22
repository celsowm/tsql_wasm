use crate::error::DbError;

pub(crate) fn parse_datetime_parts(s: &str) -> Result<(i32, i32, i32, i32, i32, i32), DbError> {
    let s = s.trim();
    let t_parts: Vec<&str> = s.splitn(2, 'T').collect();
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

pub(crate) fn apply_dateadd(part: &str, num: i64, date_str: &str) -> Result<String, DbError> {
    let (y, m, d, h, mi, s) = parse_datetime_parts(date_str)?;

    let (ny, nm, nd, nh, nmi, ns) = match part {
        "year" | "yy" | "yyyy" => (y + num as i32, m, d, h, mi, s),
        "month" | "mm" | "m" => {
            let total = (y as i64) * 12 + (m as i64 - 1) + num;
            let ny = (total / 12) as i32;
            let nm = (total % 12 + 1) as i32;
            (ny, nm, d, h, mi, s)
        }
        "day" | "dd" | "d" => {
            let total_days = date_to_days(y, m, d) + num;
            let (ny, nm, nd) = days_to_date(total_days);
            (ny, nm, nd, h, mi, s)
        }
        "hour" | "hh" => {
            let total_hours = (date_to_days(y, m, d) * 24) + h as i64 + num;
            let total_days = total_hours.div_euclid(24);
            let nh = total_hours.rem_euclid(24) as i32;
            let (ny, nm, nd) = days_to_date(total_days);
            (ny, nm, nd, nh, mi, s)
        }
        "minute" | "mi" | "n" => {
            let total_minutes = (date_to_days(y, m, d) * 24 * 60) + h as i64 * 60 + mi as i64 + num;
            let total_days = total_minutes.div_euclid(24 * 60);
            let remainder = total_minutes.rem_euclid(24 * 60);
            let nh = (remainder / 60) as i32;
            let nmi = (remainder % 60) as i32;
            let (ny, nm, nd) = days_to_date(total_days);
            (ny, nm, nd, nh, nmi, s)
        }
        "second" | "ss" | "s" => {
            let total_secs =
                (date_to_days(y, m, d) * 86400) + h as i64 * 3600 + mi as i64 * 60 + s as i64 + num;
            let total_days = total_secs.div_euclid(86400);
            let remainder = total_secs.rem_euclid(86400);
            let nh = (remainder / 3600) as i32;
            let nmi = ((remainder % 3600) / 60) as i32;
            let ns = (remainder % 60) as i32;
            let (ny, nm, nd) = days_to_date(total_days);
            (ny, nm, nd, nh, nmi, ns)
        }
        _ => return Err(DbError::Execution(format!("unknown datepart '{}'", part))),
    };

    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        ny, nm, nd, nh, nmi, ns
    ))
}
