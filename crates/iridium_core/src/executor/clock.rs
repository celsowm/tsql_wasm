use chrono::{NaiveDateTime, Utc};

pub trait Clock: Send + Sync {
    fn now_datetime_literal(&self) -> NaiveDateTime;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_datetime_literal(&self) -> NaiveDateTime {
        system_now_naive_utc()
    }
}

#[derive(Debug, Clone)]
pub struct FixedClock {
    value: NaiveDateTime,
}

impl FixedClock {
    pub fn new(value: NaiveDateTime) -> Self {
        Self { value }
    }
}

impl Clock for FixedClock {
    fn now_datetime_literal(&self) -> NaiveDateTime {
        self.value
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn system_now_naive_utc() -> NaiveDateTime {
    Utc::now().naive_utc()
}

#[cfg(target_arch = "wasm32")]
fn system_now_naive_utc() -> NaiveDateTime {
    let millis = js_sys::Date::now() as i64;
    chrono::DateTime::<Utc>::from_timestamp_millis(millis)
        .unwrap_or(chrono::DateTime::<Utc>::UNIX_EPOCH)
        .naive_utc()
}
