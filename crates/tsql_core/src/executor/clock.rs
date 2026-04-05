use chrono::{NaiveDateTime, Utc};

pub trait Clock: Send + Sync {
    fn now_datetime_literal(&self) -> NaiveDateTime;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_datetime_literal(&self) -> NaiveDateTime {
        Utc::now().naive_utc()
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
