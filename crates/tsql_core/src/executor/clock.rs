pub trait Clock: Send + Sync {
    fn now_datetime_literal(&self) -> String;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_datetime_literal(&self) -> String {
        "1970-01-01T00:00:00".to_string()
    }
}

#[derive(Debug, Clone)]
pub struct FixedClock {
    value: String,
}

impl FixedClock {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl Clock for FixedClock {
    fn now_datetime_literal(&self) -> String {
        self.value.clone()
    }
}
