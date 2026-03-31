use parking_lot::Mutex;

pub trait RandomProvider: Send + Sync {
    fn next_f64(&self) -> f64;
    fn next_u64(&self) -> u64;
}

#[derive(Debug)]
pub struct SeededRandom {
    state: Mutex<u64>,
}

impl SeededRandom {
    pub fn new(seed: u64) -> Self {
        Self {
            state: Mutex::new(seed),
        }
    }

    fn next_state(&self) -> u64 {
        let mut state = self.state.lock();
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *state
    }
}

impl RandomProvider for SeededRandom {
    fn next_f64(&self) -> f64 {
        let bits = self.next_state() >> 33;
        bits as f64 / (1u64 << 31) as f64
    }

    fn next_u64(&self) -> u64 {
        self.next_state()
    }
}

#[derive(Debug)]
pub struct ThreadRng;

impl Default for ThreadRng {
    fn default() -> Self {
        Self
    }
}

impl RandomProvider for ThreadRng {
    fn next_f64(&self) -> f64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut hasher = DefaultHasher::new();
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .hash(&mut hasher);
        let bits = hasher.finish() >> 33;
        bits as f64 / (1u64 << 31) as f64
    }

    fn next_u64(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        use std::time::{SystemTime, UNIX_EPOCH};

        let mut hasher = DefaultHasher::new();
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .hash(&mut hasher);
        hasher.finish()
    }
}
