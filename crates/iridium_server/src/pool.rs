use std::time::{Duration, Instant};

use parking_lot::Mutex;
use iridium_core::{SessionId, SessionManager};

use crate::ServerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckoutError {
    Exhausted,
}

#[derive(Debug, Clone, Copy)]
struct IdleSession {
    id: SessionId,
    returned_at: Instant,
}

#[derive(Debug)]
struct PoolState {
    available: Vec<IdleSession>,
    created_sessions: usize,
}

#[derive(Debug)]
pub struct SessionPool {
    state: Mutex<PoolState>,
    min_size: usize,
    max_size: usize,
    idle_timeout: Duration,
}

impl SessionPool {
    pub fn from_config(config: &ServerConfig) -> Self {
        Self {
            state: Mutex::new(PoolState {
                available: Vec::new(),
                created_sessions: 0,
            }),
            min_size: config.pool_min_size,
            max_size: config.pool_max_size,
            idle_timeout: Duration::from_secs(config.pool_idle_timeout_secs),
        }
    }

    pub fn ensure_min_sessions(&self, db: &dyn SessionManager) {
        let mut to_create = 0usize;
        {
            let mut state = self.state.lock();
            if state.created_sessions < self.min_size {
                to_create = self.min_size - state.created_sessions;
                state.created_sessions = self.min_size;
            }
        }

        for _ in 0..to_create {
            let session_id = db.create_session();
            let mut state = self.state.lock();
            state.available.push(IdleSession {
                id: session_id,
                returned_at: Instant::now(),
            });
        }
    }

    pub fn checkout(&self, db: &dyn SessionManager) -> Result<SessionId, CheckoutError> {
        let now = Instant::now();
        let mut to_close = Vec::new();
        let mut create_new = false;
        let mut idle_id = None;

        {
            let mut state = self.state.lock();

            while state.available.len() > self.min_size {
                let Some(oldest) = state.available.first() else {
                    break;
                };
                if now.duration_since(oldest.returned_at) < self.idle_timeout {
                    break;
                }
                let stale = state.available.remove(0);
                to_close.push(stale.id);
                state.created_sessions = state.created_sessions.saturating_sub(1);
            }

            if let Some(item) = state.available.pop() {
                idle_id = Some(item.id);
            } else if state.created_sessions < self.max_size {
                state.created_sessions += 1;
                create_new = true;
            } else {
                return Err(CheckoutError::Exhausted);
            }
        }

        for sid in to_close {
            let _ = db.close_session(sid);
        }

        if let Some(sid) = idle_id {
            return Ok(sid);
        }

        if create_new {
            return Ok(db.create_session());
        }

        Err(CheckoutError::Exhausted)
    }

    pub fn checkin(&self, db: &dyn SessionManager, session_id: SessionId) {
        if db.reset_session(session_id).is_err() {
            let _ = db.close_session(session_id);
            let mut state = self.state.lock();
            state.created_sessions = state.created_sessions.saturating_sub(1);
            return;
        }

        let mut state = self.state.lock();
        state.available.push(IdleSession {
            id: session_id,
            returned_at: Instant::now(),
        });
    }
}

