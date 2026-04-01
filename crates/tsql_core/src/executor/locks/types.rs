use std::collections::HashMap;

pub type SessionId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockResource {
    Table(String),
    Row(String, usize),
}

#[derive(Debug, Clone)]
pub struct AcquiredLock {
    pub resource: LockResource,
    pub mode: LockMode,
    pub savepoint_depth: usize,
}

#[derive(Debug, Default, Clone)]
pub struct TableLockState {
    pub readers: HashMap<SessionId, u32>,
    pub writer: Option<(SessionId, u32)>,
}

impl TableLockState {
    pub fn is_empty(&self) -> bool {
        self.readers.is_empty() && self.writer.is_none()
    }

    pub fn has_conflict(&self, session_id: SessionId, mode: LockMode) -> bool {
        match mode {
            LockMode::Read => {
                if let Some((writer, _)) = self.writer {
                    if writer != session_id {
                        return true;
                    }
                }
                false
            }
            LockMode::Write => {
                if let Some((writer, _)) = self.writer {
                    if writer != session_id {
                        return true;
                    }
                }
                self.readers
                    .iter()
                    .any(|(reader, count)| *reader != session_id && *count > 0)
            }
        }
    }

    pub fn collect_blockers(&self, session_id: SessionId, mode: LockMode) -> Vec<SessionId> {
        let mut blockers = Vec::new();
        match mode {
            LockMode::Read => {
                if let Some((writer, _)) = self.writer {
                    if writer != session_id {
                        blockers.push(writer);
                    }
                }
            }
            LockMode::Write => {
                if let Some((writer, _)) = self.writer {
                    if writer != session_id {
                        blockers.push(writer);
                    }
                }
                for (reader, count) in &self.readers {
                    if *reader != session_id && *count > 0 {
                        blockers.push(*reader);
                    }
                }
            }
        }
        blockers
    }

    pub fn acquire(&mut self, session_id: SessionId, mode: LockMode) {
        match mode {
            LockMode::Read => {
                *self.readers.entry(session_id).or_insert(0) += 1;
            }
            LockMode::Write => match self.writer.as_mut() {
                Some((writer, count)) if *writer == session_id => {
                    *count += 1;
                }
                _ => {
                    self.writer = Some((session_id, 1));
                }
            },
        }
    }

    pub fn release_one(&mut self, session_id: SessionId, mode: LockMode) {
        match mode {
            LockMode::Read => {
                if let Some(count) = self.readers.get_mut(&session_id) {
                    if *count > 1 {
                        *count -= 1;
                    } else {
                        self.readers.remove(&session_id);
                    }
                }
            }
            LockMode::Write => {
                if let Some((owner, count)) = self.writer.as_mut() {
                    if *owner == session_id {
                        if *count > 1 {
                            *count -= 1;
                        } else {
                            self.writer = None;
                        }
                    }
                }
            }
        }
    }

    pub fn release_all(&mut self, session_id: SessionId) {
        self.readers.remove(&session_id);
        if self
            .writer
            .map(|(owner, _)| owner == session_id)
            .unwrap_or(false)
        {
            self.writer = None;
        }
    }
}
