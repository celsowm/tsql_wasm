use crate::ast::IsolationLevel;

#[derive(Debug, Clone)]
pub enum JournalEvent {
    Begin {
        isolation_level: IsolationLevel,
        name: Option<String>,
    },
    Savepoint {
        name: String,
    },
    Rollback {
        savepoint: Option<String>,
    },
    Commit,
    SetIsolationLevel {
        isolation_level: IsolationLevel,
    },
    WriteIntent {
        kind: WriteKind,
        table: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    Insert,
    Update,
    Delete,
    Ddl,
}

pub trait Journal: std::fmt::Debug + Send + Sync {
    fn record(&mut self, event: JournalEvent);
}

#[derive(Debug, Default)]
pub struct NoopJournal;

impl Journal for NoopJournal {
    fn record(&mut self, _event: JournalEvent) {}
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryJournal {
    events: Vec<JournalEvent>,
}

impl InMemoryJournal {
    pub fn events(&self) -> &[JournalEvent] {
        &self.events
    }
}

impl Journal for InMemoryJournal {
    fn record(&mut self, event: JournalEvent) {
        self.events.push(event);
    }
}
