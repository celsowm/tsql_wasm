use std::collections::{HashMap, HashSet};
use super::locks::SessionId;

#[derive(Debug, Default)]
pub struct WaitForGraph {
    /// waiter_session_id -> set of session_ids it's waiting on
    edges: HashMap<SessionId, HashSet<SessionId>>,
}

impl WaitForGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_edge(&mut self, waiter: SessionId, holder: SessionId) {
        if waiter == holder {
            return;
        }
        self.edges.entry(waiter).or_default().insert(holder);
    }

    pub fn remove_edge(&mut self, waiter: SessionId, holder: SessionId) {
        if let Some(holders) = self.edges.get_mut(&waiter) {
            holders.remove(&holder);
            if holders.is_empty() {
                self.edges.remove(&waiter);
            }
        }
    }

    pub fn remove_waiter(&mut self, waiter: SessionId) {
        self.edges.remove(&waiter);
    }

    pub fn detect_cycle(&self, start_session: SessionId) -> Option<Vec<SessionId>> {
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();
        let mut path = Vec::new();

        if self.has_cycle(start_session, &mut visited, &mut in_stack, &mut path) {
            return Some(path);
        }
        None
    }

    fn has_cycle(
        &self,
        current: SessionId,
        visited: &mut HashSet<SessionId>,
        in_stack: &mut HashSet<SessionId>,
        path: &mut Vec<SessionId>,
    ) -> bool {
        visited.insert(current);
        in_stack.insert(current);
        path.push(current);

        if let Some(holders) = self.edges.get(&current) {
            for &holder in holders {
                if !visited.contains(&holder) {
                    if self.has_cycle(holder, visited, in_stack, path) {
                        return true;
                    }
                } else if in_stack.contains(&holder) {
                    // Cycle detected!
                    // Trim path to only include the cycle
                    if let Some(pos) = path.iter().position(|&x| x == holder) {
                        *path = path[pos..].to_vec();
                    }
                    return true;
                }
            }
        }

        in_stack.remove(&current);
        path.pop();
        false
    }
}
