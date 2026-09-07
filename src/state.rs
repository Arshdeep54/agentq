use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::job::Key;

#[derive(Debug, Clone)]
pub enum State {
    Pending,
    Processing,
    Completed { output: String },
    Failed { reason: String },
}

pub(crate) type StateMap = Arc<Mutex<HashMap<Key, State>>>;

pub(crate) struct JobGuard {
    pub(crate) statemap: StateMap,
    pub(crate) key: Key,
    pub(crate) outcome: Option<State>,
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        let state = self.outcome.take().unwrap_or(State::Failed {
            reason: "job panicked".to_string(),
        });
        let mut m = self.statemap.lock().unwrap_or_else(|e| e.into_inner());
        m.insert(self.key.clone(), state);
    }
}
