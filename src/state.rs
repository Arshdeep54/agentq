use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::handle::Outcome;
use crate::job::Key;

#[derive(Debug, Clone)]
pub enum State {
    Pending,
    Processing,
    Completed { output: String },
    Failed { reason: String },
}

impl From<&Outcome> for State {
    fn from(outcome: &Outcome) -> State {
        match outcome {
            Outcome::Completed { output } => State::Completed {
                output: output.clone(),
            },
            Outcome::Failed { reason } => State::Failed {
                reason: reason.clone(),
            },
        }
    }
}

pub(crate) type StateMap = Arc<Mutex<HashMap<Key, State>>>;

pub(crate) type WaiterMap = Arc<Mutex<HashMap<Key, Vec<oneshot::Sender<Outcome>>>>>;

pub(crate) struct JobGuard {
    pub(crate) statemap: StateMap,
    pub(crate) waiters: WaiterMap,
    pub(crate) key: Key,
    pub(crate) outcome: Option<Outcome>,
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        let outcome = self.outcome.take().unwrap_or(Outcome::Failed {
            reason: "job panicked".to_string(),
        });

        {
            let mut statemap = self.statemap.lock().unwrap_or_else(|e| e.into_inner());
            statemap.insert(self.key.clone(), State::from(&outcome));
        }

        let senders = {
            let mut waiters = self.waiters.lock().unwrap_or_else(|e| e.into_inner());
            waiters.remove(&self.key).unwrap_or_default()
        };

        for sender in senders {
            let _ = sender.send(outcome.clone());
        }
    }
}

pub(crate) struct ClaimGuard {
    pub(crate) statemap: StateMap,
    pub(crate) waiters: WaiterMap,
    pub(crate) key: Key,
    pub(crate) armed: bool,
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let mut statemap = self.statemap.lock().unwrap_or_else(|e| e.into_inner());
        let mut waiters = self.waiters.lock().unwrap_or_else(|e| e.into_inner());

        statemap.remove(&self.key);
        waiters.remove(&self.key);
    }
}
