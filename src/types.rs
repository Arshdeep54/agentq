use std::{
    collections::HashMap,
    error::Error,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::{
    Semaphore,
    mpsc::{Receiver, Sender},
};

#[derive(Debug, Hash, PartialEq, Eq, Clone, strum::EnumIter)]
pub enum Priority {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone)]
pub enum State {
    Pending,
    Processing,
    Completed,
    Failed { reason: String },
}

pub struct Job {
    pub(crate) key: Key,
    pub(crate) priority: Priority,
    pub(crate) func: Func,
}

impl Job {
    pub fn new(key: Key, priority: Priority, func: Func) -> Job {
        Job {
            key,
            priority,
            func,
        }
    }
}

pub type Key = String;
type StateMap = Arc<Mutex<HashMap<Key, State>>>;
pub type JobResult = Result<(), Box<dyn Error + Send + Sync>>;
pub type Func = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = JobResult> + Send>> + Send>;

pub struct Queue {
    pub(crate) lanes: HashMap<Priority, Sender<Job>>,
    pub(crate) receivers: HashMap<Priority, Receiver<Job>>,
    pub(crate) statemap: StateMap,
    pub(crate) semaphores: HashMap<Priority, Arc<Semaphore>>,
}

pub struct QueueConfig {
    pub capacity: usize,
    pub permits: usize,
}

#[derive(Debug)]
pub enum Accepted {
    Queued,
    Duplicate,
}

#[derive(Debug)]
pub enum PushError {
    LaneClosed,
}

impl std::fmt::Display for PushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushError::LaneClosed => write!(f, "lane is closed; no worker is running"),
        }
    }
}

impl std::error::Error for PushError {}

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
        if let Ok(mut m) = self.statemap.lock() {
            m.insert(self.key.clone(), state);
        }
    }
}
