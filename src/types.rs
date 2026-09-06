use std::{
    collections::HashMap,
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

#[derive(Debug)]
pub enum State {
    Pending,
    Processing,
    Completed,
    Failed,
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

pub(crate) type Key = String;
type StateMap = Arc<Mutex<HashMap<Key, State>>>;
type Func = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub struct Queue {
    pub(crate) lanes: HashMap<Priority, Sender<Job>>,
    pub(crate) receivers: HashMap<Priority, Receiver<Job>>,
    pub(crate) statemap: StateMap,
    pub(crate) semaphores: HashMap<Priority, Arc<Semaphore>>,
}

#[derive(std::fmt::Debug)]
pub enum Response {
    Duplicate,
    Successful,
    Failed,
}

pub(crate) struct JobGuard {
    pub(crate) statemap: StateMap,
    pub(crate) key: Key,
    pub(crate) completed: bool,
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        let state = if self.completed {
            State::Completed
        } else {
            State::Failed
        };
        if let Ok(mut m) = self.statemap.lock() {
            m.insert(self.key.clone(), state);
        }
    }
}
