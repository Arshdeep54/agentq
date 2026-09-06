use std::error::Error;
use std::pin::Pin;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, strum::EnumCount)]
pub enum Priority {
    Low,
    Medium,
    High,
}

pub type Key = String;

pub type JobResult = Result<(), Box<dyn Error + Send + Sync>>;

pub type Func = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = JobResult> + Send>> + Send>;

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
