use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use strum::EnumCount;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{self, Sender};

use tokio::sync::oneshot;

use crate::error::{PushError, WaitError};
use crate::handle::{JobHandle, Outcome};
use crate::job::{Job, Key, Priority};
use crate::state::{State, StateMap, WaiterMap};
use crate::worker::spawn_worker;

#[derive(Debug)]
pub enum Accepted {
    Queued(JobHandle),
    Cached { output: String },
    InFlight(JobHandle),
}

#[derive(Clone)]
pub struct Queue {
    inner: Arc<Inner>,
}

impl Queue {
    pub fn builder() -> QueueBuilder {
        QueueBuilder::default()
    }

    pub async fn push(&self, job: Job) -> Result<Accepted, PushError> {
        let receiver = {
            let mut statemap = self
                .inner
                .statemap
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let mut waiters = self.inner.waiters.lock().unwrap_or_else(|e| e.into_inner());

            match statemap.get(&job.key) {
                Some(State::Completed { output }) => {
                    return Ok(Accepted::Cached {
                        output: output.clone(),
                    });
                }
                Some(State::Pending) | Some(State::Processing) => {
                    let (sender, receiver) = oneshot::channel();
                    waiters.entry(job.key.clone()).or_default().push(sender);
                    return Ok(Accepted::InFlight(JobHandle::new(receiver)));
                }
                Some(State::Failed { .. }) | None => {
                    statemap.insert(job.key.clone(), State::Pending);
                    let (sender, receiver) = oneshot::channel();
                    waiters.entry(job.key.clone()).or_default().push(sender);
                    receiver
                }
            }
        };

        let sender = self.inner.lanes[job.priority as usize].clone();
        let key: Key = job.key.clone();

        match sender.send(job).await {
            Ok(()) => Ok(Accepted::Queued(JobHandle::new(receiver))),
            Err(err) => {
                let mut statemap = self
                    .inner
                    .statemap
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let mut waiters = self.inner.waiters.lock().unwrap_or_else(|e| e.into_inner());

                statemap.insert(
                    key.clone(),
                    State::Failed {
                        reason: err.to_string(),
                    },
                );
                waiters.remove(&key);

                Err(PushError::LaneClosed)
            }
        }
    }

    pub async fn push_and_wait(&self, job: Job) -> Result<String, WaitError> {
        let handle = match self.push(job).await? {
            Accepted::Cached { output } => return Ok(output),
            Accepted::Queued(handle) | Accepted::InFlight(handle) => handle,
        };

        match handle.await? {
            Outcome::Completed { output } => Ok(output),
            Outcome::Failed { reason } => Err(WaitError::Failed { reason }),
        }
    }

    pub fn state(&self, key: &str) -> Option<State> {
        let sm = self
            .inner
            .statemap
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        sm.get(key).cloned()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LaneConfig {
    pub capacity: usize,
    pub permits: usize,
}

impl Default for LaneConfig {
    fn default() -> LaneConfig {
        LaneConfig {
            capacity: 100,
            permits: 5,
        }
    }
}

#[derive(Debug)]
pub struct QueueBuilder {
    lanes: [LaneConfig; Priority::COUNT],
}

impl Default for QueueBuilder {
    fn default() -> QueueBuilder {
        QueueBuilder {
            lanes: [LaneConfig::default(); Priority::COUNT],
        }
    }
}

impl QueueBuilder {
    pub fn lane(mut self, priority: Priority, config: LaneConfig) -> Self {
        self.lanes[priority as usize] = config;
        self
    }

    pub fn start(self) -> Queue {
        let statemap: StateMap = Arc::new(Mutex::new(HashMap::new()));
        let waiters: WaiterMap = Arc::new(Mutex::new(HashMap::new()));

        let lanes = std::array::from_fn(|i| {
            let (sender, receiver) = mpsc::channel(self.lanes[i].capacity);

            spawn_worker(
                receiver,
                Arc::new(Semaphore::new(self.lanes[i].permits)),
                statemap.clone(),
                waiters.clone(),
            );

            sender
        });

        Queue {
            inner: Arc::new(Inner {
                lanes,
                statemap,
                waiters,
            }),
        }
    }
}

struct Inner {
    lanes: [Sender<Job>; Priority::COUNT],
    statemap: StateMap,
    waiters: WaiterMap,
}
