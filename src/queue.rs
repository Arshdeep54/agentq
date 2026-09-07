use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use strum::EnumCount;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::error::{Accepted, PushError};
use crate::job::{Job, Key, Priority};
use crate::state::{JobGuard, State, StateMap};

pub struct QueueConfig {
    pub capacity: usize,
    pub permits: usize,
}

struct Inner {
    lanes: [Sender<Job>; Priority::COUNT],
    statemap: StateMap,
}
#[derive(Clone)]
pub struct Queue {
    inner: Arc<Inner>,
}

impl Queue {
    pub fn start(config: QueueConfig) -> Queue {
        let statemap = Arc::new(Mutex::new(HashMap::new()));

        let lanes = std::array::from_fn(|_| {
            let (sender, receiver) = mpsc::channel(config.capacity);

            spawn_worker(
                receiver,
                Arc::new(Semaphore::new(config.permits)),
                statemap.clone(),
            );

            sender
        });

        Queue {
            inner: Arc::new(Inner { lanes, statemap }),
        }
    }

    pub async fn push(&self, job: Job) -> Result<Accepted, PushError> {
        {
            let mut statemap = self
                .inner
                .statemap
                .lock()
                .unwrap_or_else(|e| e.into_inner());

            match statemap.get(&job.key) {
                Some(State::Completed { output }) => {
                    return Ok(Accepted::Cached {
                        output: output.clone(),
                    });
                }
                Some(State::Pending) | Some(State::Processing) => return Ok(Accepted::InFlight),
                Some(State::Failed { .. }) | None => {
                    statemap.insert(job.key.clone(), State::Pending);
                }
            }
        }

        let sender = self.inner.lanes[job.priority as usize].clone();
        let key: Key = job.key.clone();

        match sender.send(job).await {
            Ok(()) => Ok(Accepted::Queued),
            Err(err) => {
                let mut statemap = self
                    .inner
                    .statemap
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                statemap.insert(
                    key,
                    State::Failed {
                        reason: err.to_string(),
                    },
                );
                Err(PushError::LaneClosed)
            }
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

fn spawn_worker(mut recv: Receiver<Job>, sem: Arc<Semaphore>, statemap: StateMap) {
    tokio::spawn(async move {
        while let Some(job) = recv.recv().await {
            let Ok(permit) = sem.clone().acquire_owned().await else {
                break;
            };
            let statemap = statemap.clone();

            tokio::spawn(async move {
                let _permit = permit;
                {
                    let mut statemap = statemap.lock().unwrap_or_else(|e| e.into_inner());
                    statemap.insert(job.key.clone(), State::Processing);
                }

                let mut guard = JobGuard {
                    statemap,
                    key: job.key.clone(),
                    outcome: None,
                };

                guard.outcome = Some(match (job.func)().await {
                    Ok(output) => State::Completed { output },
                    Err(e) => State::Failed {
                        reason: e.to_string(),
                    },
                });
            });
        }
    });
}
