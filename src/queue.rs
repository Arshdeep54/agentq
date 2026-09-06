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

struct Lane {
    sender: Sender<Job>,
    receiver: Option<Receiver<Job>>,
    semaphore: Arc<Semaphore>,
}

pub struct Queue {
    lanes: [Lane; Priority::COUNT],
    statemap: StateMap,
}

impl Queue {
    pub fn builder(config: QueueConfig) -> Queue {
        let lanes = std::array::from_fn(|_| {
            let (sender, receiver) = mpsc::channel(config.capacity);
            Lane {
                sender,
                receiver: Some(receiver),
                semaphore: Arc::new(Semaphore::new(config.permits)),
            }
        });

        Queue {
            lanes,
            statemap: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn run(&mut self) {
        for lane in self.lanes.iter_mut() {
            let statemap = self.statemap.clone();
            let sem = lane.semaphore.clone();
            let Some(mut recv) = lane.receiver.take() else {
                continue;
            };

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
                            Ok(()) => State::Completed,
                            Err(e) => State::Failed {
                                reason: e.to_string(),
                            },
                        });
                    });
                }
            });
        }
    }

    pub async fn push(&self, job: Job) -> Result<Accepted, PushError> {
        {
            let mut statemap = self.statemap.lock().unwrap_or_else(|e| e.into_inner());

            match statemap.get(&job.key) {
                Some(State::Completed) | Some(State::Pending) | Some(State::Processing) => {
                    return Ok(Accepted::Duplicate);
                }
                Some(State::Failed { .. }) | None => {
                    statemap.insert(job.key.clone(), State::Pending);
                }
            }
        }

        let sender = self.lanes[job.priority as usize].sender.clone();
        let key: Key = job.key.clone();

        match sender.send(job).await {
            Ok(()) => Ok(Accepted::Queued),
            Err(err) => {
                let mut statemap = self.statemap.lock().unwrap_or_else(|e| e.into_inner());
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
        let sm = self.statemap.lock().unwrap_or_else(|e| e.into_inner());
        sm.get(key).cloned()
    }
}
