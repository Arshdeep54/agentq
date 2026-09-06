use crate::types::JobGuard;
pub use crate::types::{
    Accepted, Func, Job, JobResult, Key, Priority, PushError, Queue, QueueConfig, State,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use strum::IntoEnumIterator;
use tokio::sync::{
    Semaphore,
    mpsc::{self},
};
mod types;

impl Queue {
    pub fn builder(queueconfig: QueueConfig) -> Queue {
        let mut lanes = HashMap::new();
        let mut receivers = HashMap::new();
        let mut semaphores = HashMap::new();
        for p in Priority::iter() {
            let (sender, receiver) = mpsc::channel(queueconfig.capacity);
            lanes.insert(p.clone(), sender);
            receivers.insert(p.clone(), receiver);
            semaphores.insert(p, Arc::new(Semaphore::new(queueconfig.permits)));
        }

        Queue {
            lanes,
            receivers,
            statemap: Arc::new(Mutex::new(HashMap::new())),
            semaphores,
        }
    }

    pub fn run(&mut self) {
        for (p, mut recv) in self.receivers.drain() {
            let statemap = self.statemap.clone();
            let sem = self
                .semaphores
                .get(&p)
                .expect("builder() creates a semaphore for every Priority variant")
                .clone();
            tokio::spawn(async move {
                while let Some(job) = recv.recv().await {
                    let permit = sem
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("lane semaphore is never closed");
                    let statemap = statemap.clone();

                    tokio::spawn(async move {
                        let _permit = permit;
                        {
                            let mut statemap = statemap
                                .lock()
                                .expect("statemap lock poisoned: no code panics while holding it");
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
            let mut statemap = self
                .statemap
                .lock()
                .expect("statemap lock poisoned: no code panics while holding it");

            match statemap.get(&job.key) {
                Some(State::Completed) | Some(State::Pending) | Some(State::Processing) => {
                    return Ok(Accepted::Duplicate);
                }
                Some(State::Failed { .. }) | None => {
                    statemap.insert(job.key.clone(), State::Pending);
                }
            }
        }

        let sender = self
            .lanes
            .get(&job.priority)
            .expect("builder() creates a lane for every Priority variant")
            .clone();
        let key: Key = job.key.clone();

        match sender.send(job).await {
            Ok(()) => Ok(Accepted::Queued),
            Err(err) => {
                let mut statemap = self
                    .statemap
                    .lock()
                    .expect("statemap lock poisoned: no code panics while holding it");
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
            .statemap
            .lock()
            .expect("statemap lock poisoned: no code panics while holding it");
        sm.get(key).cloned()
    }
}
