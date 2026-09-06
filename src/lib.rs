pub use crate::types::{Job, Priority, Queue, Response, State};
use crate::types::{JobGuard, Key};
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
    pub fn builder(capacity: usize, permits: usize) -> Queue {
        let mut lanes = HashMap::new();
        let mut receivers = HashMap::new();
        let mut semaphores = HashMap::new();
        for p in Priority::iter() {
            let (sender, receiver) = mpsc::channel(capacity);
            lanes.insert(p.clone(), sender);
            receivers.insert(p.clone(), receiver);
            semaphores.insert(p, Arc::new(Semaphore::new(permits)));
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
            let sem = self.semaphores.get(&p).unwrap().clone();
            tokio::spawn(async move {
                while let Some(job) = recv.recv().await {
                    let permit = sem.clone().acquire_owned().await.unwrap();
                    let statemap = statemap.clone();

                    tokio::spawn(async move {
                        let _permit = permit;
                        {
                            let mut statemap = statemap.lock().unwrap();
                            statemap.insert(job.key.clone(), State::Processing);
                        }

                        let mut guard = JobGuard {
                            statemap,
                            key: job.key.clone(),
                            completed: false,
                        };
                        (job.func)().await;
                        guard.completed = true;
                    });
                }
            });
        }
    }

    pub async fn push(&self, job: Job) -> Response {
        {
            let mut statemap = self.statemap.lock().unwrap();

            match statemap.get(&job.key) {
                Some(State::Completed) | Some(State::Pending) | Some(State::Processing) => {
                    return Response::Duplicate;
                }
                Some(State::Failed) | None => {
                    statemap.insert(job.key.clone(), State::Pending);
                }
            }
        }

        let sender = self.lanes.get(&job.priority).unwrap().clone();
        let key: Key = job.key.clone();

        match sender.send(job).await {
            Ok(()) => Response::Successful,
            Err(_) => {
                let mut statemap = self.statemap.lock().unwrap();
                statemap.insert(key, State::Failed);
                Response::Failed
            }
        }
    }
}
