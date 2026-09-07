use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::sync::mpsc::Receiver;

use crate::job::Job;
use crate::state::{JobGuard, State, StateMap};

pub(crate) fn spawn_worker(
    mut receiver: Receiver<Job>,
    semaphore: Arc<Semaphore>,
    statemap: StateMap,
) {
    tokio::spawn(async move {
        while let Some(job) = receiver.recv().await {
            let Ok(permit) = semaphore.clone().acquire_owned().await else {
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
