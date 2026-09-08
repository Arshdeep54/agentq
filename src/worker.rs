use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::sync::mpsc::Receiver;

use crate::handle::Outcome;
use crate::job::Job;
use crate::state::{JobGuard, State, StateMap, WaiterMap};

pub(crate) fn spawn_worker(
    mut receiver: Receiver<Job>,
    semaphore: Arc<Semaphore>,
    statemap: StateMap,
    waiters: WaiterMap,
) {
    tokio::spawn(async move {
        while let Some(job) = receiver.recv().await {
            let Ok(permit) = semaphore.clone().acquire_owned().await else {
                break;
            };
            let statemap = statemap.clone();
            let waiters = waiters.clone();

            tokio::spawn(async move {
                let _permit = permit;
                {
                    let mut statemap = statemap.lock().unwrap_or_else(|e| e.into_inner());
                    statemap.insert(job.key.clone(), State::Processing);
                }

                let mut guard = JobGuard {
                    statemap,
                    waiters,
                    key: job.key.clone(),
                    outcome: None,
                };

                guard.outcome = Some(match (job.func)().await {
                    Ok(output) => Outcome::Completed { output },
                    Err(e) => Outcome::Failed {
                        reason: e.to_string(),
                    },
                });
            });
        }
    });
}
