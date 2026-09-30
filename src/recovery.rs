use crate::{DurableStore, EngineError, WorkflowEngine};

/// Re-admits steps whose worker lease expired while the worker was unavailable.
pub async fn recover<S: DurableStore>(engine: &WorkflowEngine<S>) -> Result<usize, EngineError> {
    let expired = engine.store().expired_leases()?;
    let mut recovered = 0usize;
    for exec in expired {
        engine
            .readmit_after_worker_recovery(&exec.workflow_id, exec.step_index, exec.attempt)
            .await?;
        recovered += 1;
    }
    Ok(recovered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Backoff, ClaimResult, Event, Execution, Priority, Queue, RetryPolicy, StepDef, StepFunc,
        StepStatus, StoreError, Workflow,
    };
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};

    #[derive(Clone)]
    struct MemStore {
        events: Arc<Mutex<Vec<Event>>>,
        steps: Arc<Mutex<HashMap<(String, usize), StepProjection>>>,
    }

    #[derive(Clone)]
    struct StepProjection {
        status: StepStatus,
        attempt: u32,
    }

    impl MemStore {
        fn new() -> Self {
            MemStore {
                events: Arc::new(Mutex::new(Vec::new())),
                steps: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn step_status(&self, workflow_id: &str, step_index: usize) -> Option<StepStatus> {
            self.steps
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&(workflow_id.to_string(), step_index))
                .map(|row| row.status.clone())
        }

        fn seed_expired_lease(
            &self,
            workflow_id: &str,
            step_index: usize,
            attempt: u32,
            worker_id: &str,
        ) {
            let expired_at = SystemTime::now()
                .checked_sub(Duration::from_secs(60))
                .unwrap_or(SystemTime::UNIX_EPOCH);
            self.steps.lock().unwrap_or_else(|e| e.into_inner()).insert(
                (workflow_id.to_string(), step_index),
                StepProjection {
                    status: StepStatus::Leased {
                        worker_id: worker_id.to_string(),
                        expires_at: expired_at,
                    },
                    attempt,
                },
            );
        }
    }

    impl DurableStore for MemStore {
        fn append_event(&self, event: &Event) -> Result<(), StoreError> {
            let workflow_id = event_workflow_id(event).to_string();
            self.events
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event.clone());

            let mut steps = self.steps.lock().unwrap_or_else(|e| e.into_inner());
            match event {
                Event::WorkerRecovered { step_index, .. } => {
                    if let Some(row) = steps.get_mut(&(workflow_id.clone(), *step_index)) {
                        row.status = StepStatus::Pending;
                    }
                }
                Event::StepCompleted {
                    step_index, output, ..
                } => {
                    steps.insert(
                        (workflow_id, *step_index),
                        StepProjection {
                            status: StepStatus::Completed {
                                output: output.clone(),
                            },
                            attempt: 0,
                        },
                    );
                }
                _ => {}
            }
            Ok(())
        }

        fn load_events(&self, workflow_id: &str) -> Result<Vec<Event>, StoreError> {
            let events = self.events.lock().unwrap_or_else(|e| e.into_inner());
            Ok(events
                .iter()
                .filter(|e| event_workflow_id(e) == workflow_id)
                .cloned()
                .collect())
        }

        fn claim_step(
            &self,
            workflow_id: &str,
            step_index: usize,
            worker_id: &str,
            lease_ttl: Duration,
        ) -> Result<ClaimResult, StoreError> {
            let mut steps = self.steps.lock().unwrap_or_else(|e| e.into_inner());
            let key = (workflow_id.to_string(), step_index);
            let row = steps.get(&key);
            if let Some(row) = row {
                if let StepStatus::Completed { .. } = row.status {
                    return Ok(ClaimResult::AlreadyCompleted {
                        output: String::new(),
                    });
                }
                if let StepStatus::Waiting { .. } = row.status {
                    return Ok(ClaimResult::AlreadyWaiting);
                }
                if let StepStatus::Leased {
                    worker_id: holder,
                    expires_at,
                } = &row.status
                {
                    if holder != worker_id && *expires_at > SystemTime::now() {
                        return Ok(ClaimResult::HeldByOther);
                    }
                }
            }
            let attempt = row.map(|r| r.attempt).unwrap_or(0);
            let expires_at = SystemTime::now()
                .checked_add(lease_ttl)
                .unwrap_or_else(SystemTime::now);
            steps.insert(
                key,
                StepProjection {
                    status: StepStatus::Leased {
                        worker_id: worker_id.to_string(),
                        expires_at,
                    },
                    attempt,
                },
            );
            Ok(ClaimResult::Claimed)
        }

        fn renew_lease(
            &self,
            _workflow_id: &str,
            _step_index: usize,
            _worker_id: &str,
            _lease_ttl: Duration,
        ) -> Result<(), StoreError> {
            Ok(())
        }

        fn expired_leases(&self) -> Result<Vec<Execution>, StoreError> {
            let now = SystemTime::now();
            let steps = self.steps.lock().unwrap_or_else(|e| e.into_inner());
            let mut out = Vec::new();
            for ((workflow_id, step_index), row) in steps.iter() {
                if let StepStatus::Leased {
                    worker_id,
                    expires_at,
                } = &row.status
                {
                    if *expires_at <= now {
                        out.push(Execution {
                            workflow_id: workflow_id.clone(),
                            step_index: *step_index,
                            attempt: row.attempt,
                            worker_id: worker_id.clone(),
                            lease_expires_at: *expires_at,
                        });
                    }
                }
            }
            Ok(out)
        }
    }

    fn event_workflow_id(event: &Event) -> &str {
        match event {
            Event::WorkflowStarted { workflow_id } => workflow_id,
            Event::StepStarted { workflow_id, .. } => workflow_id,
            Event::StepCompleted { workflow_id, .. } => workflow_id,
            Event::StepFailed { workflow_id, .. } => workflow_id,
            Event::RetryScheduled { workflow_id, .. } => workflow_id,
            Event::StepWaiting { workflow_id, .. } => workflow_id,
            Event::StepResumed { workflow_id, .. } => workflow_id,
            Event::WorkflowCompleted { workflow_id } => workflow_id,
            Event::WorkflowFailed { workflow_id, .. } => workflow_id,
            Event::WorkflowCancelled { workflow_id, .. } => workflow_id,
            Event::WorkerRecovered { workflow_id, .. } => workflow_id,
        }
    }

    #[tokio::test]
    async fn recover_clears_expired_lease_and_logs_worker_recovered() {
        let queue = Queue::builder().start();
        let store = MemStore::new();
        let workflow_id = "wf-recover";
        store.seed_expired_lease(workflow_id, 0, 0, "dead-worker");

        let engine = WorkflowEngine::new(
            queue,
            store.clone(),
            "worker-1".to_string(),
            Duration::from_secs(60),
            Priority::High,
        );

        let body: StepFunc = Box::new(|| Box::pin(async { Ok("done".to_string()) }));
        let workflow = Workflow {
            id: workflow_id.to_string(),
            steps: vec![StepDef {
                name: "only".to_string(),
                retry_policy: RetryPolicy {
                    max_attempts: 1,
                    backoff: Backoff::Fixed(Duration::from_millis(1)),
                },
                timeout: None,
            }],
        };
        engine
            .register_workflow(workflow, vec![body])
            .expect("mount workflow");

        let count = recover(&engine).await.expect("recover");
        assert_eq!(count, 1);

        let status = store.step_status(workflow_id, 0).expect("step row exists");
        assert!(!matches!(
            status,
            StepStatus::Leased {
                expires_at,
                ..
            } if expires_at <= SystemTime::now()
        ));

        let events = store.load_events(workflow_id).expect("load events");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::WorkerRecovered { step_index: 0, .. }))
        );
    }

    #[tokio::test]
    async fn recover_continues_through_steps_after_the_recovered_one() {
        let queue = Queue::builder().start();
        let store = MemStore::new();
        let workflow_id = "wf-recover-continue";
        store.seed_expired_lease(workflow_id, 0, 0, "dead-worker");

        let engine = WorkflowEngine::new(
            queue,
            store.clone(),
            "worker-1".to_string(),
            Duration::from_secs(60),
            Priority::High,
        );

        let step1_ran = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let step1_ran_clone = step1_ran.clone();

        let step0: StepFunc = Box::new(|| Box::pin(async { Ok("recovered".to_string()) }));
        let step1: StepFunc = Box::new(move || {
            step1_ran_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Ok("second".to_string()) })
        });

        let no_retry = RetryPolicy {
            max_attempts: 1,
            backoff: Backoff::Fixed(Duration::from_millis(1)),
        };
        let workflow = Workflow {
            id: workflow_id.to_string(),
            steps: vec![
                StepDef {
                    name: "first".into(),
                    retry_policy: no_retry.clone(),
                    timeout: None,
                },
                StepDef {
                    name: "second".into(),
                    retry_policy: no_retry,
                    timeout: None,
                },
            ],
        };
        engine
            .register_workflow(workflow, vec![step0, step1])
            .expect("register workflow");

        recover(&engine).await.expect("recover");

        assert_eq!(
            step1_ran.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the step after the recovered one should run to completion, not just the recovered step"
        );
        let events = store.load_events(workflow_id).expect("load events");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::WorkflowCompleted { .. })),
            "workflow should reach WorkflowCompleted after recovery drives it through"
        );
    }
}
