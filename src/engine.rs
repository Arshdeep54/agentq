use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{
    Accepted, ClaimResult, DurableStore, Event, Job, JobLost, JobResult, Outcome, Priority, PushError,
    Queue, RetryPolicy, StepDef, StepFunc, StepStatus, StoreError, Workflow,
};

type SharedStepFunc =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = JobResult> + Send>> + Send + Sync>;

/// Marker error returned from a step body to enter [`StepStatus::Waiting`].
///
/// The worker turns this into a failed [`Outcome`]; the engine recognizes the
/// encoding and records [`Event::StepWaiting`] instead of treating it as a
/// hard failure.
#[derive(Debug)]
pub struct WaitForInput(pub String);

impl std::fmt::Display for WaitForInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{WAIT_PREFIX}{}", self.0)
    }
}

impl std::error::Error for WaitForInput {}

const WAIT_PREFIX: &str = "agentq:wait:";

fn parse_waiting_reason(reason: &str) -> Option<&str> {
    reason.strip_prefix(WAIT_PREFIX)
}

/// Drives workflow steps through the V1 [`Queue`] and records V2 [`Event`]s.
pub struct WorkflowEngine<S: DurableStore> {
    queue: Queue,
    store: S,
    worker_id: String,
    lease_ttl: Duration,
    priority: Priority,
    in_flight: Mutex<HashMap<String, RunningWorkflow>>,
    resume_inputs: Mutex<HashMap<(String, usize), String>>,
}

struct RunningWorkflow {
    steps: Vec<StepDef>,
    bodies: Vec<SharedStepFunc>,
}

/// Whether a step run finished the step or left it blocked on external input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StepOutcome {
    Completed,
    Waiting,
}

#[derive(Debug)]
pub enum EngineError {
    Store(StoreError),
    Push(PushError),
    JobLost(JobLost),
    WorkflowFailed { reason: String },
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Store(e) => write!(f, "{e}"),
            EngineError::Push(e) => write!(f, "{e}"),
            EngineError::JobLost(e) => write!(f, "{e}"),
            EngineError::WorkflowFailed { reason } => write!(f, "workflow failed: {reason}"),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::Store(e) => Some(e),
            EngineError::Push(e) => Some(e),
            EngineError::JobLost(e) => Some(e),
            EngineError::WorkflowFailed { .. } => None,
        }
    }
}

impl From<StoreError> for EngineError {
    fn from(err: StoreError) -> EngineError {
        EngineError::Store(err)
    }
}

impl<S: DurableStore> WorkflowEngine<S> {
    pub fn new(
        queue: Queue,
        store: S,
        worker_id: String,
        lease_ttl: Duration,
        priority: Priority,
    ) -> Self {
        WorkflowEngine {
            queue,
            store,
            worker_id,
            lease_ttl,
            priority,
            in_flight: Mutex::new(HashMap::new()),
            resume_inputs: Mutex::new(HashMap::new()),
        }
    }

    /// Registers a workflow's step bodies without running anything, so a
    /// later [`recover`](crate::recover) call (after a process restart) has
    /// something to re-admit an interrupted step into.
    pub fn register_workflow(
        &self,
        workflow: Workflow,
        bodies: Vec<StepFunc>,
    ) -> Result<(), EngineError> {
        if bodies.len() != workflow.steps.len() {
            return Err(EngineError::Store(StoreError::Backend(
                "step body count does not match workflow definition".into(),
            )));
        }

        let shared_bodies: Vec<SharedStepFunc> = bodies
            .into_iter()
            .map(|body| Arc::from(body) as SharedStepFunc)
            .collect();

        self.in_flight.lock().unwrap_or_else(|e| e.into_inner()).insert(
            workflow.id.clone(),
            RunningWorkflow {
                steps: workflow.steps,
                bodies: shared_bodies,
            },
        );
        Ok(())
    }

    /// Runs all steps in order, recording events and honoring per-step retry policy.
    ///
    /// Stops (without error) at the first step that enters
    /// [`StepStatus::Waiting`] — the workflow stays registered so a later
    /// [`Self::resume`] can continue it from that point.
    pub async fn run(&self, workflow: Workflow, bodies: Vec<StepFunc>) -> Result<(), EngineError> {
        let workflow_id = workflow.id.clone();
        self.register_workflow(workflow, bodies)?;

        self.store.append_event(&Event::WorkflowStarted {
            workflow_id: workflow_id.clone(),
        })?;

        let (steps, shared_bodies) = self.mounted_steps_and_bodies(&workflow_id)?;
        self.drive_from(&workflow_id, &steps, &shared_bodies, 0)
            .await
    }

    /// Unblocks a step in [`StepStatus::Waiting`], supplying opaque `input`
    /// for its next run, then continues driving the remaining steps in
    /// order — stopping again if a later step also enters `Waiting`.
    pub async fn resume(
        &self,
        workflow_id: &str,
        step_index: usize,
        input: String,
    ) -> Result<(), EngineError> {
        let events = self.store.load_events(workflow_id)?;
        let status = step_status_from_events(&events, step_index);
        match status {
            StepStatus::Waiting { .. } => {}
            _ => {
                return Err(EngineError::Store(StoreError::Backend(
                    "step is not waiting".into(),
                )));
            }
        }

        let (step, body) = {
            let guard = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            let running = guard.get(workflow_id).ok_or(StoreError::NotFound)?;
            if step_index >= running.steps.len() {
                return Err(EngineError::Store(StoreError::NotFound));
            }
            (
                running.steps[step_index].clone(),
                running.bodies[step_index].clone(),
            )
        };

        self.resume_inputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((workflow_id.to_string(), step_index), input);

        self.store.append_event(&Event::StepResumed {
            workflow_id: workflow_id.to_string(),
            step_index,
        })?;
        let attempt = current_attempt_from_events(&events, step_index);

        match self
            .run_step(workflow_id, step_index, &step, body, attempt)
            .await?
        {
            StepOutcome::Waiting => Ok(()),
            StepOutcome::Completed => {
                let (steps, bodies) = self.mounted_steps_and_bodies(workflow_id)?;
                self.drive_from(workflow_id, &steps, &bodies, step_index + 1)
                    .await
            }
        }
    }

    fn mounted_steps_and_bodies(
        &self,
        workflow_id: &str,
    ) -> Result<(Vec<StepDef>, Vec<SharedStepFunc>), EngineError> {
        let guard = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
        let running = guard
            .get(workflow_id)
            .ok_or(EngineError::Store(StoreError::NotFound))?;
        Ok((running.steps.clone(), running.bodies.clone()))
    }

    /// Runs steps `start_index..` in order. Returns `Ok(())` whether the
    /// workflow ran to completion or stopped early at a `Waiting` step —
    /// callers distinguish the two by checking the store, not this return
    /// value, since both are "no error" outcomes for the caller of `run`/
    /// `resume`. On full completion, records `WorkflowCompleted` and
    /// un-registers the workflow; on `Waiting`, leaves it registered.
    async fn drive_from(
        &self,
        workflow_id: &str,
        steps: &[StepDef],
        bodies: &[SharedStepFunc],
        start_index: usize,
    ) -> Result<(), EngineError> {
        for step_index in start_index..steps.len() {
            match self
                .run_step(workflow_id, step_index, &steps[step_index], bodies[step_index].clone(), 0)
                .await
            {
                Ok(StepOutcome::Completed) => continue,
                Ok(StepOutcome::Waiting) => return Ok(()),
                Err(err) => {
                    self.in_flight
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(workflow_id);
                    return Err(err);
                }
            }
        }

        self.store.append_event(&Event::WorkflowCompleted {
            workflow_id: workflow_id.to_string(),
        })?;
        self.in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(workflow_id);
        Ok(())
    }

    /// Opaque input supplied by the most recent [`Self::resume`] for this step, if any.
    pub fn resume_input(&self, workflow_id: &str, step_index: usize) -> Option<String> {
        self.resume_inputs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(workflow_id.to_string(), step_index))
            .cloned()
    }

    pub(crate) fn store(&self) -> &S {
        &self.store
    }

    /// Clears an expired lease in the store and re-runs the step when bodies
    /// are mounted (via [`Self::register_workflow`] or a prior [`Self::run`]
    /// in this same process) — continuing through the remaining steps if
    /// that step completes. If the workflow isn't mounted (nothing has
    /// re-registered it since a process restart), this only records
    /// [`Event::WorkerRecovered`] and does nothing further; the caller is
    /// responsible for calling [`Self::register_workflow`] first.
    pub(crate) async fn readmit_after_worker_recovery(
        &self,
        workflow_id: &str,
        step_index: usize,
        attempt: u32,
    ) -> Result<(), EngineError> {
        self.store.append_event(&Event::WorkerRecovered {
            workflow_id: workflow_id.to_string(),
            step_index,
        })?;

        let (step, body) = {
            let guard = self.in_flight.lock().unwrap_or_else(|e| e.into_inner());
            let Some(running) = guard.get(workflow_id) else {
                return Ok(());
            };
            if step_index >= running.steps.len() {
                return Ok(());
            }
            (
                running.steps[step_index].clone(),
                running.bodies[step_index].clone(),
            )
        };

        match self
            .run_step(workflow_id, step_index, &step, body, attempt)
            .await?
        {
            StepOutcome::Waiting => Ok(()),
            StepOutcome::Completed => {
                let (steps, bodies) = self.mounted_steps_and_bodies(workflow_id)?;
                self.drive_from(workflow_id, &steps, &bodies, step_index + 1)
                    .await
            }
        }
    }

    async fn run_step(
        &self,
        workflow_id: &str,
        step_index: usize,
        step: &StepDef,
        body: SharedStepFunc,
        start_attempt: u32,
    ) -> Result<StepOutcome, EngineError> {
        let mut attempt = start_attempt;

        loop {
            match self
                .store
                .claim_step(
                    workflow_id,
                    step_index,
                    &self.worker_id,
                    self.lease_ttl,
                )? {
                ClaimResult::AlreadyCompleted { .. } => return Ok(StepOutcome::Completed),
                ClaimResult::HeldByOther => {
                    return Err(EngineError::Store(StoreError::Backend(
                        "step is held by another worker".into(),
                    )));
                }
                ClaimResult::Claimed => {}
            }

            self.store.append_event(&Event::StepStarted {
                workflow_id: workflow_id.to_string(),
                step_index,
                attempt,
            })?;

            let key = format!("{workflow_id}:{step_index}:{attempt}");
            let func = body.clone();
            let job = Job::new(
                key,
                self.priority,
                Box::new(move || func()),
            );

            let handle = match self.queue.push(job).await {
                Ok(accepted) => match accepted {
                    Accepted::Queued(handle) | Accepted::InFlight(handle) => handle,
                    Accepted::Cached { output } => {
                        self.store.append_event(&Event::StepCompleted {
                            workflow_id: workflow_id.to_string(),
                            step_index,
                            output: output.clone(),
                        })?;
                        return Ok(StepOutcome::Completed);
                    }
                },
                Err(err) => return Err(EngineError::Push(err)),
            };

            let outcome = match handle.await {
                Ok(outcome) => outcome,
                Err(err) => return Err(EngineError::JobLost(err)),
            };

            match outcome {
                Outcome::Completed { output } => {
                    self.store.append_event(&Event::StepCompleted {
                        workflow_id: workflow_id.to_string(),
                        step_index,
                        output,
                    })?;
                    return Ok(StepOutcome::Completed);
                }
                Outcome::Failed { reason } => {
                    if let Some(wait_reason) = parse_waiting_reason(&reason) {
                        self.store.append_event(&Event::StepWaiting {
                            workflow_id: workflow_id.to_string(),
                            step_index,
                            reason: wait_reason.to_string(),
                        })?;
                        return Ok(StepOutcome::Waiting);
                    }

                    self.store.append_event(&Event::StepFailed {
                        workflow_id: workflow_id.to_string(),
                        step_index,
                        reason: reason.clone(),
                    })?;

                    let next_attempt = attempt + 1;
                    if next_attempt < step.retry_policy.max_attempts {
                        self.store.append_event(&Event::RetryScheduled {
                            workflow_id: workflow_id.to_string(),
                            step_index,
                            attempt: next_attempt,
                        })?;
                        let delay = backoff_duration(&step.retry_policy, attempt);
                        tokio::time::sleep(delay).await;
                        attempt = next_attempt;
                        continue;
                    }

                    self.store.append_event(&Event::WorkflowFailed {
                        workflow_id: workflow_id.to_string(),
                        reason: reason.clone(),
                    })?;
                    return Err(EngineError::WorkflowFailed { reason });
                }
            }
        }
    }
}

fn backoff_duration(policy: &RetryPolicy, failed_attempt: u32) -> Duration {
    match policy.backoff {
        crate::Backoff::Fixed(d) => d,
        crate::Backoff::Exponential { base, max } => {
            let mut duration = base;
            for _ in 0..failed_attempt {
                duration = duration.saturating_mul(2);
                if duration >= max {
                    return max;
                }
            }
            duration.min(max)
        }
    }
}

fn current_attempt_from_events(events: &[Event], step_index: usize) -> u32 {
    let mut attempt = 0u32;
    for event in events {
        match event {
            Event::StepStarted {
                step_index: si,
                attempt: a,
                ..
            } if *si == step_index => attempt = *a,
            Event::RetryScheduled {
                step_index: si,
                attempt: a,
                ..
            } if *si == step_index => attempt = *a,
            _ => {}
        }
    }
    attempt
}

fn step_status_from_events(events: &[Event], step_index: usize) -> StepStatus {
    let mut status = StepStatus::Pending;
    let mut attempt = 0u32;

    for event in events {
        match event {
            Event::StepStarted {
                step_index: si,
                attempt: a,
                ..
            } if *si == step_index => {
                attempt = *a;
                status = StepStatus::Pending;
            }
            Event::StepCompleted {
                step_index: si,
                output,
                ..
            } if *si == step_index => {
                status = StepStatus::Completed {
                    output: output.clone(),
                };
            }
            Event::StepFailed {
                step_index: si,
                reason,
                ..
            } if *si == step_index => {
                status = StepStatus::Failed {
                    reason: reason.clone(),
                    attempt,
                };
            }
            Event::StepWaiting {
                step_index: si,
                reason,
                ..
            } if *si == step_index => {
                status = StepStatus::Waiting {
                    reason: reason.clone(),
                };
            }
            Event::RetryScheduled {
                step_index: si,
                attempt: a,
                ..
            } if *si == step_index => {
                attempt = *a;
                status = StepStatus::Pending;
            }
            Event::StepResumed {
                step_index: si, ..
            } if *si == step_index => {
                status = StepStatus::Pending;
            }
            Event::WorkerRecovered {
                step_index: si, ..
            } if *si == step_index => {
                status = StepStatus::Pending;
            }
            _ => {}
        }
    }

    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Backoff, Execution};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct MemStore {
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl MemStore {
        fn new() -> Self {
            MemStore {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl DurableStore for MemStore {
        fn append_event(&self, event: &Event) -> Result<(), StoreError> {
            self.events
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event.clone());
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
            _workflow_id: &str,
            _step_index: usize,
            _worker_id: &str,
            _lease_ttl: Duration,
        ) -> Result<ClaimResult, StoreError> {
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
            Ok(Vec::new())
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
            Event::WorkerRecovered { workflow_id, .. } => workflow_id,
        }
    }

    fn workflow_completed(events: &[Event]) -> bool {
        events
            .iter()
            .any(|e| matches!(e, Event::WorkflowCompleted { .. }))
    }

    fn filter_step_lifecycle(events: &[Event]) -> Vec<Event> {
        events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::StepFailed { .. }
                        | Event::RetryScheduled { .. }
                        | Event::StepCompleted { .. }
                )
            })
            .cloned()
            .collect()
    }

    #[tokio::test]
    async fn retry_after_failure_then_completes() {
        let queue = Queue::builder().start();
        let store = MemStore::new();
        let engine = WorkflowEngine::new(
            queue,
            store.clone(),
            "worker-1".to_string(),
            Duration::from_secs(60),
            Priority::High,
        );

        let attempts = AtomicUsize::new(0);
        let body: StepFunc = Box::new(move || {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if n == 0 {
                    Err("first try failed".into())
                } else {
                    Ok("ok".to_string())
                }
            })
        });

        let workflow = Workflow {
            id: "wf-retry".to_string(),
            steps: vec![StepDef {
                name: "only".to_string(),
                retry_policy: RetryPolicy {
                    max_attempts: 2,
                    backoff: Backoff::Fixed(Duration::from_millis(1)),
                },
                timeout: None,
            }],
        };

        engine
            .run(workflow, vec![body])
            .await
            .expect("workflow should complete");

        let events = store.load_events("wf-retry").expect("load events");
        assert!(workflow_completed(&events));

        let lifecycle = filter_step_lifecycle(&events);
        assert_eq!(lifecycle.len(), 3);
        assert!(matches!(lifecycle[0], Event::StepFailed { .. }));
        assert!(matches!(lifecycle[1], Event::RetryScheduled { .. }));
        assert!(matches!(lifecycle[2], Event::StepCompleted { .. }));
    }

    #[tokio::test]
    async fn waiting_step_pauses_workflow_until_resumed_then_continues() {
        let queue = Queue::builder().start();
        let store = MemStore::new();
        let engine = WorkflowEngine::new(
            queue,
            store.clone(),
            "worker-1".to_string(),
            Duration::from_secs(60),
            Priority::High,
        );

        let step2_ran = Arc::new(AtomicUsize::new(0));
        let step2_ran_clone = step2_ran.clone();

        let step0: StepFunc = Box::new(|| Box::pin(async { Ok("first".to_string()) }));

        let step1_attempts = AtomicUsize::new(0);
        let step1: StepFunc = Box::new(move || {
            let n = step1_attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if n == 0 {
                    Err(Box::new(WaitForInput("need approval".to_string()))
                        as Box<dyn std::error::Error + Send + Sync>)
                } else {
                    Ok("approved".to_string())
                }
            })
        });

        let step2: StepFunc = Box::new(move || {
            step2_ran_clone.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok("done".to_string()) })
        });

        let no_retry = RetryPolicy {
            max_attempts: 1,
            backoff: Backoff::Fixed(Duration::from_millis(1)),
        };
        let workflow = Workflow {
            id: "wf-wait".to_string(),
            steps: vec![
                StepDef { name: "first".into(), retry_policy: no_retry.clone(), timeout: None },
                StepDef { name: "approval".into(), retry_policy: no_retry.clone(), timeout: None },
                StepDef { name: "last".into(), retry_policy: no_retry, timeout: None },
            ],
        };

        engine
            .run(workflow, vec![step0, step1, step2])
            .await
            .expect("run should not error while a step is waiting");

        assert_eq!(
            step2_ran.load(Ordering::SeqCst),
            0,
            "step after a Waiting step must not run before resume"
        );
        let events = store.load_events("wf-wait").expect("load events");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::StepWaiting { step_index: 1, .. }))
        );
        assert!(!workflow_completed(&events));

        engine
            .resume("wf-wait", 1, "approved by human".to_string())
            .await
            .expect("resume should drive the workflow to completion");

        assert_eq!(
            step2_ran.load(Ordering::SeqCst),
            1,
            "step after the resumed step should run exactly once after resume"
        );
        let events = store.load_events("wf-wait").expect("load events");
        assert!(workflow_completed(&events));
    }
}
