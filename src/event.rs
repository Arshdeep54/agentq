#[derive(Debug, Clone)]
pub enum Event {
    WorkflowStarted { workflow_id: String },
    StepStarted {
        workflow_id: String,
        step_index: usize,
        attempt: u32,
    },
    StepCompleted {
        workflow_id: String,
        step_index: usize,
        output: String,
    },
    StepFailed {
        workflow_id: String,
        step_index: usize,
        reason: String,
    },
    RetryScheduled {
        workflow_id: String,
        step_index: usize,
        attempt: u32,
    },
    StepWaiting {
        workflow_id: String,
        step_index: usize,
        reason: String,
    },
    StepResumed {
        workflow_id: String,
        step_index: usize,
    },
    WorkflowCompleted { workflow_id: String },
    WorkflowFailed {
        workflow_id: String,
        reason: String,
    },
    WorkerRecovered {
        workflow_id: String,
        step_index: usize,
    },
}

#[derive(Debug, Clone)]
pub enum ClaimResult {
    Claimed,
    AlreadyCompleted { output: String },
    HeldByOther,
}
