#[derive(Debug, Clone)]
pub struct StepDef {
    pub name: String,
    pub retry_policy: RetryPolicy,
    pub timeout: Option<std::time::Duration>,
}

#[derive(Debug, Clone)]
pub enum StepStatus {
    Pending,
    Leased {
        worker_id: String,
        expires_at: std::time::SystemTime,
    },
    Completed { output: String },
    Failed { reason: String, attempt: u32 },
    Waiting { reason: String },
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub backoff: Backoff,
}

#[derive(Debug, Clone)]
pub enum Backoff {
    Fixed(std::time::Duration),
    Exponential {
        base: std::time::Duration,
        max: std::time::Duration,
    },
}

#[derive(Debug, Clone)]
pub struct Workflow {
    pub id: String,
    pub steps: Vec<StepDef>,
}

#[derive(Debug, Clone)]
pub struct Execution {
    pub workflow_id: String,
    pub step_index: usize,
    pub attempt: u32,
    pub worker_id: String,
    pub lease_expires_at: std::time::SystemTime,
}

/// A step's body. `Fn`, not `FnOnce` like `Job`'s `Func` — the engine may
/// call this again across retry attempts, so it can't be consumed on first run.
pub type StepFunc = Box<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::JobResult> + Send>> + Send + Sync>;
