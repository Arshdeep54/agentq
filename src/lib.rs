#![doc = include_str!("../docs/agentq.md")]

mod engine;
mod error;
mod event;
mod handle;
mod job;
mod queue;
mod state;
mod store;
mod worker;
mod workflow;

pub use crate::engine::{EngineError, WaitForInput, WorkflowEngine};
pub use crate::error::{JobLost, PushError, WaitError};
pub use crate::event::{ClaimResult, Event};
pub use crate::handle::{JobHandle, Outcome};
pub use crate::job::{Func, Job, JobResult, Key, Priority};
pub use crate::queue::{Accepted, LaneConfig, Queue, QueueBuilder};
pub use crate::state::State;
pub use crate::store::{DurableStore, StoreError};
#[cfg(feature = "sqlite")]
pub use crate::store::SqliteStore;
pub use crate::workflow::{
    Backoff, Execution, RetryPolicy, StepDef, StepFunc, StepStatus, Workflow,
};
