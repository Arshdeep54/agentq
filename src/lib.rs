#![doc = include_str!("../docs/agentq.md")]

mod error;
mod event;
mod handle;
mod job;
mod queue;
mod state;
mod workflow;
mod worker;

pub use crate::error::{JobLost, PushError, WaitError};
pub use crate::event::{ClaimResult, Event};
pub use crate::handle::{JobHandle, Outcome};
pub use crate::job::{Func, Job, JobResult, Key, Priority};
pub use crate::queue::{Accepted, LaneConfig, Queue, QueueBuilder};
pub use crate::state::State;
pub use crate::workflow::{
    Backoff, Execution, RetryPolicy, StepDef, StepFunc, StepStatus, Workflow,
};
