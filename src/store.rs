use std::fmt;

#[derive(Debug)]
pub enum StoreError {
    NotFound,
    Backend(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::NotFound => write!(f, "workflow or step not found"),
            StoreError::Backend(msg) => write!(f, "store backend error: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}

pub trait DurableStore: Send + Sync {
    fn append_event(&self, event: &crate::Event) -> Result<(), StoreError>;
    fn load_events(&self, workflow_id: &str) -> Result<Vec<crate::Event>, StoreError>;
    fn claim_step(
        &self,
        workflow_id: &str,
        step_index: usize,
        worker_id: &str,
        lease_ttl: std::time::Duration,
    ) -> Result<crate::ClaimResult, StoreError>;
    fn renew_lease(
        &self,
        workflow_id: &str,
        step_index: usize,
        worker_id: &str,
        lease_ttl: std::time::Duration,
    ) -> Result<(), StoreError>;
    fn expired_leases(&self) -> Result<Vec<crate::Execution>, StoreError>;
}

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStore;
