use std::fmt;

#[derive(Debug)]
pub enum PushError {
    LaneClosed,
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::LaneClosed => write!(f, "lane is closed; no worker is running"),
        }
    }
}

impl std::error::Error for PushError {}

#[derive(Debug)]
pub struct JobLost;

impl fmt::Display for JobLost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "job outcome was lost; the worker ended without reporting"
        )
    }
}

impl std::error::Error for JobLost {}

#[derive(Debug)]
pub enum WaitError {
    Push(PushError),
    Lost(JobLost),
    Failed { reason: String },
}

impl fmt::Display for WaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WaitError::Push(err) => write!(f, "could not queue the job: {err}"),
            WaitError::Lost(err) => write!(f, "{err}"),
            WaitError::Failed { reason } => write!(f, "the job failed: {reason}"),
        }
    }
}

impl std::error::Error for WaitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WaitError::Push(err) => Some(err),
            WaitError::Lost(err) => Some(err),
            WaitError::Failed { .. } => None,
        }
    }
}

impl From<PushError> for WaitError {
    fn from(err: PushError) -> WaitError {
        WaitError::Push(err)
    }
}

impl From<JobLost> for WaitError {
    fn from(err: JobLost) -> WaitError {
        WaitError::Lost(err)
    }
}
