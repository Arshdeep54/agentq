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
