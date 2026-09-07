use std::fmt;

#[derive(Debug)]
pub enum Accepted {
    Queued,
    Cached { output: String },
    InFlight,
}

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
