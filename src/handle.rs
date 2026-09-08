use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::sync::oneshot;

use crate::error::JobLost;

#[derive(Debug, Clone)]
pub enum Outcome {
    Completed { output: String },
    Failed { reason: String },
}

#[derive(Debug)]
pub struct JobHandle {
    receiver: oneshot::Receiver<Outcome>,
}

impl JobHandle {
    pub(crate) fn new(receiver: oneshot::Receiver<Outcome>) -> Self {
        JobHandle { receiver }
    }
}

impl Future for JobHandle {
    type Output = Result<Outcome, JobLost>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|result| result.map_err(|_| JobLost))
    }
}
