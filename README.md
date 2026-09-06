# agentq

An embeddable Rust job queue for agent tool calls. Idempotency keys so a retry
doesn't re-execute work that already succeeded, bounded lanes so producers get
backpressure instead of unbounded memory growth, and a concurrency limit per
lane so a burst can't fan out into hundreds of simultaneous calls.

No Redis. No separate service to run. It's a crate you drop into your own
process.

## The problem

Agent frameworks retry failed tool calls. If a call actually succeeded but the
acknowledgment was lost, a naive retry runs it again: duplicate writes,
duplicate charges, corrupted state. And when a batch of calls fails at once,
the retries all fire at once too, against whatever rate-limited or
metered API you were talking to.

Durable execution platforms solve this, but they want you to run a separate
service and adopt their workflow model. agentq is the small version: the two
primitives that stop the bleeding, embedded directly in your binary.

## What it does

- **Idempotency keys.** Every job carries a caller-supplied key. Push the same
  key while an earlier attempt is queued, running, or already completed, and
  the second push is rejected instead of executed.
- **Bounded lanes.** Each priority gets its own bounded channel. When a lane
  fills, `push` waits rather than letting the queue grow without limit.
- **Bounded concurrency.** Each lane has its own semaphore, so the number of
  jobs *running* at once is capped independently of how many are *queued*.
- **Failure isolation.** A job that panics doesn't take down its lane's
  worker, and its key is left in a retryable state rather than stuck forever.

## Install

```toml
[dependencies]
agentq = "0.1"
```

## Usage

```rust
use agentq::{Job, Priority, Queue, Response};

#[tokio::main]
async fn main() {
    // capacity: how many jobs may wait in each lane
    // permits:  how many jobs may run concurrently in each lane
    let mut queue = Queue::builder(100, 5);
    queue.run();

    let job = Job::new(
        "charge-order-4821".to_string(),
        Priority::High,
        Box::new(|| {
            Box::pin(async {
                // your tool call goes here
            })
        }),
    );

    match queue.push(job).await {
        Response::Successful => println!("queued"),
        Response::Duplicate => println!("already ran or still running, skipped"),
        Response::Failed => println!("could not queue"),
    }
}
```

`run()` takes `&mut self` and is called once. After that `push` only needs
`&self`, so you can wrap the queue in an `Arc` and push from as many tasks as
you like.

## How it works

Each `Priority` gets a bounded `tokio::mpsc` channel and its own
`tokio::sync::Semaphore`. `run()` spawns one task per lane, which pulls jobs
off that lane's channel. Before dispatching a job it acquires a permit from
the lane's semaphore, so a saturated lane applies backpressure at the channel
rather than piling up unbounded in-flight work.

Each job then runs in its own spawned task, holding that permit until it
finishes. That isolation is what keeps a panicking job from killing the lane,
and an RAII guard makes sure every job lands in a terminal state
(`Completed` or `Failed`) even if it panics partway through.

Dedup state lives in a single map keyed by idempotency key. `push` checks and
claims a key under one lock, so two concurrent pushes with the same key can't
both get through.

## Not in 0.1

Being explicit about what this doesn't do yet:

- **No automatic retries or backoff.** A failed job is recorded as failed. It
  is not re-attempted for you; re-pushing is the caller's decision.
- **No key expiry.** Dedup keys are retained for the lifetime of the process.
  A long-running producer with unbounded distinct keys will grow memory.
- **Lanes are isolated, not weighted.** Every lane currently gets the same
  capacity and the same permit count, and there is no arbitration between
  them. A `High` job doesn't preempt a `Low` one; they simply don't share a
  queue.
- **No persistence and no multi-node coordination.** Everything lives in the
  process. If it dies, queued jobs die with it.

## Roadmap

- A `status(key)` method, so callers can observe how a job ended
- Capped retries with exponential backoff
- Per-lane capacity and permit configuration
- Time-windowed dedup keys

## License

MIT
