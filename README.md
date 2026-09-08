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

- **Idempotency keys.** Every job carries a caller-supplied key. Push a key
  that already completed and you get that job's output back from cache
  instead of running it again. Push one that's still queued or running and
  you're told so, rather than starting a duplicate.
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
use agentq::{Accepted, Job, LaneConfig, Priority, Queue};

#[tokio::main]
async fn main() {
    // capacity: how many jobs may wait in this lane
    // permits:  how many jobs may run concurrently in this lane
    // lanes left unconfigured use LaneConfig::default()
    let queue = Queue::builder()
        .lane(
            Priority::High,
            LaneConfig {
                capacity: 32,
                permits: 1,
            },
        )
        .lane(
            Priority::Low,
            LaneConfig {
                capacity: 128,
                permits: 4,
            },
        )
        .start();

    let job = Job::new(
        "charge-order-4821".to_string(),
        Priority::High,
        Box::new(|| {
            Box::pin(async {
                // your tool call goes here.
                // return Err(...) to record the job as failed.
                Ok(())
            })
        }),
    );

    match queue.push(job).await {
        Ok(Accepted::Queued(_)) => println!("queued"),
        Ok(Accepted::Cached { output }) => println!("already ran, cached: {output}"),
        Ok(Accepted::InFlight) => println!("already running, skipped"),
        Err(e) => println!("could not queue: {e}"),
    }
}
```

A job returns `Result<String, Box<dyn Error + Send + Sync>>`. The `String` is
cached against the idempotency key, so a later push of the same key gets that
output back without re-executing. Returning `Err`
records the job as failed with the error's message attached, and leaves the
key retryable. A panic is caught too, and recorded separately, so an expected
failure and a bug in your job body don't look the same.

Ask about any key with `state`:

```rust
match queue.state("charge-order-4821") {
    Some(State::Completed { output }) => println!("done: {output}"),
    Some(State::Failed { reason }) => println!("failed: {reason}"),
    Some(other) => println!("in flight: {other:?}"),
    None => println!("never seen"),
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
- **No key expiry.** Dedup keys and their cached outputs are retained for the
  lifetime of the process. A long-running producer with unbounded distinct
  keys will grow memory.
- **No way to wait for a job.** There is no handle to await and no completion
  signal. If you get `InFlight`, your only option is to poll `state(key)`.
- **Lanes are isolated, not weighted.** Every lane currently gets the same
  capacity and the same permit count, and there is no arbitration between
  them. A `High` job doesn't preempt a `Low` one; they simply don't share a
  queue.
- **No persistence and no multi-node coordination.** Everything lives in the
  process. If it dies, queued jobs die with it.

## Roadmap

- Capped retries with exponential backoff
- Per-lane capacity and permit configuration
- Time-windowed dedup keys

## License

MIT
