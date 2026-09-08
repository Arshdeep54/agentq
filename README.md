# agentq

[![crates.io](https://img.shields.io/crates/v/agentq.svg)](https://crates.io/crates/agentq)
[![docs.rs](https://docs.rs/agentq/badge.svg)](https://docs.rs/agentq)
[![guides](https://img.shields.io/badge/guides-agentq.hiesenbug.dev-0D9488)](https://agentq.hiesenbug.dev)
[![CI](https://github.com/Arshdeep54/agentq/actions/workflows/rust.yml/badge.svg)](https://github.com/Arshdeep54/agentq/actions/workflows/rust.yml)
[![license](https://img.shields.io/crates/l/agentq.svg)](LICENSE)

> An embeddable job queue for agent tool calls. Idempotency keys so a retry
> doesn't re-execute work that already succeeded, bounded lanes for
> backpressure, and a concurrency limit per lane.
>
> No Redis. No separate service. A crate you drop into your own process.

---

## Why

Agent frameworks retry failed tool calls. If a call actually succeeded but the
acknowledgment was lost, a naive retry runs it again: duplicate writes,
duplicate charges, corrupted state. And when a batch of calls fails at once,
the retries all fire at once too, against whatever rate-limited or metered API
you were talking to.

Durable execution platforms solve this, but they want you to run a separate
service and adopt their workflow model. agentq is the small version: the
primitives that stop the bleeding, embedded directly in your binary.

## Features

- **Idempotency keys.** Push a key that already completed and you get that
  job's output back from cache instead of running it again. Push one that's
  still running and you join it, receiving the same outcome when it lands.
- **Per-lane concurrency.** Each priority lane has its own capacity and its
  own concurrency limit, so cheap work and expensive work get different
  budgets and a saturated lane never starves another.
- **Backpressure.** Lanes are bounded. When one fills, `push` waits rather
  than letting the queue grow without limit.
- **Failure isolation.** A job that returns an error, or panics outright,
  records a terminal state and leaves every other job untouched.
- **Cancel safe.** Dropping a `push` future part-way through leaves no
  claimed key behind.

## Quick start

```toml
[dependencies]
agentq = "0.1"
```

```rust
use agentq::{Job, LaneConfig, Priority, Queue};

let queue = Queue::builder()
    .lane(Priority::High, LaneConfig { capacity: 32, permits: 1 })
    .lane(Priority::Low, LaneConfig { capacity: 128, permits: 4 })
    .start();

let job = Job::new(
    "charge-order-4821".to_string(),
    Priority::High,
    Box::new(|| Box::pin(async { Ok("charged".to_string()) })),
);

let output = queue.push_and_wait(job).await?;
```

Guides covering idempotency, priority lanes, error handling and the
architecture are at **[agentq.hiesenbug.dev](https://agentq.hiesenbug.dev)**.
The generated API reference is on [docs.rs](https://docs.rs/agentq).

## Caveats

A job that pushes to its own queue and waits on the result can deadlock, if
every permit in that lane is held by jobs doing the same thing.

Keys and their cached outputs are retained for the life of the process, so a
producer with unbounded distinct keys grows memory. Dropping the queue
abandons in-flight work.

agentq deliberately does not persist anything and does not coordinate across
processes. If you need durability or a queue shared across machines, you want
a different tool.

## Upcoming

- Capped retries with exponential backoff
- Time-windowed keys, so cached outputs expire
- Graceful shutdown, draining in-flight work before exit
- Arbitration between lanes, so `High` can preempt `Low`

## License

MIT
