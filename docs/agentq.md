An embeddable job queue for agent tool calls.

agentq runs inside your own process. There is no Redis, no separate service,
and nothing to deploy. It exists to stop a retried unit of work from being
executed twice, and to keep a burst of work from overwhelming whatever is on
the other end of it.

# What it gives you

- **Idempotency keys.** Every job carries a caller-supplied key. Push a key
  that already completed and you get that job's output back from cache
  instead of running it again. Push one that is still running and you join it,
  receiving the same outcome when it lands.
- **Per-lane concurrency.** Each [`Priority`] lane has its own capacity and
  its own concurrency limit, so cheap work and expensive work can be given
  very different budgets, and a saturated lane never starves another.
- **Backpressure.** Lanes are bounded. When one fills, [`Queue::push`] waits
  rather than letting the queue grow without limit.
- **Failure isolation.** A job that returns an error, or panics outright,
  records a terminal state and leaves every other job untouched.

Longer guides, covering idempotency, lane configuration, error handling and
the internals, live at <https://agentq.hiesenbug.dev>.

# Getting started

Build a queue, push work into it, and wait for the result:

```no_run
use agentq::{Job, LaneConfig, Priority, Queue};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
# Ok(())
# }
```

[`Queue::push_and_wait`] returns the cached output if the key already
completed, joins the running job if the key is in flight, and otherwise
queues the work and waits for it. A job that returns `Err` surfaces as
[`WaitError::Failed`].

If you only want to enqueue without waiting, [`Queue::push`] returns an
[`Accepted`] describing which of those three things happened, carrying a
[`JobHandle`] you can await later.

# Jobs

A job is an async closure paired with an idempotency key and a priority. It
returns `Result<String, Box<dyn Error + Send + Sync>>`. The `String` is cached
against the key, so a later push of the same key gets that output back without
re-executing.

# Workflows

A workflow is an ordered list of [`StepDef`] entries under a stable id. Each
step carries a name for logging and UI, a [`RetryPolicy`] that caps attempts
and chooses backoff, and an optional per-step timeout.

While a workflow runs, every step is in some [`StepStatus`]: waiting to start,
leased to a worker, completed with output, failed after an attempt, or blocked
on an external condition. The engine records [`Event`] values along the way so
you can reconstruct what happened; workers learn whether they claimed a step
through [`ClaimResult`].

[`WorkflowEngine`] drives a [`Workflow`]'s steps through the same [`Queue`]
V1 jobs use — it is the only thing that calls [`Queue::push`] for a workflow
step. A failed step is retried per its [`RetryPolicy`] by the engine itself,
not by `Queue`; `Queue` stays exactly as retry-agnostic as it is for plain
jobs. A step blocked on something outside the process (approval, a webhook,
a human) ends up [`StepStatus::Waiting`], and [`WorkflowEngine::resume`]
unblocks it with whatever input arrived.

Where a step's status and event history live is up to a [`DurableStore`]
implementation, not the engine. [`SqliteStore`] ships as the one concrete
implementation, behind the `sqlite` Cargo feature, storing an append-only
event log alongside a queryable projection of each step's current status —
so a consumer who only wants V1's `Queue` never pulls in `rusqlite`. If you
need a different backend, implement [`DurableStore`] yourself; the engine
doesn't know or care what's behind the trait.

A worker that dies mid-step leaves its lease to expire; [`recover`] finds
those expired leases via [`DurableStore::expired_leases`] and re-admits the
steps, recording [`Event::WorkerRecovered`].

# Sharing a queue

[`Queue`] is cheap to clone and every clone refers to the same queue, so share
it across tasks with `queue.clone()` rather than wrapping it in an `Arc`.

Both [`Queue::push`] and [`Queue::push_and_wait`] are cancel safe. Dropping the
future before it completes, from a timeout or a `select!` branch, leaves no
claimed key behind: either the job was dispatched and runs to completion, or
the key is released and can be pushed again.

# How it works

Each [`Priority`] gets a bounded channel and its own semaphore, sized by the
[`LaneConfig`] you give it. [`QueueBuilder::start`] spawns one worker task per
lane, which pulls jobs off that lane's channel. Before dispatching a job the
worker acquires a permit from the lane's semaphore, so a saturated lane
applies backpressure at the channel rather than piling up unbounded in-flight
work.

Each job then runs in its own spawned task, holding that permit until it
finishes. That isolation is what keeps a panicking job from killing the lane,
and a drop guard makes sure every job lands in a terminal state even if it
panics partway through.

Dedup state lives in a single map keyed by idempotency key. [`Queue::push`]
checks and claims a key under one lock, so two concurrent pushes with the same
key cannot both get through.

# Caveats

A job that pushes to its own queue and waits on the result can deadlock, if
every permit in that lane is held by jobs doing the same thing. There is no
mechanism to release a permit while waiting.

Keys and their cached outputs are retained for the life of the process, so a
producer with unbounded distinct keys grows memory. Dropping the queue
abandons in-flight work.

[`Queue`] itself deliberately does not persist anything and does not
coordinate across processes. Everything lives in your binary; if it dies,
queued work dies with it. If you need a job's state to survive a restart, use
a [`Workflow`] with a [`DurableStore`] (see above) rather than the raw
`Queue` — but agentq still does not coordinate across machines; if you need
that, you want a different tool.

# Upcoming

- Capped retries with exponential backoff. A failed job is currently recorded
  as failed and not re-attempted; re-pushing is the caller's decision.
- Time-windowed keys, so cached outputs expire rather than accumulating.
- Graceful shutdown, draining in-flight work before exit.
- Arbitration between lanes, so a `High` job can preempt a `Low` one rather
  than the lanes merely being independent.
