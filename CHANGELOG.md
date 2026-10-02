# Changelog

## 0.2.1

- Fix `NonRetryable` leaking a workflow into a terminal race with a
  concurrent completion.
- Lock `cancel` and step completion against each other so a cancellation
  can't race a step that's finishing.
- Fix a worker's lease being erased when a step starts, instead of renewed.
- Renew the lease during step execution so long-running steps don't lose
  their lease mid-flight.
- Persist `resume`'s input in the durable event log.

## 0.2.0

Durable workflows, built additively on top of V1's `Queue`. A V1 consumer
who never touches workflows sees no behavior change.

### Added

- `Workflow` / `StepDef` / `RetryPolicy` / `Backoff` — an ordered list of
  steps under a stable id, each with its own retry policy.
- `WorkflowEngine` — drives a workflow's steps through the same `Queue` V1
  jobs use. Retry lives entirely in the engine; `Queue` itself is untouched
  and stays retry-agnostic.
- `WaitForInput` — a step returns this to block on something outside the
  process (an approval, a webhook). The engine records it as
  `StepStatus::Waiting` instead of a hard failure.
- `WorkflowEngine::resume` — unblocks a `Waiting` step with opaque input,
  then continues driving the remaining steps in order.
- `NonRetryable` — a step returns this to fail the workflow immediately,
  skipping the rest of its `RetryPolicy`'s attempts.
- `WorkflowEngine::cancel` — idempotently cancels a workflow; a step attempt
  already pushed to the queue is not aborted, but no further step runs.
- Per-step timeouts (`StepDef::timeout`) are now enforced: a step attempt
  that exceeds its timeout is treated as a failure and retried like any
  other. The original job keeps running in the background after a
  timeout — see the doc comment on `WorkflowEngine::run_step` for the
  exact tradeoff this makes.
- `DurableStore` — a trait for where step status and event history live.
  `SqliteStore` ships as the one concrete implementation, behind the
  optional `sqlite` Cargo feature (off by default — `rusqlite` never enters
  the dependency tree unless you ask for it).
- `SqliteStore::load_events_with_timestamps` — like `load_events`, with
  each event's recorded time. Existing SQLite databases are migrated
  automatically (one `ALTER TABLE`, applied once).
- `agentq::recover` — re-admits steps whose worker died mid-lease, found
  via `DurableStore::expired_leases`.
- A step claim that's already `Waiting` is now recognized as such
  (`ClaimResult::AlreadyWaiting`) instead of being treated as a fresh claim.
- `WorkflowCompleted` is no longer appended twice if a workflow's last step
  is re-driven after already completing.

### Known limitations

- `resume` and `agentq::recover` only work for workflows the current
  process has registered step bodies for (`WorkflowEngine::
  register_workflow`) — step bodies are `Fn` closures and can't be
  serialized, so a full process restart needs your code to re-register a
  workflow before resuming or recovering it. See the "Durable store" guide
  for why this is inherent, not an oversight.
- A step timeout does not cancel the underlying job; it can leave an
  orphaned execution running in the background with no event recorded for
  its eventual outcome.

## 0.1.0

Initial release: `Queue`, `Job`, `Priority` lanes, idempotency keys,
per-lane backpressure and bounded concurrency.
