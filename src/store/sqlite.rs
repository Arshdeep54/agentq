use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::{ClaimResult, Event, Execution};
use crate::store::{DurableStore, StoreError};

/// SQLite-backed [`DurableStore`] with an append-only event log and a `steps` projection.
pub struct SqliteStore {
    conn: Mutex<Connection>,
}

impl SqliteStore {
    /// Opens or creates the database at `path` and applies the schema.
    pub fn new(path: &str) -> Result<Self, StoreError> {
        let conn = Connection::open(path).map_err(|e| StoreError::Backend(e.to_string()))?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_workflow ON events(workflow_id, id);

            CREATE TABLE IF NOT EXISTS steps (
                workflow_id TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                status TEXT NOT NULL,
                worker_id TEXT,
                expires_at_secs INTEGER,
                expires_at_nanos INTEGER,
                output TEXT,
                reason TEXT,
                attempt INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (workflow_id, step_index)
            );
            ",
        )
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

fn lock_conn(store: &SqliteStore) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
    store
        .conn
        .lock()
        .map_err(|_| StoreError::Backend("sqlite connection mutex poisoned".to_string()))
}

impl DurableStore for SqliteStore {
    fn append_event(&self, event: &Event) -> Result<(), StoreError> {
        let workflow_id = event_workflow_id(event);
        let payload = encode_event(event);
        let mut conn = lock_conn(self)?;
        let tx = conn
            .transaction()
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        tx.execute(
            "INSERT INTO events (workflow_id, payload) VALUES (?1, ?2)",
            params![workflow_id, payload],
        )
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        apply_event_projection(&tx, event).map_err(|e| StoreError::Backend(e.to_string()))?;
        tx.commit()
            .map_err(|e| StoreError::Backend(e.to_string()))
    }

    fn load_events(&self, workflow_id: &str) -> Result<Vec<Event>, StoreError> {
        let conn = lock_conn(self)?;
        let mut stmt = conn
            .prepare("SELECT payload FROM events WHERE workflow_id = ?1 ORDER BY id ASC")
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let rows = stmt
            .query_map(params![workflow_id], |row| row.get::<_, String>(0))
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let mut events = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| StoreError::Backend(e.to_string()))?;
            events.push(decode_event(&payload)?);
        }
        Ok(events)
    }

    fn claim_step(
        &self,
        workflow_id: &str,
        step_index: usize,
        worker_id: &str,
        lease_ttl: Duration,
    ) -> Result<ClaimResult, StoreError> {
        let mut conn = lock_conn(self)?;
        let tx = conn
            .transaction()
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let result = claim_step_tx(&tx, workflow_id, step_index, worker_id, lease_ttl)
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        tx.commit()
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(result)
    }

    fn renew_lease(
        &self,
        workflow_id: &str,
        step_index: usize,
        worker_id: &str,
        lease_ttl: Duration,
    ) -> Result<(), StoreError> {
        let mut conn = lock_conn(self)?;
        let tx = conn
            .transaction()
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let row = load_step_row(&tx, workflow_id, step_index)
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let Some(row) = row else {
            return Err(StoreError::NotFound);
        };
        if row.status != "leased" || row.worker_id.as_deref() != Some(worker_id) {
            return Err(StoreError::Backend(
                "lease not held by worker".to_string(),
            ));
        }
        let (secs, nanos) = system_time_parts(
            SystemTime::now()
                .checked_add(lease_ttl)
                .unwrap_or_else(SystemTime::now),
        );
        let updated = tx
            .execute(
                "UPDATE steps SET expires_at_secs = ?1, expires_at_nanos = ?2
                 WHERE workflow_id = ?3 AND step_index = ?4 AND status = 'leased' AND worker_id = ?5",
                params![secs, nanos, workflow_id, step_index as i64, worker_id],
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        if updated == 0 {
            return Err(StoreError::Backend(
                "lease not held by worker".to_string(),
            ));
        }
        tx.commit()
            .map_err(|e| StoreError::Backend(e.to_string()))
    }

    fn expired_leases(&self) -> Result<Vec<Execution>, StoreError> {
        let (now_secs, now_nanos) = system_time_parts(SystemTime::now());
        let conn = lock_conn(self)?;
        let mut stmt = conn
            .prepare(
                "SELECT step_index, attempt, worker_id, expires_at_secs, expires_at_nanos
                 FROM steps
                 WHERE status = 'leased'
                   AND worker_id IS NOT NULL
                   AND (
                     expires_at_secs < ?1
                     OR (expires_at_secs = ?1 AND expires_at_nanos <= ?2)
                   )",
            )
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        let rows = stmt
            .query_map(params![now_secs, now_nanos], |row| {
                Ok((
                    row.get::<_, i64>(0)? as usize,
                    row.get::<_, i64>(1)? as u32,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            })
            .map_err(|e| StoreError::Backend(e.to_string()))?;

        let mut out = Vec::new();
        for row in rows {
            let (step_index, attempt, worker_id, expires_at_secs, expires_at_nanos) =
                row.map_err(|e| StoreError::Backend(e.to_string()))?;
            out.push(Execution {
                step_index,
                attempt,
                worker_id,
                lease_expires_at: system_time_from_parts(expires_at_secs, expires_at_nanos)?,
            });
        }
        Ok(out)
    }
}

struct StepRow {
    status: String,
    worker_id: Option<String>,
    expires_at_secs: Option<i64>,
    expires_at_nanos: Option<i64>,
    output: Option<String>,
    attempt: i64,
}

fn claim_step_tx(
    tx: &Transaction,
    workflow_id: &str,
    step_index: usize,
    worker_id: &str,
    lease_ttl: Duration,
) -> Result<ClaimResult, rusqlite::Error> {
    let row = load_step_row(tx, workflow_id, step_index)?;
    let now = SystemTime::now();

    if let Some(ref row) = row {
        if row.status == "completed" {
            return Ok(ClaimResult::AlreadyCompleted {
                output: row.output.clone().unwrap_or_default(),
            });
        }
        if row.status == "leased" {
            if let (Some(holder), Some(exp_secs), Some(exp_nanos)) = (
                row.worker_id.as_deref(),
                row.expires_at_secs,
                row.expires_at_nanos,
            ) {
                let expires_at =
                    system_time_from_parts_rusqlite(Some(exp_secs), Some(exp_nanos))?;
                if holder != worker_id && expires_at > now {
                    return Ok(ClaimResult::HeldByOther);
                }
            }
        }
    }

    let attempt = row.as_ref().map(|r| r.attempt).unwrap_or(0);
    let (expires_at_secs, expires_at_nanos) =
        system_time_parts(now.checked_add(lease_ttl).unwrap_or(now));

    tx.execute(
        "INSERT INTO steps (workflow_id, step_index, status, worker_id, expires_at_secs, expires_at_nanos, output, reason, attempt)
         VALUES (?1, ?2, 'leased', ?3, ?4, ?5, NULL, NULL, ?6)
         ON CONFLICT(workflow_id, step_index) DO UPDATE SET
           status = 'leased',
           worker_id = excluded.worker_id,
           expires_at_secs = excluded.expires_at_secs,
           expires_at_nanos = excluded.expires_at_nanos,
           output = NULL,
           reason = NULL,
           attempt = excluded.attempt",
        params![
            workflow_id,
            step_index as i64,
            worker_id,
            expires_at_secs,
            expires_at_nanos,
            attempt,
        ],
    )?;

    Ok(ClaimResult::Claimed)
}

fn load_step_row(
    tx: &Transaction,
    workflow_id: &str,
    step_index: usize,
) -> Result<Option<StepRow>, rusqlite::Error> {
    tx.query_row(
        "SELECT status, worker_id, expires_at_secs, expires_at_nanos, output, attempt
         FROM steps WHERE workflow_id = ?1 AND step_index = ?2",
        params![workflow_id, step_index as i64],
        |row| {
            Ok(StepRow {
                status: row.get(0)?,
                worker_id: row.get(1)?,
                expires_at_secs: row.get(2)?,
                expires_at_nanos: row.get(3)?,
                output: row.get(4)?,
                attempt: row.get(5)?,
            })
        },
    )
    .optional()
}

fn apply_event_projection(tx: &Transaction, event: &Event) -> Result<(), rusqlite::Error> {
    match event {
        Event::WorkflowStarted { .. } => Ok(()),
        Event::StepStarted {
            workflow_id,
            step_index,
            attempt,
        } => {
            upsert_step_status(
                tx,
                workflow_id,
                *step_index,
                "pending",
                None,
                None,
                None,
                None,
                None,
                Some(*attempt as i64),
            )
        }
        Event::StepCompleted {
            workflow_id,
            step_index,
            output,
        } => upsert_step_status(
            tx,
            workflow_id,
            *step_index,
            "completed",
            None,
            None,
            None,
            Some(output.as_str()),
            None,
            None,
        ),
        Event::StepFailed {
            workflow_id,
            step_index,
            reason,
        } => {
            let attempt = current_attempt(tx, workflow_id, *step_index)?.unwrap_or(0);
            upsert_step_status(
                tx,
                workflow_id,
                *step_index,
                "failed",
                None,
                None,
                None,
                None,
                Some(reason.as_str()),
                Some(attempt),
            )
        }
        Event::RetryScheduled {
            workflow_id,
            step_index,
            attempt,
        } => upsert_step_status(
            tx,
            workflow_id,
            *step_index,
            "pending",
            None,
            None,
            None,
            None,
            None,
            Some(*attempt as i64),
        ),
        Event::StepWaiting {
            workflow_id,
            step_index,
            reason,
        } => upsert_step_status(
            tx,
            workflow_id,
            *step_index,
            "waiting",
            None,
            None,
            None,
            None,
            Some(reason.as_str()),
            None,
        ),
        Event::StepResumed {
            workflow_id,
            step_index,
        } => {
            let attempt = current_attempt(tx, workflow_id, *step_index)?.unwrap_or(0);
            upsert_step_status(
                tx,
                workflow_id,
                *step_index,
                "pending",
                None,
                None,
                None,
                None,
                None,
                Some(attempt),
            )
        }
        Event::WorkerRecovered {
            workflow_id,
            step_index,
        } => {
            let attempt = current_attempt(tx, workflow_id, *step_index)?.unwrap_or(0);
            upsert_step_status(
                tx,
                workflow_id,
                *step_index,
                "pending",
                None,
                None,
                None,
                None,
                None,
                Some(attempt),
            )
        }
        Event::WorkflowCompleted { .. } | Event::WorkflowFailed { .. } => Ok(()),
    }
}

fn current_attempt(
    tx: &Transaction,
    workflow_id: &str,
    step_index: usize,
) -> Result<Option<i64>, rusqlite::Error> {
    tx.query_row(
        "SELECT attempt FROM steps WHERE workflow_id = ?1 AND step_index = ?2",
        params![workflow_id, step_index as i64],
        |row| row.get(0),
    )
    .optional()
}

#[allow(clippy::too_many_arguments)]
fn upsert_step_status(
    tx: &Transaction,
    workflow_id: &str,
    step_index: usize,
    status: &str,
    worker_id: Option<&str>,
    expires_at_secs: Option<i64>,
    expires_at_nanos: Option<i64>,
    output: Option<&str>,
    reason: Option<&str>,
    attempt: Option<i64>,
) -> Result<(), rusqlite::Error> {
    let attempt_val = if let Some(a) = attempt {
        a
    } else {
        current_attempt(tx, workflow_id, step_index)?.unwrap_or(0)
    };

    tx.execute(
        "INSERT INTO steps (workflow_id, step_index, status, worker_id, expires_at_secs, expires_at_nanos, output, reason, attempt)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(workflow_id, step_index) DO UPDATE SET
           status = excluded.status,
           worker_id = excluded.worker_id,
           expires_at_secs = excluded.expires_at_secs,
           expires_at_nanos = excluded.expires_at_nanos,
           output = excluded.output,
           reason = excluded.reason,
           attempt = excluded.attempt",
        params![
            workflow_id,
            step_index as i64,
            status,
            worker_id,
            expires_at_secs,
            expires_at_nanos,
            output,
            reason,
            attempt_val,
        ],
    )?;
    Ok(())
}

fn event_workflow_id(event: &Event) -> &str {
    match event {
        Event::WorkflowStarted { workflow_id } => workflow_id,
        Event::StepStarted { workflow_id, .. } => workflow_id,
        Event::StepCompleted { workflow_id, .. } => workflow_id,
        Event::StepFailed { workflow_id, .. } => workflow_id,
        Event::RetryScheduled { workflow_id, .. } => workflow_id,
        Event::StepWaiting { workflow_id, .. } => workflow_id,
        Event::StepResumed { workflow_id, .. } => workflow_id,
        Event::WorkflowCompleted { workflow_id } => workflow_id,
        Event::WorkflowFailed { workflow_id, .. } => workflow_id,
        Event::WorkerRecovered { workflow_id, .. } => workflow_id,
    }
}

fn encode_event(event: &Event) -> String {
    let mut lines = Vec::new();
    match event {
        Event::WorkflowStarted { workflow_id } => {
            lines.push("WorkflowStarted".to_string());
            lines.push(escape_field(workflow_id));
        }
        Event::StepStarted {
            workflow_id,
            step_index,
            attempt,
        } => {
            lines.push("StepStarted".to_string());
            lines.push(escape_field(workflow_id));
            lines.push(step_index.to_string());
            lines.push(attempt.to_string());
        }
        Event::StepCompleted {
            workflow_id,
            step_index,
            output,
        } => {
            lines.push("StepCompleted".to_string());
            lines.push(escape_field(workflow_id));
            lines.push(step_index.to_string());
            lines.push(escape_field(output));
        }
        Event::StepFailed {
            workflow_id,
            step_index,
            reason,
        } => {
            lines.push("StepFailed".to_string());
            lines.push(escape_field(workflow_id));
            lines.push(step_index.to_string());
            lines.push(escape_field(reason));
        }
        Event::RetryScheduled {
            workflow_id,
            step_index,
            attempt,
        } => {
            lines.push("RetryScheduled".to_string());
            lines.push(escape_field(workflow_id));
            lines.push(step_index.to_string());
            lines.push(attempt.to_string());
        }
        Event::StepWaiting {
            workflow_id,
            step_index,
            reason,
        } => {
            lines.push("StepWaiting".to_string());
            lines.push(escape_field(workflow_id));
            lines.push(step_index.to_string());
            lines.push(escape_field(reason));
        }
        Event::StepResumed {
            workflow_id,
            step_index,
        } => {
            lines.push("StepResumed".to_string());
            lines.push(escape_field(workflow_id));
            lines.push(step_index.to_string());
        }
        Event::WorkflowCompleted { workflow_id } => {
            lines.push("WorkflowCompleted".to_string());
            lines.push(escape_field(workflow_id));
        }
        Event::WorkflowFailed {
            workflow_id,
            reason,
        } => {
            lines.push("WorkflowFailed".to_string());
            lines.push(escape_field(workflow_id));
            lines.push(escape_field(reason));
        }
        Event::WorkerRecovered {
            workflow_id,
            step_index,
        } => {
            lines.push("WorkerRecovered".to_string());
            lines.push(escape_field(workflow_id));
            lines.push(step_index.to_string());
        }
    }
    lines.join("\n")
}

fn decode_event(payload: &str) -> Result<Event, StoreError> {
    let mut lines = payload.split('\n');
    let kind = lines.next().ok_or_else(|| StoreError::Backend("empty event payload".into()))?;
    match kind {
        "WorkflowStarted" => {
            let workflow_id = read_field(&mut lines)?;
            Ok(Event::WorkflowStarted { workflow_id })
        }
        "StepStarted" => {
            let workflow_id = read_field(&mut lines)?;
            let step_index = read_usize(&mut lines)?;
            let attempt = read_u32(&mut lines)?;
            Ok(Event::StepStarted {
                workflow_id,
                step_index,
                attempt,
            })
        }
        "StepCompleted" => {
            let workflow_id = read_field(&mut lines)?;
            let step_index = read_usize(&mut lines)?;
            let output = read_field(&mut lines)?;
            Ok(Event::StepCompleted {
                workflow_id,
                step_index,
                output,
            })
        }
        "StepFailed" => {
            let workflow_id = read_field(&mut lines)?;
            let step_index = read_usize(&mut lines)?;
            let reason = read_field(&mut lines)?;
            Ok(Event::StepFailed {
                workflow_id,
                step_index,
                reason,
            })
        }
        "RetryScheduled" => {
            let workflow_id = read_field(&mut lines)?;
            let step_index = read_usize(&mut lines)?;
            let attempt = read_u32(&mut lines)?;
            Ok(Event::RetryScheduled {
                workflow_id,
                step_index,
                attempt,
            })
        }
        "StepWaiting" => {
            let workflow_id = read_field(&mut lines)?;
            let step_index = read_usize(&mut lines)?;
            let reason = read_field(&mut lines)?;
            Ok(Event::StepWaiting {
                workflow_id,
                step_index,
                reason,
            })
        }
        "StepResumed" => {
            let workflow_id = read_field(&mut lines)?;
            let step_index = read_usize(&mut lines)?;
            Ok(Event::StepResumed {
                workflow_id,
                step_index,
            })
        }
        "WorkflowCompleted" => {
            let workflow_id = read_field(&mut lines)?;
            Ok(Event::WorkflowCompleted { workflow_id })
        }
        "WorkflowFailed" => {
            let workflow_id = read_field(&mut lines)?;
            let reason = read_field(&mut lines)?;
            Ok(Event::WorkflowFailed {
                workflow_id,
                reason,
            })
        }
        "WorkerRecovered" => {
            let workflow_id = read_field(&mut lines)?;
            let step_index = read_usize(&mut lines)?;
            Ok(Event::WorkerRecovered {
                workflow_id,
                step_index,
            })
        }
        other => Err(StoreError::Backend(format!("unknown event kind: {other}"))),
    }
}

fn escape_field(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\n', "\\n")
}

fn unescape_field(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn read_field<'a, I: Iterator<Item = &'a str>>(lines: &mut I) -> Result<String, StoreError> {
    let line = lines
        .next()
        .ok_or_else(|| StoreError::Backend("truncated event payload".into()))?;
    Ok(unescape_field(line))
}

fn read_usize<'a, I: Iterator<Item = &'a str>>(lines: &mut I) -> Result<usize, StoreError> {
    let line = lines
        .next()
        .ok_or_else(|| StoreError::Backend("truncated event payload".into()))?;
    line.parse()
        .map_err(|_| StoreError::Backend("invalid step_index in event payload".into()))
}

fn read_u32<'a, I: Iterator<Item = &'a str>>(lines: &mut I) -> Result<u32, StoreError> {
    let line = lines
        .next()
        .ok_or_else(|| StoreError::Backend("truncated event payload".into()))?;
    line.parse()
        .map_err(|_| StoreError::Backend("invalid attempt in event payload".into()))
}

fn system_time_parts(t: SystemTime) -> (i64, i64) {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    (dur.as_secs() as i64, dur.subsec_nanos() as i64)
}

fn system_time_from_parts(secs: Option<i64>, nanos: Option<i64>) -> Result<SystemTime, StoreError> {
    system_time_from_parts_rusqlite(secs, nanos).map_err(|e| StoreError::Backend(e.to_string()))
}

fn system_time_from_parts_rusqlite(
    secs: Option<i64>,
    nanos: Option<i64>,
) -> Result<SystemTime, rusqlite::Error> {
    let secs = secs.unwrap_or(0);
    let nanos = nanos.unwrap_or(0);
    Ok(UNIX_EPOCH + Duration::new(secs as u64, nanos as u32))
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn append_events_then_load_in_order() {
        let store = SqliteStore::new(":memory:").expect("open db");
        let wf = "wf-order";
        let events = [
            Event::WorkflowStarted {
                workflow_id: wf.to_string(),
            },
            Event::StepStarted {
                workflow_id: wf.to_string(),
                step_index: 0,
                attempt: 0,
            },
            Event::StepCompleted {
                workflow_id: wf.to_string(),
                step_index: 0,
                output: "done".to_string(),
            },
        ];
        for e in &events {
            store.append_event(e).expect("append");
        }
        let loaded = store.load_events(wf).expect("load");
        assert_eq!(loaded.len(), events.len());
        for (got, want) in loaded.iter().zip(events.iter()) {
            assert_eq!(encode_event(got), encode_event(want));
            assert_eq!(format!("{got:?}"), format!("{want:?}"));
        }
    }

    #[test]
    fn claim_step_claimed_then_held_by_other() {
        let store = SqliteStore::new(":memory:").expect("open db");
        let wf = "wf-claim";
        let ttl = Duration::from_secs(300);
        let first = store
            .claim_step(wf, 0, "worker-a", ttl)
            .expect("claim");
        assert!(matches!(first, ClaimResult::Claimed));
        let second = store
            .claim_step(wf, 0, "worker-b", ttl)
            .expect("claim");
        assert!(matches!(second, ClaimResult::HeldByOther));
    }
}
