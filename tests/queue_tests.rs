use agentq::{Accepted, Job, LaneConfig, Outcome, Priority, Queue, State, WaitError};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::test]
async fn test_duplicate_key() {
    let queue = Queue::builder().start();

    let job1 = Job::new(
        "dup-key".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Ok(String::new()) })),
    );
    let job2 = Job::new(
        "dup-key".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Ok(String::new()) })),
    );

    let first = queue.push(job1).await;
    let second = queue.push(job2).await;

    assert!(matches!(first, Ok(Accepted::Queued(_))));
    assert!(matches!(second, Ok(Accepted::InFlight(_))));
}

#[tokio::test]
async fn test_build() {
    let queue = Queue::builder().start();
    let (sender, receiver) = oneshot::channel();
    let job = Job::new(
        "key".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                println!("job running");
                let _ = sender.send("done".to_string());
                Ok("job output".to_string())
            })
        }),
    );

    assert!(matches!(queue.push(job).await, Ok(Accepted::Queued(_))));

    let signal = tokio::time::timeout(Duration::from_secs(2), receiver)
        .await
        .expect("job never ran")
        .expect("job never signalled");
    assert_eq!(signal, "done");
}

#[tokio::test]
async fn concurrency_never_exceeds_lane_permits() {
    const PERMITS: usize = 5;
    const JOB_COUNT: usize = 10;

    let queue = Queue::builder()
        .lane(
            Priority::High,
            LaneConfig {
                capacity: 50,
                permits: PERMITS,
            },
        )
        .start();

    let running = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));

    for i in 0..JOB_COUNT {
        let running = running.clone();
        let max_seen = max_seen.clone();
        let completed = completed.clone();

        let job = Job::new(
            format!("job-{i}"),
            Priority::High,
            Box::new(move || {
                Box::pin(async move {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, Ordering::SeqCst);

                    tokio::time::sleep(Duration::from_millis(50)).await;

                    running.fetch_sub(1, Ordering::SeqCst);
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(String::new())
                })
            }),
        );
        assert!(matches!(queue.push(job).await, Ok(Accepted::Queued(_))));
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while completed.load(Ordering::SeqCst) < JOB_COUNT && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        completed.load(Ordering::SeqCst),
        JOB_COUNT,
        "not all jobs completed before the deadline"
    );
    assert!(
        max_seen.load(Ordering::SeqCst) <= PERMITS,
        "expected at most {PERMITS} concurrent jobs, saw {}",
        max_seen.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn panic_in_one_job_does_not_kill_the_lane() {
    let queue = Queue::builder().start();

    let panicking_job = Job::new(
        "will-panic".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { panic!("boom") })),
    );
    assert!(matches!(
        queue.push(panicking_job).await,
        Ok(Accepted::Queued(_))
    ));

    let (tx, rx) = oneshot::channel();
    let survivor_job = Job::new(
        "survivor".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                let _ = tx.send(());
                Ok(String::new())
            })
        }),
    );
    assert!(matches!(
        queue.push(survivor_job).await,
        Ok(Accepted::Queued(_))
    ));

    let result = tokio::time::timeout(Duration::from_secs(2), rx).await;
    assert!(
        result.is_ok(),
        "lane appears dead: survivor job never ran after a panic"
    );
}

#[tokio::test]
async fn job_returning_err_is_recorded_as_failed() {
    let queue = Queue::builder().start();

    let (done_tx, done_rx) = oneshot::channel();
    let job = Job::new(
        "will-fail".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                let _ = done_tx.send(());
                Err("upstream returned 500".into())
            })
        }),
    );
    assert!(matches!(queue.push(job).await, Ok(Accepted::Queued(_))));

    tokio::time::timeout(Duration::from_secs(2), done_rx)
        .await
        .expect("job never ran")
        .expect("job never signalled");
    tokio::time::sleep(Duration::from_millis(100)).await;

    match queue.state("will-fail") {
        Some(State::Failed { reason }) => {
            assert_eq!(reason, "upstream returned 500");
        }
        other => panic!("expected Failed with a reason, got {other:?}"),
    }

    let retry = Job::new(
        "will-fail".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Ok(String::new()) })),
    );
    assert!(matches!(queue.push(retry).await, Ok(Accepted::Queued(_))));
}

#[tokio::test]
async fn panicked_job_key_can_be_retried() {
    let queue = Queue::builder().start();

    let (started_tx, started_rx) = oneshot::channel();
    let first_attempt = Job::new(
        "retry-me".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                let _ = started_tx.send(());
                panic!("boom");
            })
        }),
    );
    assert!(matches!(
        queue.push(first_attempt).await,
        Ok(Accepted::Queued(_))
    ));

    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .expect("first attempt never started")
        .expect("first attempt never signalled");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let retry = Job::new(
        "retry-me".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Ok(String::new()) })),
    );
    let response = queue.push(retry).await;

    assert!(
        matches!(response, Ok(Accepted::Queued(_))),
        "a panicked job left its key un-retryable, got {response:?}"
    );
}

#[tokio::test]
async fn completed_key_returns_cached_output_without_rerunning() {
    let queue = Queue::builder().start();

    let runs = Arc::new(AtomicUsize::new(0));
    let (done_tx, done_rx) = oneshot::channel();

    let runs_for_job = runs.clone();
    let first = Job::new(
        "cache-me".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                runs_for_job.fetch_add(1, Ordering::SeqCst);
                let _ = done_tx.send(());
                Ok("the answer".to_string())
            })
        }),
    );
    assert!(matches!(queue.push(first).await, Ok(Accepted::Queued(_))));

    tokio::time::timeout(Duration::from_secs(2), done_rx)
        .await
        .expect("job never ran")
        .expect("job never signalled");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let runs_for_second = runs.clone();
    let second = Job::new(
        "cache-me".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                runs_for_second.fetch_add(1, Ordering::SeqCst);
                Ok("should never run".to_string())
            })
        }),
    );

    match queue.push(second).await {
        Ok(Accepted::Cached { output }) => assert_eq!(output, "the answer"),
        other => panic!("expected cached output, got {other:?}"),
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the cached key was executed a second time"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pushes_of_same_key_admit_exactly_one() {
    let queue = Queue::builder()
        .lane(
            Priority::High,
            LaneConfig {
                capacity: 10,
                permits: 5,
            },
        )
        .start();
    let runs = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let queue = queue.clone();
        let runs = runs.clone();
        tasks.push(tokio::spawn(async move {
            let job = Job::new(
                "racy".to_string(),
                Priority::High,
                Box::new(move || {
                    Box::pin(async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok("done".to_string())
                    })
                }),
            );
            queue.push(job).await
        }));
    }

    let mut queued = 0;
    let mut handles = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            Ok(Accepted::Queued(handle)) => {
                queued += 1;
                handles.push(handle);
            }
            Ok(Accepted::InFlight(handle)) => handles.push(handle),
            other => panic!("unexpected push result: {other:?}"),
        }
    }

    assert_eq!(
        queued, 1,
        "more than one concurrent push was admitted for the same key"
    );
    assert_eq!(handles.len(), 8, "some callers were left without a handle");

    for handle in handles {
        match tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("a caller was never notified")
        {
            Ok(Outcome::Completed { output }) => assert_eq!(output, "done"),
            other => panic!("expected a completed outcome, got {other:?}"),
        }
    }

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the job body executed more than once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lanes_have_independent_concurrency_limits() {
    let queue = Queue::builder()
        .lane(
            Priority::Low,
            LaneConfig {
                capacity: 50,
                permits: 1,
            },
        )
        .lane(
            Priority::High,
            LaneConfig {
                capacity: 50,
                permits: 4,
            },
        )
        .start();

    let low_running = Arc::new(AtomicUsize::new(0));
    let high_max = Arc::new(AtomicUsize::new(0));
    let high_running = Arc::new(AtomicUsize::new(0));

    for i in 0..10 {
        let low_running = low_running.clone();
        queue
            .push(Job::new(
                format!("low-{i}"),
                Priority::Low,
                Box::new(move || {
                    Box::pin(async move {
                        low_running.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        low_running.fetch_sub(1, Ordering::SeqCst);
                        Ok(String::new())
                    })
                }),
            ))
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        low_running.load(Ordering::SeqCst),
        1,
        "Low lane exceeded its single permit"
    );

    for i in 0..8 {
        let high_running = high_running.clone();
        let high_max = high_max.clone();
        queue
            .push(Job::new(
                format!("high-{i}"),
                Priority::High,
                Box::new(move || {
                    Box::pin(async move {
                        let now = high_running.fetch_add(1, Ordering::SeqCst) + 1;
                        high_max.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        high_running.fetch_sub(1, Ordering::SeqCst);
                        Ok(String::new())
                    })
                }),
            ))
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(
        high_max.load(Ordering::SeqCst) > 1,
        "High lane was blocked by the saturated Low lane"
    );
    assert!(
        high_max.load(Ordering::SeqCst) <= 4,
        "High lane exceeded its permits, saw {}",
        high_max.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn handle_resolves_with_the_job_output() {
    let queue = Queue::builder().start();

    let job = Job::new(
        "with-handle".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Ok("the output".to_string()) })),
    );

    let Ok(Accepted::Queued(handle)) = queue.push(job).await else {
        panic!("expected the job to be queued");
    };

    match tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("handle never resolved")
    {
        Ok(Outcome::Completed { output }) => assert_eq!(output, "the output"),
        other => panic!("expected a completed outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn handle_resolves_when_awaited_after_the_job_already_finished() {
    let queue = Queue::builder().start();

    let job = Job::new(
        "already-done".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Ok("fast".to_string()) })),
    );

    let Ok(Accepted::Queued(handle)) = queue.push(job).await else {
        panic!("expected the job to be queued");
    };

    tokio::time::sleep(Duration::from_millis(200)).await;

    match tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("handle hung after the job had already completed")
    {
        Ok(Outcome::Completed { output }) => assert_eq!(output, "fast"),
        other => panic!("expected a completed outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn handle_reports_failure_when_the_job_returns_err() {
    let queue = Queue::builder().start();

    let job = Job::new(
        "will-error".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Err("upstream exploded".into()) })),
    );

    let Ok(Accepted::Queued(handle)) = queue.push(job).await else {
        panic!("expected the job to be queued");
    };

    match tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("handle never resolved")
    {
        Ok(Outcome::Failed { reason }) => assert_eq!(reason, "upstream exploded"),
        other => panic!("expected a failed outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn handle_resolves_when_the_job_panics() {
    let queue = Queue::builder().start();

    let job = Job::new(
        "will-panic".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { panic!("boom") })),
    );

    let Ok(Accepted::Queued(handle)) = queue.push(job).await else {
        panic!("expected the job to be queued");
    };

    match tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("handle hung after the job panicked")
    {
        Ok(Outcome::Failed { .. }) => {}
        other => panic!("expected a failed outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn dropping_the_handle_does_not_stop_the_job() {
    let queue = Queue::builder().start();

    let (done_tx, done_rx) = oneshot::channel();
    let job = Job::new(
        "detached".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                let _ = done_tx.send(());
                Ok("ran anyway".to_string())
            })
        }),
    );

    let Ok(Accepted::Queued(handle)) = queue.push(job).await else {
        panic!("expected the job to be queued");
    };
    drop(handle);

    tokio::time::timeout(Duration::from_secs(2), done_rx)
        .await
        .expect("job did not run after its handle was dropped")
        .expect("job never signalled");

    tokio::time::sleep(Duration::from_millis(100)).await;
    match queue.state("detached") {
        Some(State::Completed { output }) => assert_eq!(output, "ran anyway"),
        other => panic!("expected completed state, got {other:?}"),
    }
}

#[tokio::test]
async fn two_callers_of_one_key_share_a_single_execution_and_both_get_the_output() {
    let queue = Queue::builder().start();
    let runs = Arc::new(AtomicUsize::new(0));

    let runs_for_job = runs.clone();
    let first_job = Job::new(
        "shared-key".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                runs_for_job.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok("shared answer".to_string())
            })
        }),
    );
    let Ok(Accepted::Queued(first)) = queue.push(first_job).await else {
        panic!("expected the first push to be queued");
    };

    let runs_for_second = runs.clone();
    let second_job = Job::new(
        "shared-key".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                runs_for_second.fetch_add(1, Ordering::SeqCst);
                Ok("should never run".to_string())
            })
        }),
    );
    let Ok(Accepted::InFlight(second)) = queue.push(second_job).await else {
        panic!("expected the second push to join the in-flight job");
    };

    let first = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("first handle never resolved");
    let second = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .expect("in-flight handle never resolved");

    match (first, second) {
        (Ok(Outcome::Completed { output: a }), Ok(Outcome::Completed { output: b })) => {
            assert_eq!(a, "shared answer");
            assert_eq!(b, "shared answer");
        }
        other => panic!("expected both to complete, got {other:?}"),
    }

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the job executed more than once"
    );
}

#[tokio::test]
async fn in_flight_handle_reports_failure_of_the_running_job() {
    let queue = Queue::builder().start();

    let failing = Job::new(
        "shared-failure".to_string(),
        Priority::High,
        Box::new(|| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Err("upstream exploded".into())
            })
        }),
    );
    let Ok(Accepted::Queued(_owner)) = queue.push(failing).await else {
        panic!("expected the first push to be queued");
    };

    let joiner = Job::new(
        "shared-failure".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Ok(String::new()) })),
    );
    let Ok(Accepted::InFlight(handle)) = queue.push(joiner).await else {
        panic!("expected the second push to join the in-flight job");
    };

    match tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("in-flight handle never resolved")
    {
        Ok(Outcome::Failed { reason }) => assert_eq!(reason, "upstream exploded"),
        other => panic!("expected a failed outcome, got {other:?}"),
    }
}

#[tokio::test]
async fn push_and_wait_returns_the_output() {
    let queue = Queue::builder().start();

    let job = Job::new(
        "wait-for-me".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Ok("the answer".to_string()) })),
    );

    let output = tokio::time::timeout(Duration::from_secs(2), queue.push_and_wait(job))
        .await
        .expect("push_and_wait never returned")
        .expect("expected the job to succeed");

    assert_eq!(output, "the answer");
}

#[tokio::test]
async fn push_and_wait_surfaces_a_job_failure_as_an_error() {
    let queue = Queue::builder().start();

    let job = Job::new(
        "wait-for-failure".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { Err("upstream exploded".into()) })),
    );

    match tokio::time::timeout(Duration::from_secs(2), queue.push_and_wait(job))
        .await
        .expect("push_and_wait never returned")
    {
        Err(WaitError::Failed { reason }) => assert_eq!(reason, "upstream exploded"),
        other => panic!("expected a job failure, got {other:?}"),
    }
}

#[tokio::test]
async fn push_and_wait_returns_a_cached_output_without_rerunning() {
    let queue = Queue::builder().start();
    let runs = Arc::new(AtomicUsize::new(0));

    for _ in 0..3 {
        let runs = runs.clone();
        let job = Job::new(
            "cached-wait".to_string(),
            Priority::High,
            Box::new(move || {
                Box::pin(async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Ok("computed once".to_string())
                })
            }),
        );

        let output = tokio::time::timeout(Duration::from_secs(2), queue.push_and_wait(job))
            .await
            .expect("push_and_wait never returned")
            .expect("expected the job to succeed");

        assert_eq!(output, "computed once");
    }

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the job ran more than once across repeated push_and_wait calls"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_push_and_wait_on_one_key_runs_once_and_serves_everyone() {
    let queue = Queue::builder().start();
    let runs = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let queue = queue.clone();
        let runs = runs.clone();
        tasks.push(tokio::spawn(async move {
            let job = Job::new(
                "hot-key".to_string(),
                Priority::High,
                Box::new(move || {
                    Box::pin(async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Ok("one answer".to_string())
                    })
                }),
            );
            queue.push_and_wait(job).await
        }));
    }

    for task in tasks {
        let output = task.await.unwrap().expect("a caller did not get an output");
        assert_eq!(output, "one answer");
    }

    assert_eq!(runs.load(Ordering::SeqCst), 1, "the job ran more than once");
}

fn slow_job(key: &str) -> Job {
    Job::new(
        key.to_string(),
        Priority::High,
        Box::new(|| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                Ok("slow".to_string())
            })
        }),
    )
}

#[tokio::test]
async fn cancelling_a_blocked_push_releases_the_key() {
    let queue = Queue::builder()
        .lane(
            Priority::High,
            LaneConfig {
                capacity: 1,
                permits: 1,
            },
        )
        .start();

    for i in 0..3 {
        queue
            .push(slow_job(&format!("filler-{i}")))
            .await
            .expect("filler push failed");
    }

    let blocked = tokio::time::timeout(
        Duration::from_millis(100),
        queue.push(slow_job("cancelled-key")),
    )
    .await;
    assert!(
        blocked.is_err(),
        "expected the lane to be saturated so the push would block"
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;

    match queue.push(slow_job("cancelled-key")).await {
        Ok(Accepted::Queued(_)) => {}
        other => panic!("cancelled push left the key claimed, got {other:?}"),
    }
}

#[tokio::test]
async fn cancelling_a_blocked_push_resolves_a_joined_caller_with_job_lost() {
    let queue = Queue::builder()
        .lane(
            Priority::High,
            LaneConfig {
                capacity: 1,
                permits: 1,
            },
        )
        .start();

    for i in 0..3 {
        queue
            .push(slow_job(&format!("filler-{i}")))
            .await
            .expect("filler push failed");
    }

    let claimed = queue.clone();
    let blocked = tokio::spawn(async move { claimed.push(slow_job("abandoned")).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let Ok(Accepted::InFlight(handle)) = queue.push(slow_job("abandoned")).await else {
        panic!("expected the second caller to join the claimed key");
    };

    blocked.abort();

    match tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("joined caller hung after the claim was abandoned")
    {
        Err(_) => {}
        other => panic!("expected the joined caller to be told the job was lost, got {other:?}"),
    }
}

#[tokio::test]
async fn a_successful_push_is_not_undone_by_the_claim_guard() {
    let queue = Queue::builder().start();

    let output = tokio::time::timeout(
        Duration::from_secs(2),
        queue.push_and_wait(Job::new(
            "not-cancelled".to_string(),
            Priority::High,
            Box::new(|| Box::pin(async { Ok("ran".to_string()) })),
        )),
    )
    .await
    .expect("push_and_wait never returned")
    .expect("the job should have succeeded");

    assert_eq!(output, "ran");

    match queue.state("not-cancelled") {
        Some(State::Completed { output }) => assert_eq!(output, "ran"),
        other => panic!("expected the key to remain recorded, got {other:?}"),
    }
}
