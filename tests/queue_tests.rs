use agentq::{Accepted, Job, LaneConfig, Outcome, Priority, Queue, State};
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
    assert!(matches!(second, Ok(Accepted::InFlight)));
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
    for task in tasks {
        if matches!(task.await.unwrap(), Ok(Accepted::Queued(_))) {
            queued += 1;
        }
    }

    assert_eq!(
        queued, 1,
        "more than one concurrent push was admitted for the same key"
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
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
