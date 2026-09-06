use agentq::{Job, Priority, Queue, Response};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

#[tokio::test]
async fn test_duplicate_key() {
    let mut queue = Queue::builder(5, 5);
    queue.run();

    let job1 = Job::new(
        "dup-key".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async {})),
    );
    let job2 = Job::new(
        "dup-key".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async {})),
    );

    let first = queue.push(job1).await;
    let second = queue.push(job2).await;

    assert!(matches!(first, Response::Successful));
    assert!(matches!(second, Response::Duplicate));
}

#[tokio::test]
async fn test_build() {
    let mut queue = Queue::builder(5, 5);
    let (sender, receiver) = oneshot::channel();
    let job = Job::new(
        "key".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                println!("job running");
                let _ = sender.send("done".to_string());
            })
        }),
    );

    queue.run();
    let res = queue.push(job).await;
    println!("{:?}", res);

    let res = receiver.await;
    println!("from run {:?}", res);
}

#[tokio::test]
async fn concurrency_never_exceeds_lane_permits() {
    const PERMITS: usize = 5;
    const JOB_COUNT: usize = 10;

    let mut queue = Queue::builder(50, PERMITS);
    queue.run();

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
                })
            }),
        );
        queue.push(job).await;
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
    let mut queue = Queue::builder(10, 5);
    queue.run();

    let panicking_job = Job::new(
        "will-panic".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async { panic!("boom") })),
    );
    queue.push(panicking_job).await;

    let (tx, rx) = oneshot::channel();
    let survivor_job = Job::new(
        "survivor".to_string(),
        Priority::High,
        Box::new(move || {
            Box::pin(async move {
                let _ = tx.send(());
            })
        }),
    );
    queue.push(survivor_job).await;

    let result = tokio::time::timeout(Duration::from_secs(2), rx).await;
    assert!(
        result.is_ok(),
        "lane appears dead: survivor job never ran after a panic"
    );
}

#[tokio::test]
async fn panicked_job_key_can_be_retried() {
    let mut queue = Queue::builder(10, 5);
    queue.run();

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
        Response::Successful
    ));

    tokio::time::timeout(Duration::from_secs(2), started_rx)
        .await
        .expect("first attempt never started")
        .expect("first attempt never signalled");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let retry = Job::new(
        "retry-me".to_string(),
        Priority::High,
        Box::new(|| Box::pin(async {})),
    );
    let response = queue.push(retry).await;

    assert!(
        matches!(response, Response::Successful),
        "a panicked job left its key un-retryable, got {response:?}"
    );
}
