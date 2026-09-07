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
                Ok("done".to_string())
            })
        }),
    );

    match queue.push(job).await {
        Ok(Accepted::Queued) => println!("queued"),
        Ok(Accepted::Cached { output }) => println!("already ran, cached result: {output}"),
        Ok(Accepted::InFlight) => println!("already running, skipped"),
        Err(e) => println!("could not queue: {e}"),
    }
}
