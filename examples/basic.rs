use agentq::{Accepted, Job, Priority, Queue, QueueConfig};

#[tokio::main]
async fn main() {
    // capacity: how many jobs may wait in each lane
    // permits:  how many jobs may run concurrently in each lane
    let mut queue = Queue::builder(QueueConfig {
        capacity: 100,
        permits: 5,
    });
    queue.run();

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
        Ok(Accepted::Queued) => println!("queued"),
        Ok(Accepted::Duplicate) => println!("already ran or still running, skipped"),
        Err(e) => println!("could not queue: {e}"),
    }
}
