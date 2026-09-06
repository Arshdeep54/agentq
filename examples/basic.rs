use agentq::{Job, Priority, Queue, Response};

#[tokio::main]
async fn main() {
    // capacity: how many jobs may wait in each lane
    // permits:  how many jobs may run concurrently in each lane
    let mut queue = Queue::builder(100, 5);
    queue.run();

    let job = Job::new(
        "charge-order-4821".to_string(),
        Priority::High,
        Box::new(|| {
            Box::pin(async {
                // your tool call goes here
            })
        }),
    );

    match queue.push(job).await {
        Response::Successful => println!("queued"),
        Response::Duplicate => println!("already ran or still running, skipped"),
        Response::Failed => println!("could not queue"),
    }
}
