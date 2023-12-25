use std::future::pending;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use effectum::{Error, Job, JobRunner, RunningJob, Queue, Worker};
use serde::{Deserialize, Serialize};
use futures::future::try_join_all;
use itertools::Itertools;
use simple_logger::SimpleLogger;

#[derive(Debug)]
pub struct JobContext {
    // database pool or other things here
}

#[derive(Serialize, Deserialize)]
struct RemindMePayload {
    email: String,
    message: String,
}

async fn remind_me_job(_job: RunningJob, _context: Arc<JobContext>) -> Result<(), Error> {
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {

    SimpleLogger::new().init().unwrap();

    // Create a queue
    let queue = Queue::new(&PathBuf::from("effectum.db")).await?;

    // Create workers
    let workers = (1..10).into_iter().map(|i| {
        let context = Arc::new(JobContext{});
        let a_job = JobRunner::builder("remind_me", remind_me_job).build();
        Worker::builder(&queue, context).max_concurrency(5).jobs([a_job]).build()
    }).collect_vec();

    let _ = try_join_all(workers).await.expect("Failed to create workers");

    // Submit jobs to the queue
    println!("Submitting jobs to queue ..");
    let jobs = (1..100).into_iter().map(|i| {
        Job::builder("remind_me")
            .json_payload(&RemindMePayload {
                email: "me@example.com".to_string(),
                message: "Time to go!".to_string()
            }).unwrap()
            .add_to(&queue)
    }).collect_vec();

    // Only submit jobs if database is new
    let _ = try_join_all(jobs).await.expect("Failed to create jobs");

    // Wait for jobs to be completed
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Verify no pending jobs
    let pending_count = queue.num_active_jobs().await.unwrap().pending;
    println!("Number of pending jobs = {}", pending_count);
    assert_eq!(pending_count, 0);

    Ok(())
}