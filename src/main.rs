use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use effectum::{Error, Job, JobState, JobRunner, RunningJob, Queue, Worker};
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

async fn remind_me_job(job: RunningJob, context: Arc<JobContext>) -> Result<(), Error> {
    let payload: RemindMePayload = job.json_payload()?;
    // do something with the job
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {

    SimpleLogger::new().init().unwrap();

    // Create a queue
    let queue = Queue::new(&PathBuf::from("effectum.db")).await?;

    // Define a type job for the queue.
    let context = Arc::new(JobContext{
        // database pool or other things here
    });

    let workers = (1..10).into_iter().map(|i| {
        let context = Arc::new(JobContext{});
        let a_job = JobRunner::builder("remind_me", remind_me_job).build();
        Worker::builder(&queue, context).max_concurrency(5).jobs([a_job]).build()
    }).collect_vec();

    try_join_all(workers).await.expect("Failed to create workers");

    // Submit a job to the queue.
    println!("Submitting job to queue ..");
    let jobs = (1..100).into_iter().map(|i| {
        Job::builder("remind_me")
            .json_payload(&RemindMePayload {
                email: "me@example.com".to_string(),
                message: "Time to go!".to_string()
            }).unwrap()
            .add_to(&queue)
    }).collect_vec();

    try_join_all(jobs).await.expect("Failed to create jobs");

    tokio::time::sleep(Duration::from_secs(30)).await;

    Ok(())
}