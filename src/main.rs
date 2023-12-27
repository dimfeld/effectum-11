use effectum::{Error, Job, JobRunner, Queue, RunningJob, Worker};
use futures::future::try_join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::future::pending;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{event, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug)]
pub struct JobContext {
    // database pool or other things here
}

#[derive(Serialize, Deserialize)]
struct RemindMePayload {
    email: String,
    message: String,
}

async fn remind_me_job(job: RunningJob, _context: Arc<JobContext>) -> Result<(), Error> {
    let payload: RemindMePayload = job.json_payload()?;
    event!(Level::INFO, job=%job.id, "running job");
    // do something with the job
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let env_filter = EnvFilter::try_from_env("LOG").unwrap_or_else(|_| EnvFilter::new("trace"));

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(env_filter)
        .init();

    // Create a queue
    let queue = Queue::new(&PathBuf::from("effectum.db")).await?;

    // Create workers
    let workers = (1..10)
        .into_iter()
        .map(|i| {
            let context = Arc::new(JobContext {});
            let a_job = JobRunner::builder("remind_me", remind_me_job).build();
            Worker::builder(&queue, context)
                .max_concurrency(5)
                .jobs([a_job])
                .build()
        })
        .collect_vec();

    let workers = try_join_all(workers)
        .await
        .expect("Failed to create workers");

    // Submit jobs to the queue
    println!("Submitting jobs to queue ..");
    let jobs = (1..100)
        .into_iter()
        .map(|i| {
            Job::builder("remind_me")
                .json_payload(&RemindMePayload {
                    email: "me@example.com".to_string(),
                    message: "Time to go!".to_string(),
                })
                .unwrap()
                .add_to(&queue)
        })
        .collect_vec();

    try_join_all(jobs).await.expect("Failed to create jobs");

    // Wait for jobs to be completed
    // tokio::time::sleep(Duration::from_secs(30)).await;
    for i in 1..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        // let counts = workers.iter().map(|w| w.counts()).collect_vec();
        // println!("worker 1: {} {}", counts[0].started, counts[0].finished);

        let total_counts = queue.num_active_jobs().await.unwrap();
        println!("total {} {}", total_counts.pending, total_counts.running);
    }

    // Verify no pending jobs
    let pending_count = queue.num_active_jobs().await.unwrap().pending;
    println!("Number of pending jobs = {}", pending_count);
    assert_eq!(pending_count, 0);

    Ok(())
}
