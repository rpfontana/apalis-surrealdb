use std::time::Duration;

use apalis_core::backend::TaskSink;
use apalis_core::error::BoxDynError;
use apalis_core::task::Task;
use apalis_core::worker::builder::WorkerBuilder;
use apalis_core::worker::context::WorkerContext;
use apalis_surrealdb::{SurrealStorage, connect};

#[tokio::main]
async fn main() {
    let conn = std::sync::Arc::new(connect("mem://").await.unwrap());
    conn.use_ns("apalis").use_db("apalis").await.unwrap();
    SurrealStorage::setup(&conn).await.unwrap();
    let mut backend = SurrealStorage::new(&conn);

    // the task only becomes due two seconds from now
    let delayed = Task::builder(42u32)
        .run_after(Duration::from_secs(2))
        .build();
    backend.push_task(delayed).await.unwrap();

    async fn task(job: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        assert_eq!(job, 42);
        worker.stop()?;
        Ok(())
    }
    let worker = WorkerBuilder::new("worker-1").backend(backend).build(task);
    worker.run().await.unwrap();
}
