use apalis_core::backend::TaskSink;
use apalis_core::error::BoxDynError;
use apalis_core::task::builder::TaskBuilder;
use apalis_core::worker::builder::WorkerBuilder;
use apalis_core::worker::context::WorkerContext;
use apalis_surrealdb::{SurrealStorage, connect};

#[tokio::main]
async fn main() {
    let dedupe_key = "a5bc4337-7789-4feb-b421-89c7231bac10";

    let conn = std::sync::Arc::new(connect("mem://").await.unwrap());
    conn.use_ns("apalis").use_db("apalis").await.unwrap();
    SurrealStorage::setup(&conn).await.unwrap();
    let mut backend = SurrealStorage::new(&conn);

    let task_1 = TaskBuilder::new(42u32)
        .with_idempotency_key(dedupe_key)
        .build();
    let task_2 = TaskBuilder::new(43u32)
        .with_idempotency_key(dedupe_key)
        .build();

    // the second push is skipped, the key already exists
    backend.push_task(task_1).await.unwrap();
    backend.push_task(task_2).await.unwrap();

    async fn task(job: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        assert_eq!(job, 42);
        worker.stop()?;
        Ok(())
    }
    let worker = WorkerBuilder::new("worker-1").backend(backend).build(task);
    worker.run().await.unwrap();
}
