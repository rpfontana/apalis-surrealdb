use apalis_core::backend::{TaskSink, shared::MakeShared};
use apalis_core::error::BoxDynError;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::builder::WorkerBuilder;
use apalis_core::worker::context::WorkerContext;
use apalis_surrealdb::{SharedSurrealStorage, SurrealStorage, connect};

#[tokio::main]
async fn main() {
    let conn = std::sync::Arc::new(connect("mem://").await.unwrap());
    conn.use_ns("apalis").use_db("apalis").await.unwrap();
    SurrealStorage::setup(&conn).await.unwrap();

    let mut shared = SharedSurrealStorage::new(&conn);
    let mut string_store = shared.make_shared().unwrap();
    let mut int_store = shared.make_shared().unwrap();

    string_store.push("hello".to_owned()).await.unwrap();
    int_store.push(99u32).await.unwrap();

    async fn run<T, I>(_job: T, _id: TaskId<I>, worker: WorkerContext) -> Result<(), BoxDynError> {
        worker.stop()?;
        Ok(())
    }

    let int_worker = WorkerBuilder::new("ints").backend(int_store).build(run);
    let string_worker = WorkerBuilder::new("strings")
        .backend(string_store)
        .build(run);
    tokio::try_join!(int_worker.run(), string_worker.run()).unwrap();
}
