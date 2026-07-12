use apalis_codec::msgpack::MsgPackCodec;
use apalis_core::backend::TaskSink;
use apalis_core::error::BoxDynError;
use apalis_core::worker::builder::WorkerBuilder;
use apalis_core::worker::context::WorkerContext;
use apalis_surrealdb::{SurrealStorage, connect};

#[tokio::main]
async fn main() {
    let conn = connect("mem://").await.unwrap();
    conn.use_ns("apalis").use_db("apalis").await.unwrap();
    SurrealStorage::setup(&conn).await.unwrap();
    let mut backend = SurrealStorage::new(&conn).with_codec::<MsgPackCodec>();
    backend.push(42u32).await.unwrap();

    async fn task(job: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        assert_eq!(job, 42);
        worker.stop()?;
        Ok(())
    }
    let worker = WorkerBuilder::new("worker-1")
        .backend(backend)
        .build(task);
    worker.run().await.unwrap();
}
