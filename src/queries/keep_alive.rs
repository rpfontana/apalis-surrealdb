use apalis_core::{timer::Delay, worker::context::WorkerContext};
use futures::{Stream, stream};
use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{RecordId, Value},
};

use crate::{
    Config, SurrealError, WORKER_TABLE,
    queries::{reenqueue_orphaned::reenqueue_orphaned, register_worker::register_worker},
};

const KEEP_ALIVE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/keep_alive.surql"
));

/// Refresh the worker heartbeat so it is not treated as orphaned
pub async fn keep_alive(
    conn: &Surreal<Any>,
    config: &Config,
    worker: &WorkerContext,
) -> Result<(), SurrealError> {
    let name = worker.name().to_owned();
    let id = RecordId::new(WORKER_TABLE, name.clone());
    let mut response = conn
        .query(KEEP_ALIVE)
        .bind(("worker", id))
        .bind(("queue", config.queue().to_string()))
        .await?;
    let updated: Vec<Value> = response.take(0)?;
    if updated.is_empty() {
        return Err(SurrealError::WorkerNotFound(name));
    }
    Ok(())
}

/// Re-enqueue orphaned tasks then register the worker
pub async fn initial_heartbeat(
    conn: &Surreal<Any>,
    config: &Config,
    worker: &WorkerContext,
    storage_name: &str,
) -> Result<(), SurrealError> {
    reenqueue_orphaned(conn, config).await?;
    register_worker(conn, config, worker, storage_name).await?;
    Ok(())
}

/// Emit a keep-alive at every keep-alive interval
pub fn keep_alive_stream(
    conn: Surreal<Any>,
    config: Config,
    worker: WorkerContext,
) -> impl Stream<Item = Result<(), SurrealError>> + Send {
    stream::unfold((), move |()| {
        let conn = conn.clone();
        let config = config.clone();
        let worker = worker.clone();
        async move {
            Delay::new(*config.keep_alive()).await;
            let res = keep_alive(&conn, &config, &worker).await;
            Some((res, ()))
        }
    })
}
