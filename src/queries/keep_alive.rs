use apalis_core::{timer::Delay, worker::context::WorkerContext};
use futures::{Stream, stream};
use std::sync::Arc;
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
    conn: &Arc<Surreal<Any>>,
    _config: &Config,
    worker: &WorkerContext,
    instance: &str,
) -> Result<(), SurrealError> {
    let id = RecordId::new(WORKER_TABLE, instance.to_owned());
    let mut response = conn.query(KEEP_ALIVE).bind(("worker", id)).await?;
    let updated: Vec<Value> = response.take(0)?;
    if updated.is_empty() {
        return Err(SurrealError::WorkerNotFound(format!(
            "{} (instance {instance})",
            worker.name()
        )));
    }
    Ok(())
}

/// Re-enqueue orphaned tasks then register the worker
pub async fn initial_heartbeat(
    conn: &Arc<Surreal<Any>>,
    config: &Config,
    worker: &WorkerContext,
    storage_name: &str,
    instance: &str,
) -> Result<(), SurrealError> {
    reenqueue_orphaned(conn, config).await?;
    register_worker(conn, config, worker, storage_name, instance).await?;
    Ok(())
}

/// Emit a keep-alive at every keep-alive interval
pub fn keep_alive_stream(
    conn: Arc<Surreal<Any>>,
    config: Config,
    worker: WorkerContext,
    instance: Arc<str>,
) -> impl Stream<Item = Result<(), SurrealError>> + Send {
    stream::unfold((), move |()| {
        let conn = conn.clone();
        let config = config.clone();
        let worker = worker.clone();
        let instance = instance.clone();
        async move {
            Delay::new(*config.keep_alive()).await;
            let res = keep_alive(&conn, &config, &worker, &instance).await;
            Some((res, ()))
        }
    })
}
