use apalis_core::worker::context::WorkerContext;
use surrealdb::{Surreal, engine::any::Any, types::RecordId};

use crate::{Config, SurrealError, WORKER_TABLE};

const REGISTER_WORKER: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/register_worker.surql"
));

/// Register a worker, taking over the name if it is already held
pub async fn register_worker(
    conn: &Surreal<Any>,
    config: &Config,
    worker: &WorkerContext,
    storage_name: &str,
    instance: &str,
) -> Result<(), SurrealError> {
    let name = worker.name().to_owned();
    let id = RecordId::new(WORKER_TABLE, name.clone());
    let keep_alive = config.keep_alive().as_secs() as i64;

    let mut response = conn
        .query(REGISTER_WORKER)
        .bind(("worker", id))
        .bind(("queue", config.queue().to_string()))
        .bind(("storage", storage_name.to_owned()))
        .bind(("layers", worker.get_service().to_owned()))
        .bind(("instance", instance.to_owned()))
        .bind(("keep_alive", keep_alive))
        .await?;

    if let Some(err) = response.take_errors().into_values().next() {
        return Err(SurrealError::Database(err));
    }

    let live: Option<bool> = response.take(4)?;
    if live.unwrap_or(false) {
        log::warn!(
            "Worker {name} was still heartbeating within keep_alive; taking over the name. \
             Two live workers sharing a name will fight over the heartbeat."
        );
    }
    Ok(())
}
