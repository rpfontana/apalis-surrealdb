use apalis_core::worker::context::WorkerContext;
use surrealdb::{Surreal, engine::any::Any, types::RecordId};

use crate::{Config, SurrealError, WORKER_TABLE};

const REGISTER_WORKER: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/register_worker.surql"
));

const WORKER_ALREADY_EXISTS: &str = "WORKER_ALREADY_EXISTS";

/// Register a worker, failing if a live instance already holds the name
pub async fn register_worker(
    conn: &Surreal<Any>,
    config: &Config,
    worker: &WorkerContext,
    storage_name: &str,
) -> Result<(), SurrealError> {
    let name = worker.name().to_owned();
    let id = RecordId::new(WORKER_TABLE, name.clone());
    let keep_alive = config.keep_alive().as_secs() as i64;

    let response = conn
        .query(REGISTER_WORKER)
        .bind(("worker", id))
        .bind(("queue", config.queue().to_string()))
        .bind(("storage", storage_name.to_owned()))
        .bind(("layers", worker.get_service().to_owned()))
        .bind(("keep_alive", keep_alive))
        .await;

    let mut response = match response {
        Ok(response) => response,
        Err(err) if is_already_exists(&err) => {
            return Err(SurrealError::WorkerAlreadyExists(name));
        }
        Err(err) => return Err(SurrealError::Database(err)),
    };

    let errors = response.take_errors();
    if errors.values().any(is_already_exists) {
        return Err(SurrealError::WorkerAlreadyExists(name));
    }
    if let Some(err) = errors.into_values().next() {
        return Err(SurrealError::Database(err));
    }
    Ok(())
}

fn is_already_exists(err: &surrealdb::Error) -> bool {
    err.is_thrown() && err.message().contains(WORKER_ALREADY_EXISTS)
}
