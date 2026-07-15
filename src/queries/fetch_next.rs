use apalis_sql::TaskRow;
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{
    CompactType, Config, SurrealError, SurrealTask,
    from_row::SurrealTaskRow,
    queries::{MAX_TX_RETRIES, is_retryable_conflict, kill_task::kill_task},
};

const FETCH_NEXT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/fetch_next.surql"
));

/// Atomically claim the next batch of due tasks for a worker
pub async fn fetch_next(
    conn: &Surreal<Any>,
    config: &Config,
    instance: &str,
) -> Result<Vec<SurrealTask<CompactType>>, SurrealError> {
    let queue = config.queue().to_string();
    let limit = config.buffer_size() as i64;
    let worker = instance.to_owned();

    for _ in 0..MAX_TX_RETRIES {
        let mut response = conn
            .query(FETCH_NEXT)
            .bind(("queue", queue.clone()))
            .bind(("worker", worker.clone()))
            .bind(("limit", limit))
            .await?;
        let errors = response.take_errors();
        if errors.values().any(is_retryable_conflict) {
            continue;
        }
        if let Some(err) = errors.into_values().next() {
            return Err(SurrealError::Database(err));
        }
        // BEGIN and COMMIT each occupy a result index, so the UPDATE is at 2
        let rows: Vec<SurrealTaskRow> = response.take(2)?;
        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let id = row.id.clone();
            match decode_row(row) {
                Ok(task) => tasks.push(task),
                Err(e) => {
                    log::error!("Killing task {id:?} the worker cannot decode: {e}");
                    if let Err(e) = kill_task(conn, &id, &e.to_string()).await {
                        log::error!("Failed to kill undecodable task {id:?}: {e}");
                    }
                }
            }
        }
        return Ok(tasks);
    }
    Ok(Vec::new())
}

fn decode_row(row: SurrealTaskRow) -> Result<SurrealTask<CompactType>, SurrealError> {
    let task_row: TaskRow = row.try_into()?;
    Ok(task_row.try_into_task_compact::<Ulid, Surreal<Any>>()?)
}
