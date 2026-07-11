use apalis_sql::TaskRow;
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{
    CompactType, SurrealError, SurrealTask,
    from_row::SurrealTaskRow,
    queries::{MAX_TX_RETRIES, is_retryable_conflict},
};

const FETCH_NEXT_SHARED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/fetch_next_shared.surql"
));

/// Atomically claim the next batch of due tasks across several queues for the shared driver
pub async fn fetch_next_shared(
    conn: &Surreal<Any>,
    queues: &[String],
    limit: i64,
) -> Result<Vec<SurrealTask<CompactType>>, SurrealError> {
    for _ in 0..MAX_TX_RETRIES {
        let mut response = conn
            .query(FETCH_NEXT_SHARED)
            .bind(("queues", queues.to_vec()))
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
        return rows
            .into_iter()
            .map(|row| {
                let task_row: TaskRow = row.try_into()?;
                Ok(task_row.try_into_task_compact::<Ulid, Surreal<Any>>()?)
            })
            .collect();
    }
    Ok(Vec::new())
}
