use surrealdb::{Surreal, engine::any::Any};

use crate::{
    CompactType, SurrealError, SurrealTask,
    from_row::SurrealTaskRow,
    queries::{
        MAX_TX_RETRIES, TxOutcome, classify_tx_errors, fetch_next::decode_row, kill_task::kill_task,
    },
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
    let mut conflict = None;
    for _ in 0..MAX_TX_RETRIES {
        let mut response = conn
            .query(FETCH_NEXT_SHARED)
            .bind(("queues", queues.to_vec()))
            .bind(("limit", limit))
            .await?;
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => {
                conflict = Some(err);
                continue;
            }
            Some(TxOutcome::Fail(err)) => return Err(SurrealError::Database(err)),
            None => {}
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
    // surface the conflict after exhausting retries instead of faking an empty batch
    Err(SurrealError::Database(conflict.unwrap_or_else(|| {
        surrealdb::Error::query(
            "Transaction conflict retries exhausted".to_owned(),
            surrealdb::types::QueryError::TransactionConflict,
        )
    })))
}
