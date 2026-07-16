use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{QueryError, RecordId},
};

use crate::{
    SurrealError,
    queries::{MAX_TX_RETRIES, TxOutcome, classify_tx_errors},
};

const KILL_TASK: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/kill_task.surql"
));

pub async fn kill_task(
    conn: &Surreal<Any>,
    id: &RecordId,
    reason: &str,
) -> Result<(), SurrealError> {
    let mut conflict = None;
    for _ in 0..MAX_TX_RETRIES {
        let mut response = conn
            .query(KILL_TASK)
            .bind(("id", id.clone()))
            .bind(("reason", reason.to_owned()))
            .await?;
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => conflict = Some(err),
            Some(TxOutcome::Fail(err)) => return Err(SurrealError::Database(err)),
            None => return Ok(()),
        }
    }
    // surface the conflict after exhausting retries so the poison row is not silently left Queued
    Err(SurrealError::Database(conflict.unwrap_or_else(|| {
        surrealdb::Error::query(
            "Transaction conflict retries exhausted".to_owned(),
            QueryError::TransactionConflict,
        )
    })))
}
