use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{Bytes, Datetime, QueryError, RecordId, SurrealValue},
};
use ulid::Ulid;

use crate::{
    CompactType, Config, JOB_TABLE, SurrealError, SurrealTask,
    queries::{MAX_TX_RETRIES, is_retryable_conflict},
};

const PUSH_TASKS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/push_tasks.surql"
));

#[derive(Clone, SurrealValue)]
#[surreal(crate = "surrealdb::types")]
struct PushRow {
    id: RecordId,
    args: Bytes,
    queue: String,
    max_attempts: i64,
    run_at: Datetime,
    priority: i64,
    metadata: serde_json::Value,
    idempotency_key: Option<String>,
}

/// Insert a batch of tasks in a single transaction, skipping duplicates by idempotency key
pub async fn push_tasks(
    conn: &Surreal<Any>,
    config: &Config,
    tasks: Vec<SurrealTask<CompactType>>,
) -> Result<(), SurrealError> {
    let queue = config.queue().to_string();
    let rows: Vec<PushRow> = tasks
        .into_iter()
        .map(|task| {
            let id = task
                .parts
                .task_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| Ulid::new().to_string());
            let run_at =
                Datetime::from_timestamp(task.parts.run_at as i64, 0).unwrap_or_else(Datetime::now);
            PushRow {
                id: RecordId::new(JOB_TABLE, id),
                args: Bytes::from(task.args),
                queue: queue.clone(),
                max_attempts: i64::from(task.parts.ctx.max_attempts()),
                run_at,
                priority: i64::from(task.parts.ctx.priority()),
                metadata: serde_json::Value::Object(task.parts.ctx.meta().clone()),
                idempotency_key: task.parts.idempotency_key,
            }
        })
        .collect();

    let mut conflict = None;
    for _ in 0..MAX_TX_RETRIES {
        let mut response = conn.query(PUSH_TASKS).bind(("tasks", rows.clone())).await?;
        let mut errors = response.take_errors().into_values();
        if let Some(err) = errors.find(is_retryable_conflict) {
            conflict = Some(err);
            continue;
        }
        if let Some(err) = errors.next() {
            return Err(SurrealError::Database(err));
        }
        return Ok(());
    }
    // surface the conflict after exhausting retries so the batch is not silently dropped
    Err(SurrealError::Database(conflict.unwrap_or_else(|| {
        surrealdb::Error::query(
            "Transaction conflict retries exhausted".to_owned(),
            QueryError::TransactionConflict,
        )
    })))
}
