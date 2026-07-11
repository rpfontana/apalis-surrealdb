use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{RecordId, Value},
};
use ulid::Ulid;

use crate::{JOB_TABLE, SurrealError};

const LOCK_TASK: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/lock_task.surql"
));

/// Transition a claimed task to Running, guarding on the owning worker
pub async fn lock_task(
    conn: &Surreal<Any>,
    task_id: &Ulid,
    worker: &str,
) -> Result<(), SurrealError> {
    let id = RecordId::new(JOB_TABLE, task_id.to_string());
    let mut response = conn
        .query(LOCK_TASK)
        .bind(("id", id))
        .bind(("worker", worker.to_owned()))
        .await?;
    let updated: Vec<Value> = response.take(0)?;
    if updated.is_empty() {
        return Err(SurrealError::TaskNotFound(task_id.to_string()));
    }
    Ok(())
}
