use apalis_core::{
    error::{AbortError, BoxDynError},
    task::{Parts, status::Status},
};
use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{RecordId, Value},
};
use ulid::Ulid;

use crate::{JOB_TABLE, SurrealContext, SurrealError};

const ACK_TASK: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/ack_task.surql"
));

/// Record the outcome of a task, guarding on the owning worker
pub async fn ack_task(
    conn: &Surreal<Any>,
    task_id: &Ulid,
    worker: &str,
    result: serde_json::Value,
    status: &Status,
    attempt: i64,
) -> Result<(), SurrealError> {
    let id = RecordId::new(JOB_TABLE, task_id.to_string());
    let mut response = conn
        .query(ACK_TASK)
        .bind(("id", id))
        .bind(("worker", worker.to_owned()))
        .bind(("status", status.to_string()))
        .bind(("attempts", attempt))
        .bind(("result", result))
        .await?;
    let updated: Vec<Value> = response.take(0)?;
    if updated.is_empty() {
        return Err(SurrealError::TaskNotFound(task_id.to_string()));
    }
    Ok(())
}

/// Decide the terminal status from the task outcome and remaining attempts
pub fn calculate_status<Res>(
    parts: &Parts<SurrealContext, Ulid>,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match res {
        Ok(_) => Status::Done,
        Err(e) => match e {
            _ if parts.ctx.max_attempts() as usize <= parts.attempt.current() => Status::Killed,
            e if e.downcast_ref::<AbortError>().is_some() => Status::Killed,
            _ => Status::Failed,
        },
    }
}
