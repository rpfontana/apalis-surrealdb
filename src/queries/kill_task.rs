use surrealdb::{Surreal, engine::any::Any, types::RecordId};

use crate::SurrealError;

const KILL_TASK: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/kill_task.surql"
));

pub async fn kill_task(
    conn: &Surreal<Any>,
    id: &RecordId,
    reason: &str,
) -> Result<(), SurrealError> {
    conn.query(KILL_TASK)
        .bind(("id", id.clone()))
        .bind(("reason", reason.to_owned()))
        .await?
        .check()?;
    Ok(())
}
