use apalis_sql::TaskRow;
use surrealdb::types::{Bytes, Datetime, RecordId, RecordIdKey, SurrealValue};

use crate::errors::SurrealError;

#[derive(Debug, SurrealValue)]
pub(crate) struct SurrealTaskRow {
    pub(crate) id: RecordId,
    pub(crate) args: Bytes,
    pub(crate) queue: String,
    pub(crate) status: String,
    pub(crate) attempts: i64,
    pub(crate) max_attempts: Option<i64>,
    pub(crate) run_at: Datetime,
    pub(crate) last_result: Option<serde_json::Value>,
    pub(crate) lock_at: Option<Datetime>,
    pub(crate) lock_by: Option<String>,
    pub(crate) done_at: Option<Datetime>,
    pub(crate) priority: i64,
    pub(crate) metadata: Option<serde_json::Value>,
    pub(crate) idempotency_key: Option<String>,
}

impl TryFrom<SurrealTaskRow> for TaskRow {
    type Error = SurrealError;

    fn try_from(row: SurrealTaskRow) -> Result<Self, Self::Error> {
        let RecordIdKey::String(id) = row.id.key else {
            return Err(SurrealError::MissingTaskId);
        };

        Ok(TaskRow {
            job: row.args.to_vec(),
            id,
            job_type: row.queue,
            status: row.status,
            attempts: row.attempts as usize,
            max_attempts: row.max_attempts.map(|value| value as usize),
            run_at: Some(row.run_at.into_inner()),
            last_result: row.last_result,
            lock_at: row.lock_at.map(Datetime::into_inner),
            lock_by: row.lock_by,
            done_at: row.done_at.map(Datetime::into_inner),
            priority: Some(row.priority as usize),
            metadata: row.metadata,
            idempotency_key: row.idempotency_key,
        })
    }
}
