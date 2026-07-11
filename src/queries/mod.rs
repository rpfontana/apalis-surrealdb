use surrealdb::types::QueryError;

pub mod ack_task;
pub mod fetch_next;
pub mod keep_alive;
pub mod lock_task;
pub mod push_tasks;
pub mod register_worker;
pub mod reenqueue_orphaned;

pub(crate) const MAX_TX_RETRIES: usize = 5;

/// A transaction that lost an optimistic-concurrency race and is safe to replay
pub(crate) fn is_retryable_conflict(err: &surrealdb::Error) -> bool {
    matches!(err.query_details(), Some(QueryError::TransactionConflict))
        || err.message().contains("Transaction conflict")
}
