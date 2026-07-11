use surrealdb::types::QueryError;

pub mod ack_task;
pub mod fetch_by_id;
pub mod fetch_next;
pub mod fetch_next_shared;
pub mod keep_alive;
pub mod list_queues;
pub mod list_tasks;
pub mod list_workers;
pub mod lock_task;
pub mod metrics;
pub mod push_tasks;
pub mod register_worker;
pub mod reenqueue_orphaned;
pub mod vacuum;
pub mod wait_for;

pub(crate) const MAX_TX_RETRIES: usize = 5;

/// A transaction that lost an optimistic-concurrency race and is safe to replay
pub(crate) fn is_retryable_conflict(err: &surrealdb::Error) -> bool {
    matches!(err.query_details(), Some(QueryError::TransactionConflict))
        || err.message().contains("Transaction conflict")
}
