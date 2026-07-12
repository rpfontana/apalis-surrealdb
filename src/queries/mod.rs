use surrealdb::types::QueryError;

/// Acknowledge a task on success or failure
pub mod ack_task;
/// Fetch a single task by its id
pub mod fetch_by_id;
/// Claim a batch of due tasks for a worker
pub mod fetch_next;
/// Claim due tasks across the queues of a shared storage
pub mod fetch_next_shared;
/// Keep workers alive by refreshing their heartbeat
pub mod keep_alive;
/// List queues with their statistics
pub mod list_queues;
/// List tasks in a queue
pub mod list_tasks;
/// List workers registered with the backend
pub mod list_workers;
/// Lock a claimed task for processing
pub mod lock_task;
/// Collect queue and global metrics
pub mod metrics;
/// Insert a batch of tasks
pub mod push_tasks;
/// Register a worker with the backend
pub mod register_worker;
/// Re-enqueue tasks stranded by dead workers
pub mod reenqueue_orphaned;
/// Purge completed tasks from storage
pub mod vacuum;
/// Wait for tasks to reach a terminal state
pub mod wait_for;

pub(crate) const MAX_TX_RETRIES: usize = 5;

/// A transaction that lost an optimistic-concurrency race and is safe to replay
pub(crate) fn is_retryable_conflict(err: &surrealdb::Error) -> bool {
    matches!(err.query_details(), Some(QueryError::TransactionConflict))
        || err.message().contains("Transaction conflict")
}
