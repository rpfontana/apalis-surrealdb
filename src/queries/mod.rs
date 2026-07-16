use std::collections::{BTreeMap, HashMap};

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
/// Park a task that can never run
pub mod kill_task;
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
/// Re-enqueue tasks stranded by dead workers
pub mod reenqueue_orphaned;
/// Register a worker with the backend
pub mod register_worker;
/// Purge completed tasks from storage
pub mod vacuum;
/// Wait for tasks to reach a terminal state
pub mod wait_for;

pub(crate) const MAX_TX_RETRIES: usize = 5;

/// What to do with a transaction attempt
#[derive(Debug)]
pub(crate) enum TxOutcome {
    Retry(surrealdb::Error),
    Fail(surrealdb::Error),
}

fn is_conflict(err: &surrealdb::Error) -> bool {
    matches!(err.query_details(), Some(QueryError::TransactionConflict))
        || err.message().contains("Transaction conflict")
}

fn is_not_executed(err: &surrealdb::Error) -> bool {
    matches!(err.query_details(), Some(QueryError::NotExecuted))
}

/// Classify a BEGIN..COMMIT error set: siblings of a failed statement are `NotExecuted` noise, only the failing slot holds the real error
pub(crate) fn classify_tx_errors(errors: HashMap<usize, surrealdb::Error>) -> Option<TxOutcome> {
    if errors.is_empty() {
        return None;
    }
    // BTreeMap so the surfaced error is the lowest slot, not HashMap order
    let ordered: BTreeMap<usize, surrealdb::Error> = errors.into_iter().collect();
    let mut first_conflict = None;
    let mut first_real = None;
    let mut first_not_executed = None;
    for (_, err) in ordered {
        if is_conflict(&err) {
            first_conflict.get_or_insert(err);
        } else if is_not_executed(&err) {
            first_not_executed.get_or_insert(err);
        } else {
            first_real.get_or_insert(err);
        }
    }
    if let Some(err) = first_real {
        Some(TxOutcome::Fail(err))
    } else if let Some(err) = first_conflict {
        Some(TxOutcome::Retry(err))
    } else {
        // all NotExecuted: shouldn't happen, but fail rather than retry silently
        first_not_executed.map(TxOutcome::Fail)
    }
}
