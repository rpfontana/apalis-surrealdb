use apalis_core::error::BoxDynError;
use apalis_sql::from_row::FromRowError;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SurrealError {
    #[error(transparent)]
    Database(#[from] surrealdb::Error),

    #[error("Failed to decode task: {0}")]
    Decode(BoxDynError),

    #[error(transparent)]
    Row(#[from] FromRowError),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Worker already exists: {0}")]
    WorkerAlreadyExists(String),

    #[error("Worker not found: {0}")]
    WorkerNotFound(String),

    #[error("Task is missing a record id")]
    MissingTaskId,

    #[error("Task is missing its worker context")]
    MissingWorkerContext,
}
