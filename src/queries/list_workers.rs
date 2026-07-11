use apalis_core::backend::{BackendExt, ListWorkers, RunningWorker};
use surrealdb::types::{Datetime, SurrealValue};
use ulid::Ulid;

use crate::{CompactType, SurrealContext, SurrealError, SurrealStorage};

const LIST_WORKERS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/list_workers.surql"
));

const LIST_ALL_WORKERS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/list_all_workers.surql"
));

#[derive(Debug, SurrealValue)]
struct WorkerRow {
    name: String,
    queue: String,
    storage_name: String,
    layers: Option<String>,
    started_at: Datetime,
    last_seen: Datetime,
}

impl From<WorkerRow> for RunningWorker {
    fn from(row: WorkerRow) -> Self {
        RunningWorker {
            id: row.name,
            queue: row.queue,
            backend: row.storage_name,
            started_at: row.started_at.into_inner().timestamp() as u64,
            last_heartbeat: row.last_seen.into_inner().timestamp() as u64,
            layers: row.layers.unwrap_or_default(),
        }
    }
}

impl<Args: Sync, D, F> ListWorkers for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let queue = self.config().queue().to_string();
        async move {
            let mut response = conn
                .query(LIST_WORKERS)
                .bind(("queue", queue))
                .bind(("limit", 100))
                .bind(("offset", 0))
                .await?;
            let rows: Vec<WorkerRow> = response.take(0)?;
            Ok(rows.into_iter().map(RunningWorker::from).collect())
        }
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        let conn = self.conn.clone();
        async move {
            let mut response = conn
                .query(LIST_ALL_WORKERS)
                .bind(("limit", 100))
                .bind(("offset", 0))
                .await?;
            let rows: Vec<WorkerRow> = response.take(0)?;
            Ok(rows.into_iter().map(RunningWorker::from).collect())
        }
    }
}
