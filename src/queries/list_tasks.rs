use apalis_core::{
    backend::{BackendExt, Filter, ListAllTasks, ListTasks, codec::Codec},
    task::{Task, status::Status},
};
use apalis_sql::TaskRow;
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{
    CompactType, SurrealContext, SurrealError, SurrealStorage, SurrealTask, from_row::SurrealTaskRow,
};

const LIST_JOBS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/list_jobs.surql"
));

const LIST_ALL_JOBS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/list_all_jobs.surql"
));

impl<Args, D, F> ListTasks<Args> for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    fn list_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<SurrealTask<Args>>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let queue = self.config().queue().to_string();
        let status = filter
            .status
            .as_ref()
            .unwrap_or(&Status::Pending)
            .to_string();
        let limit = i64::from(filter.limit());
        let offset = i64::from(filter.offset());
        async move {
            let mut response = conn
                .query(LIST_JOBS)
                .bind(("queue", queue))
                .bind(("status", status))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;
            let rows: Vec<SurrealTaskRow> = response.take(0)?;
            rows.into_iter()
                .map(|row| {
                    let task_row: TaskRow = row.try_into()?;
                    task_row
                        .try_into_task_compact::<Ulid, Surreal<Any>>()?
                        .try_map(|a| D::decode(&a))
                        .map_err(|e| SurrealError::Decode(e.into()))
                })
                .collect()
        }
    }
}

impl<Args, D, F> ListAllTasks for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
{
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<
        Output = Result<Vec<Task<Self::Compact, Self::Context, Self::IdType>>, Self::Error>,
    > + Send {
        let conn = self.conn.clone();
        let status = filter
            .status
            .as_ref()
            .unwrap_or(&Status::Pending)
            .to_string();
        let limit = i64::from(filter.limit());
        let offset = i64::from(filter.offset());
        async move {
            let mut response = conn
                .query(LIST_ALL_JOBS)
                .bind(("status", status))
                .bind(("limit", limit))
                .bind(("offset", offset))
                .await?;
            let rows: Vec<SurrealTaskRow> = response.take(0)?;
            rows.into_iter()
                .map(|row| {
                    let task_row: TaskRow = row.try_into()?;
                    Ok(task_row.try_into_task_compact::<Ulid, Surreal<Any>>()?)
                })
                .collect()
        }
    }
}
