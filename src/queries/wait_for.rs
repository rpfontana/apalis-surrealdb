use std::{collections::HashSet, str::FromStr, time::Duration};

use apalis_core::{
    backend::{BackendExt, TaskResult, WaitForCompletion},
    task::{status::Status, task_id::TaskId},
};
use futures::{StreamExt, stream, stream::BoxStream};
use serde::de::DeserializeOwned;
use surrealdb::types::{RecordId, SurrealValue};
use ulid::Ulid;

use crate::{CompactType, JOB_TABLE, SurrealContext, SurrealError, SurrealStorage};

const FETCH_COMPLETED_TASKS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/fetch_completed_tasks.surql"
));

const POLL_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, SurrealValue)]
struct ResultRow {
    id: String,
    status: String,
    result: Option<serde_json::Value>,
}

impl ResultRow {
    fn into_task_result<O>(self) -> Result<TaskResult<O, Ulid>, SurrealError>
    where
        Result<O, String>: DeserializeOwned,
    {
        let task_id = TaskId::from_str(&self.id).map_err(|e| SurrealError::Decode(Box::new(e)))?;
        let status = Status::from_str(&self.status).map_err(|e| SurrealError::Decode(e.into()))?;
        let result: Result<O, String> =
            serde_json::from_value(self.result.unwrap_or(serde_json::Value::Null))
                .map_err(|e| SurrealError::Decode(e.into()))?;
        Ok(TaskResult::new(task_id, status, result))
    }
}

fn record_ids<'a>(ids: impl Iterator<Item = &'a String>) -> Vec<RecordId> {
    ids.map(|id| RecordId::new(JOB_TABLE, id.clone())).collect()
}

impl<O: 'static + Send, Args, D, F> WaitForCompletion<O> for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
    Result<O, String>: DeserializeOwned,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O, Ulid>, SurrealError>>;

    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
        let conn = self.conn.clone();
        let ids: HashSet<String> = task_ids.into_iter().map(|id| id.to_string()).collect();

        stream::unfold(ids, move |mut remaining| {
            let conn = conn.clone();
            async move {
                if remaining.is_empty() {
                    return None;
                }
                let fetch = async {
                    let mut response = conn
                        .query(FETCH_COMPLETED_TASKS)
                        .bind(("ids", record_ids(remaining.iter())))
                        .await?;
                    response
                        .take::<Vec<ResultRow>>(0)
                        .map_err(SurrealError::from)
                };
                let rows = match fetch.await {
                    Ok(rows) => rows,
                    Err(e) => return Some((stream::iter(vec![Err(e)]), HashSet::new())),
                };
                if rows.is_empty() {
                    apalis_core::timer::sleep(POLL_INTERVAL).await;
                    return Some((stream::iter(Vec::new()), remaining));
                }
                let results = rows
                    .into_iter()
                    .map(|row| {
                        remaining.remove(&row.id);
                        row.into_task_result()
                    })
                    .collect::<Vec<_>>();
                Some((stream::iter(results), remaining))
            }
        })
        .flatten()
        .boxed()
    }

    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<O, Ulid>>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let ids: Vec<String> = task_ids.into_iter().map(|id| id.to_string()).collect();
        async move {
            let mut response = conn
                .query(FETCH_COMPLETED_TASKS)
                .bind(("ids", record_ids(ids.iter())))
                .await?;
            let rows: Vec<ResultRow> = response.take(0)?;
            rows.into_iter().map(ResultRow::into_task_result).collect()
        }
    }
}
